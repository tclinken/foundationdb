#include "BackupAgent.h"
#include <ctime>
#include <climits>
#include "flow/IAsyncFile.h"
#include "flow/genericactors.actor.h"
#include "flow/Hash3.h"
#include <numeric>

#undef FLOW_ACOMPILER_STATE
#define FLOW_ACOMPILER_STATE 1

const std::string BackupAgent::keyFolderId = "config_folderid";
const std::string BackupAgent::keyBeginVersion = "beginVersion";
const std::string BackupAgent::keyEndVersion = "endVersion";
const std::string BackupAgent::keyOutputDir = "output_dir";
const std::string BackupAgent::keyConfigBackupTag = "config_backup_tag";
const std::string BackupAgent::keyConfigLogUid = "config_log_uid";
const std::string BackupAgent::keyConfigBackupRanges = "config_backup_ranges";
const std::string BackupAgent::keyConfigStopWhenDoneKey =
    "config_stop_when_done";
const std::string BackupAgent::keyStateStop = "state_stop";
const std::string BackupAgent::keyStateStatus = "state_status";
const std::string BackupAgent::keyStateGlobalStatus = "global_status";
const std::string BackupAgent::keyStateStopVersion = "state_stopversion";
const std::string BackupAgent::keyErrors = "state_errors";
const std::string BackupAgent::keyLastUid = "last_uid";
const std::string BackupAgent::keyBeginKey = "beginKey";
const std::string BackupAgent::keyEndKey = "endKey";

ACTOR static Future<Void>
logErrorWorker(Reference<ReadYourWritesTransaction> tr, Key errorKey,
               std::string message)
{
    Version currentVersion = wait(tr->getReadVersion());
    FDBTuple t;
    t.append(currentVersion);
    t.append(g_random->randomAlphaNumeric());
    TraceEvent("BA_logError")
        .detail("key", printable(errorKey))
        .detail("message", message)
        .detail("readVersion", currentVersion);
    tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
    tr->set(errorKey.toString() + t.get(), message);
    return Void();
}

static Future<Void>
logError(Database cx, Key errorKey, const std::string& message)
{
    return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) {
        return logErrorWorker(tr, errorKey, message);
    });
}

static Future<Void>
logError(Database cx, const std::string& errorKey, const std::string& message)
{
    return logError(cx, StringRef(errorKey), message);
}

static Future<Void>
logError(Reference<ReadYourWritesTransaction> tr, Key errorKey,
         const std::string& message)
{
    return logError(tr->getDatabase(), errorKey, message);
}

static Future<Void>
logError(Reference<ReadYourWritesTransaction> tr, const std::string& errorKey,
         const std::string& message)
{
    return logError(tr->getDatabase(), StringRef(errorKey), message);
}

static Version
getVersionFromString(std::string const& value)
{
    Version version(-1);
    int n = 0;
    if (sscanf(value.c_str(), "%lld%n", (long long*)&version, &n) != 1 ||
        n != value.size()) {
        TraceEvent(SevWarnAlways, "getVersionFromString")
            .detail("InvalidVersion", value);
        throw restore_invalid_version();
    }
    return version;
}

ACTOR static Future<Reference<IAsyncFile>>
openBackupFile(std::string fileName, std::string errorKey,
               Reference<ReadYourWritesTransaction> tr)
{
    try {
        state Reference<IAsyncFile> backupFile = wait(g_network->open(
            fileName,
            IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE |
                IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_NO_AIO,
            0644));
        return backupFile;
    } catch (Error& e) {
        state Error err = e;
        Void _uvar = wait(logError(
            tr, errorKey,
            format("ERROR: Failed to open file `%s' because of error %s",
                   fileName.c_str(), err.what())));
        throw err;
    }
}

static Future<Reference<IAsyncFile>>
openBackupFile(std::string fileName, std::string errorKey, Database cx)
{
    return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) {
        return openBackupFile(fileName, errorKey, tr);
    });
}

ACTOR Future<Void>
truncateCloseFile(Database cx, std::string errorKey, std::string fileName,
                  Reference<IAsyncFile> file, int64_t truncateSize = -1)
{
    if (truncateSize == -1) {
        int64_t size = wait(file->size());
        truncateSize = size;
    }
    state Future<Void> truncate = file->truncate(truncateSize);
    state Future<Void> sync = file->sync();

    try {
        Void _uvar = wait(truncate);
    } catch (Error& e) {
        if (e.code() == error_code_actor_cancelled)
            throw;

        state Error err = e;
        Void _uvar = wait(logError(
            cx, errorKey,
            format("ERROR: Failed to write to file `%s' because of error %s",
                   fileName.c_str(), err.what())));
        throw err;
    }

    try {
        Void _uvar = wait(sync);
    } catch (Error& e) {
        if (e.code() == error_code_actor_cancelled)
            throw;
        TraceEvent("BA_TruncateCloseFileSyncError");

        state Transaction tr(cx);
        loop
        {
            try {
                FDBTuple t;
                t.append(std::numeric_limits<Version>::max());
                t.append('0');
                tr.set(
                    errorKey + t.get(),
                    format("WARNING: Cannot sync file `%s'", fileName.c_str()));
                Void _uvar = wait(tr.commit());
                break;
            } catch (Error& e) {
                Void _uvar = wait(tr.onError(e));
            }
        }
    }
    file = Reference<IAsyncFile>();
    return Void();
}

ACTOR static Future<std::string>
getPath(Reference<ReadYourWritesTransaction> tr, std::string configOutputPath,
        std::string backupUID)
{
    ASSERT(tr);
    tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
    state Future<Optional<Value>> uid = tr->get(StringRef(backupUID));
    state Future<Optional<Value>> dir = tr->get(StringRef(configOutputPath));
    Void _uvar = wait(success(uid) && success(dir));

    if (!uid.get().present() || !uid.get().get().toString().size())
        return std::string();

    if (dir.get().present() && dir.get().get().toString().size())
        return joinPath(dir.get().get().toString(),
                        "backup-" + uid.get().get().toString());

    return joinPath("./", "backup-" + uid.get().get().toString());
}

ACTOR static Future<std::string>
getAndMakePath(Reference<ReadYourWritesTransaction> tr, Reference<Task> task)
{
    ASSERT(task);
    state std::string path;

    if (task->params[BackupAgent::keyOutputDir].length())
        path = joinPath(task->params[BackupAgent::keyOutputDir],
                        "backup-" + task->params[BackupAgent::keyFolderId]);
    else
        path =
            joinPath("./", "backup-" + task->params[BackupAgent::keyFolderId]);

    try {
        createDirectory(path);
    } catch (Error& e) {
        state Error err = e;
        Void _uvar = wait(logError(
            tr, task->params[BackupAgent::keyErrors],
            format("ERROR: failed to create directory `%s'", path.c_str())));
        throw err;
    }

    return path;
}

static Future<std::string>
getAndMakePath(Database cx, Reference<Task> task)
{
    return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) {
        return getAndMakePath(tr, task);
    });
}

// Checks the database to make sure that this module knows how to decode
// its backup data format.
ACTOR static Future<Void>
checkVersion(Reference<ReadYourWritesTransaction> tr)
{
    ASSERT(tr);
    tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
    Optional<Value> value =
        wait(tr->get(LiteralStringRef("\xff/backupDataFormat")));
    int version(0);
    if (value.present() && value.get().toString().size()) {
        version = getVersionFromString(value.get().toString());
    }

    if (version != backupVersion) {
        TraceEvent(SevError, "BA_checkVersion")
            .detail("IncompatibleVersion", version);
        throw incompatible_protocol_version();
    }

    return Void();
}

// Transaction log data is stored by the FoundationDB core in the
// \xff / bklog / keyspace in a funny order for performance reasons.
// Return the ranges of keys that contain the data for the given range
// of versions.
Standalone<VectorRef<KeyRangeRef>>
getLogRanges(Version beginVersion, Version endVersion, std::string backupUid)
{
    Standalone<VectorRef<KeyRangeRef>> ret;

    std::string baLogRangePrefix = backupLogKeys.begin.toString() + backupUid;

    // TraceEvent("getLogRanges").detail("backupUid",
    // backupUid).detail("prefix", printable(StringRef(baLogRangePrefix)));

    for (int64_t vblock = beginVersion / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE;
         vblock < (endVersion + CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE - 1) /
                      CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE;
         ++vblock) {
        uint64_t bv = bigEndian64(std::max(
            beginVersion, vblock * CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE));
        uint64_t ev = bigEndian64(std::min(
            endVersion, (vblock + 1) * CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE));
        uint32_t data = vblock & 0xffffffff;
        uint8_t hash = (uint8_t)hashlittle(&data, sizeof(uint32_t), 0);
        std::string vblock_prefix =
            baLogRangePrefix +
            std::string((const char*)(&hash), sizeof(uint8_t));
        ret.push_back_deep(
            ret.arena(),
            KeyRangeRef(vblock_prefix +
                            std::string((const char*)(&bv), sizeof(uint64_t)),
                        vblock_prefix +
                            std::string((const char*)(&ev), sizeof(uint64_t))));
    }

    return ret;
}

Version
getTaskParamVersion(Reference<Task> task, std::string param)
{
    ASSERT(task);

    Version version = -1;
    // get param version
    if (task->params.find(param) != task->params.end()) {
        version = getVersionFromString(task->params[param]);
    } else {
        TraceEvent(SevWarnAlways, "TaskWithoutParamVersion")
            .detail("InvalidParam", param);
    }

    return version;
}

// Given a key from one of the ranges returned by get_log_ranges,
// returns(version, part) where version is the database version number of
// the transaction log data in the value, and part is 0 for the first such
// data for a given version, 1 for the second block of data, etc.
std::pair<uint64_t, uint32_t>
decodeBKMutationLogKey(Key key)
{
    // TraceEvent("decodeBKMutationLogKeyStart").detail("Key", printable(key));

    return std::make_pair(
        bigEndian64(*(int64_t*)(key.begin() + backupLogPrefixBytes +
                                sizeof(UID) + sizeof(uint8_t))),
        bigEndian32(*(int32_t*)(key.begin() + backupLogPrefixBytes +
                                sizeof(UID) + sizeof(uint8_t) +
                                sizeof(int64_t))));
}

typedef std::pair<Standalone<RangeResultRef>, Version> RangeResultWithVersion;

ACTOR Future<Void>
readCommitted(Database cx, PromiseStream<RangeResultWithVersion> results,
              FlowLock* lock, KeyRangeRef range, bool terminator = false,
              bool systemAccess = false)
{
    state KeySelector begin = firstGreaterOrEqual(range.begin);
    state KeySelector end = firstGreaterOrEqual(range.end);
    state GetRangeLimits limits(CLIENT_KNOBS->ROW_LIMIT_UNLIMITED,
                                CLIENT_KNOBS->BACKUP_GET_RANGE_LIMIT_BYTES);
    state Reference<ReadYourWritesTransaction> tr(
        new ReadYourWritesTransaction(cx));
    state FlowLock::Releaser releaser;

    ASSERT(lock);

    loop
    {
        try {
            if (systemAccess)
                tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

            // add lock
            Void _uvar = wait(lock->take(
                TaskDefaultYield, CLIENT_KNOBS->BACKUP_GET_RANGE_LIMIT_BYTES +
                                      CLIENT_KNOBS->VALUE_SIZE_LIMIT +
                                      CLIENT_KNOBS->KEY_SIZE_LIMIT));
            releaser = FlowLock::Releaser(
                *lock, CLIENT_KNOBS->BACKUP_GET_RANGE_LIMIT_BYTES +
                           CLIENT_KNOBS->VALUE_SIZE_LIMIT +
                           CLIENT_KNOBS->KEY_SIZE_LIMIT);

            Standalone<RangeResultRef> value =
                wait(tr->getRange(begin, end, limits));
            releaser.remaining -=
                value.expectedSize(); // its the responsibility of the caller to
                                      // release after this point
            ASSERT(releaser.remaining >= 0);

            results.send(
                RangeResultWithVersion(value, tr->getReadVersion().get()));

            if (value.size() > 0)
                begin = firstGreaterThan(value.end()[-1].key);

            if (!value.more && !limits.isReached()) {
                results.sendError(end_of_stream());
                return Void();
            }
        } catch (Error& e) {
            if (e.code() != error_code_past_version &&
                e.code() != error_code_future_version)
                throw;
            tr = Reference<ReadYourWritesTransaction>(
                new ReadYourWritesTransaction(cx));
        }
    }
}

struct RCGroup
{
    Standalone<RangeResultRef> items;
    Version version;
    uint64_t groupKey;

    RCGroup()
      : version(-1)
      , groupKey(ULLONG_MAX){};

    template <class Ar>
    void serialize(Ar& ar)
    {
        ar& items& version& groupKey;
    }
};

ACTOR Future<Void>
readCommitted(
    Database cx, PromiseStream<RCGroup> results, Future<Void> active,
    FlowLock* lock, KeyRangeRef range,
    std::function<std::pair<uint64_t, uint32_t>(Key key)> groupBy,
    bool terminator = false, bool systemAccess = false,
    std::function<Future<Void>(Reference<ReadYourWritesTransaction> tr)>
        withEachFunction = nullptr)
{
    ASSERT(lock);

    // TraceEvent("log_readCommitted").detail("rangeBegin",
    // printable(range.begin)).detail("rangeEnd", printable(range.end));

    state KeySelector nextKey = firstGreaterOrEqual(range.begin);
    state KeySelector end = firstGreaterOrEqual(range.end);
    state GetRangeLimits limits(CLIENT_KNOBS->ROW_LIMIT_UNLIMITED,
                                CLIENT_KNOBS->BACKUP_GET_RANGE_LIMIT_BYTES);

    state RCGroup rcGroup = RCGroup();
    state uint64_t skipGroup(ULLONG_MAX);
    state Reference<ReadYourWritesTransaction> tr(
        new ReadYourWritesTransaction(cx));

    loop
    {
        try {
            if (systemAccess)
                tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

            if (withEachFunction)
                Void _uvar = wait(withEachFunction(tr));

            state Standalone<RangeResultRef> rangevalue =
                wait(tr->getRange(nextKey, end, limits));

            // add lock
            Void _uvar =
                wait(active && lock->take(TaskDefaultYield,
                                          rangevalue.expectedSize() +
                                              rcGroup.items.expectedSize()));
            state FlowLock::Releaser releaser(*lock,
                                              rangevalue.expectedSize() +
                                                  rcGroup.items.expectedSize());

            int index(0);
            for (auto& s : rangevalue) {
                uint64_t groupKey = groupBy(s.key).first;
                // TraceEvent("log_readCommitted").detail("groupKey",
                // groupKey).detail("skipGroup", skipGroup).detail("nextKey",
                // printable(nextKey.key)).detail("end",
                // printable(end.key)).detail("valuesize",
                // value.size()).detail("index",index++).detail("size",s.value.size());
                if (groupKey != skipGroup) {
                    if (rcGroup.version == -1) {
                        rcGroup.version = tr->getReadVersion().get();
                        rcGroup.groupKey = groupKey;
                    } else if (rcGroup.groupKey != groupKey) {
                        // TraceEvent("log_readCommitted").detail("sendGroup0",
                        // rcGroup.groupKey).detail("itemSize",
                        // rcGroup.items.size()).detail("data_length",rcGroup.items[0].value.size());
                        state uint32_t len(0);
                        for (size_t j = 0; j < rcGroup.items.size(); ++j) {
                            len += rcGroup.items[j].value.size();
                        }
                        // TraceEvent("SendGroup").detail("groupKey",
                        // rcGroup.groupKey).detail("version",
                        // rcGroup.version).detail("length",
                        // len).detail("releaser.remaining",
                        // releaser.remaining);
                        releaser.remaining -=
                            rcGroup.items.expectedSize(); // its the
                                                          // responsibility of
                                                          // the caller to
                                                          // release after this
                                                          // point
                        ASSERT(releaser.remaining >= 0);
                        results.send(rcGroup);
                        nextKey = firstGreaterThan(rcGroup.items.end()[-1].key);
                        skipGroup = rcGroup.groupKey;

                        rcGroup = RCGroup();
                        rcGroup.version = tr->getReadVersion().get();
                        rcGroup.groupKey = groupKey;
                    }
                    rcGroup.items.push_back_deep(rcGroup.items.arena(), s);
                }
            }

            if (rangevalue.size() > 0)
                nextKey = firstGreaterThan(rangevalue.end()[-1].key);

            if (!rangevalue.more && !limits.isReached()) {
                if (rcGroup.version != -1) {
                    releaser.remaining -=
                        rcGroup.items.expectedSize(); // its the responsibility
                                                      // of the caller to
                                                      // release after this
                                                      // point
                    ASSERT(releaser.remaining >= 0);
                    // TraceEvent("log_readCommitted").detail("sendGroup1",
                    // rcGroup.groupKey).detail("itemSize",
                    // rcGroup.items.size()).detail("data_length",
                    // rcGroup.items[0].value.size());
                    results.send(rcGroup);
                }

                results.sendError(end_of_stream());
                return Void();
            }
        } catch (Error& e) {
            if (e.code() != error_code_past_version &&
                e.code() != error_code_future_version)
                throw;
            Void _uvar = wait(tr->onError(e));
        }
    }
}

std::pair<uint64_t, uint32_t>
testUnpack(Key key)
{
    return std::make_pair(bigEndian32(*(int32_t*)(key.begin() + 5)), 0);
}

ACTOR Future<Void>
_testReadCommitted(Database cx)
{
    state FlowLock lock(CLIENT_KNOBS->BACKUP_LOCK_BYTES);
    state KeyRange range(
        KeyRangeRef(LiteralStringRef("test/"), LiteralStringRef("test0")));
    state PromiseStream<RCGroup> results;

    state Future<Void> rc =
        readCommitted(cx, results, Void(), &lock, range, testUnpack);
    loop
    {
        try {
            state RCGroup values = waitNext(results.getFuture());

            // release lock
            lock.release(values.items.expectedSize());

            TraceEvent("BA_testReadCommitted")
                .detail("version", values.version)
                .detail("group_key", values.groupKey)
                .detail("length_items", values.items.size());

            Void _uvar = wait(delay(0.25));
        } catch (Error& e) {
            if (e.code() == error_code_end_of_stream) {
                return Void();
            }
            throw;
        }
    }
}

Future<Void>
testReadCommitted(Database cx)
{
    return _testReadCommitted(cx);
}

struct BackupRangeTaskFunc : TaskFuncBase
{
    static const char* name;
    static const uint32_t version = 1;

    const char* getName() const override { return name; };
    Future<Void> execute(Database cx, Reference<TaskBucket> tb,
                         Reference<FutureBucket> fb,
                         Reference<Task> task) override
    {
        return _execute(cx, tb, fb, task);
    };
    Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
                        Reference<TaskBucket> tb, Reference<FutureBucket> fb,
                        Reference<Task> task) override
    {
        return _finish(tr, tb, fb, task);
    };

    ACTOR static Future<Standalone<VectorRef<KeyRef>>> getBlockOfShards(
        Reference<ReadYourWritesTransaction> tr, std::string beginKey,
        std::string endKey, int limit)
    {

        TraceEvent("BA_BackupRangeTaskFuncgetBlockOfShardsStart")
            .detail("beginKey", printable(StringRef(beginKey)))
            .detail("endKey", printable(StringRef(endKey)))
            .detail("limit", limit);

        tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
        state std::string pre("\xff/keyServers/");
        state Standalone<VectorRef<KeyRef>> results;
        Standalone<RangeResultRef> values = wait(tr->getRange(
            KeyRange(KeyRangeRef(pre + beginKey + '\x00', pre + endKey)),
            limit));

        for (auto& s : values) {
            KeyRef k = s.key.removePrefix(pre);
            // TraceEvent("BA_getBlockOfShards").detail("item",
            // printable(s)).detail("keyRemovePrefix", printable(k));
            results.push_back_deep(results.arena(), k);
        }

        return results;
    }

    ACTOR static Future<Void> addTask(
        Reference<ReadYourWritesTransaction> tr,
        Reference<TaskBucket> taskBucket, Reference<TaskFuture> taskFuture,
        std::string folderId, std::string beginKey, std::string endKey,
        std::string outPath, std::string errorsKey, std::string joinedFutureKey)
    {

        ASSERT(taskBucket);
        ASSERT(taskFuture);

        // Create a joined future, if not specified
        if (joinedFutureKey.empty()) {
            Reference<TaskFuture> newOnDone =
                wait(taskFuture->joinedFuture(tr, taskBucket));
            joinedFutureKey = newOnDone->key;
        }

        state Reference<Task> task(new Task(BackupRangeTaskFunc::name,
                                            BackupRangeTaskFunc::version,
                                            joinedFutureKey));
        task->params[BackupAgent::keyFolderId] = folderId;
        task->params[BackupAgent::keyBeginKey] = beginKey;
        task->params[BackupAgent::keyEndKey] = endKey;
        task->params[BackupAgent::keyOutputDir] = outPath;
        task->params[BackupAgent::keyErrors] = errorsKey;
        std::string key = taskBucket->addTask(tr, task);

        TraceEvent("BA_BackupRangeTaskFunc_addTask")
            .detail("type", task->params[Task::reservedTaskParamKeyType])
            .detail("key", key)
            .detail("onDone", joinedFutureKey)
            .detail("backup_uid", folderId)
            .detail("output_path", outPath)
            .detail("beginKey", printable(StringRef(beginKey)))
            .detail("endKey", printable(StringRef(endKey)));

        return Void();
    }

    ACTOR static Future<Void> endFile(Database cx, std::string errorKey,
                                      Reference<IAsyncFile> outFile,
                                      std::string outFileName, std::string path,
                                      std::string beginKey, std::string nextKey,
                                      Version atVersion)
    {
        if (!outFile) {
            return Void();
        }

        state int64_t offset = wait(outFile->size());
        Void _uvar =
            wait(outFile->write(beginKey.c_str(), beginKey.size(), offset));
        offset += beginKey.size();

        Void _uvar =
            wait(outFile->write(nextKey.c_str(), nextKey.size(), offset));
        offset += nextKey.size();

        //// padding
        // state int64_t padding = (4096 - ((offset + 20) % 4096));
        // if (padding) {
        //	std::string strPad(padding, '\xff');
        //	Void _uvar = wait(outFile->write(strPad.c_str(), padding,
        // offset)); 	offset += padding;
        //}

        state uint32_t data = beginKey.size();
        Void _uvar = wait(outFile->write(&data, sizeof(uint32_t), offset));
        offset += sizeof(uint32_t);

        data = nextKey.size();
        Void _uvar = wait(outFile->write(&data, sizeof(uint32_t), offset));
        offset += sizeof(uint32_t);

        // Placeholder: checksum
        uint64_t data1 = 0;
        Void _uvar = wait(outFile->write(&data1, sizeof(uint64_t), offset));
        offset += sizeof(uint64_t);

        // Version of this "trailer"
        data = 1;
        Void _uvar = wait(outFile->write(&data, sizeof(uint32_t), offset));
        offset += sizeof(uint32_t);

        state std::string finalName = joinPath(
            path, BackupAgent::getDataFilename(format("%lld", atVersion)));
        Void _uvar =
            wait(truncateCloseFile(cx, errorKey, finalName, outFile, offset));

        // rename file
        renameFile(outFileName, finalName);

        TraceEvent("BA_BackupRangeTaskFunc_endFile")
            .detail("Rename_file_from", outFileName)
            .detail("to_new_file", finalName);

        return Void();
    }

    ACTOR static Future<Void> _execute(Database cx,
                                       Reference<TaskBucket> taskBucket,
                                       Reference<FutureBucket> futureBucket,
                                       Reference<Task> task)
    {
        state FlowLock lock(CLIENT_KNOBS->BACKUP_LOCK_BYTES);
        ASSERT(taskBucket);
        ASSERT(futureBucket);
        ASSERT(task);

        state UID randomID = g_random->randomUniqueID();

        // check task version
        uint32_t taskVersion = task->getVersion();
        if (taskVersion > BackupRangeTaskFunc::version) {
            TraceEvent(SevError, "BA_BackupRangeTaskFunc_execute")
                .detail("taskVersion", taskVersion)
                .detail("is_greater_than_the_BackupRangeTaskFunc_version",
                        BackupRangeTaskFunc::version);
            Void _uvar = wait(logError(
                cx, task->params[BackupAgent::keyErrors],
                format("ERROR: %s task version `%lu' is greater than supported "
                       "version `%lu'",
                       task->params[Task::reservedTaskParamKeyType].c_str(),
                       (unsigned long)taskVersion,
                       (unsigned long)BackupRangeTaskFunc::version)));
            throw task_invalid_version();
        }

        TraceEvent("BA_BackupRangeTaskFunc_execute", randomID)
            .detail("beginKey", printable(StringRef(
                                    task->params[BackupAgent::keyBeginKey])))
            .detail("endKey",
                    printable(StringRef(task->params[BackupAgent::keyEndKey])))
            .detail("taskKey", printable(StringRef(task->key)))
            .detail("onDone", task->params[Task::reservedTaskParamKeyDone]);

        // Find out if there is a shard boundary in(beginKey, endKey)
        Standalone<VectorRef<KeyRef>> keys = wait(
            runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) {
                return getBlockOfShards(
                    tr, task->params[BackupAgent::keyBeginKey],
                    task->params[BackupAgent::keyEndKey], 1);
            }));
        if (keys.size() > 0) {
            task->params["addBackupRangeTasks"] = "true";
            TraceEvent("BA_BackupRangeTaskFunc_executeBoundary", randomID)
                .detail("shardboundary", keys.size())
                .detail("beginKey",
                        printable(
                            StringRef(task->params[BackupAgent::keyBeginKey])))
                .detail("endKey", printable(StringRef(
                                      task->params[BackupAgent::keyEndKey])))
                .detail("taskKey", printable(StringRef(task->key)))
                .detail("onDone", task->params[Task::reservedTaskParamKeyDone]);
            return Void();
        }

        // Read everything from beginKey to endKey, write it to an output file,
        // run the output file processor, and
        // then set on_done.If we are still writing after X seconds, end the
        // output file and insert a new backup_range
        // task for the remainder.
        state std::string path = wait(getAndMakePath(cx, task));
        state double timeout = now() + CLIENT_KNOBS->BACKUP_RANGE_TIMEOUT;
        state Reference<IAsyncFile> outFile;
        state Version outVersion;
        state std::string lastKey;
        state std::string outFileName;
        state std::string beginKey = task->params[BackupAgent::keyBeginKey];

        state KeyRange range(KeyRangeRef(task->params[BackupAgent::keyBeginKey],
                                         task->params[BackupAgent::keyEndKey]));

        // retrieve kvData
        state PromiseStream<RangeResultWithVersion> results;

        state Future<Void> rc = readCommitted(cx, results, &lock, range, true);
        loop
        {
            try {
                state RangeResultWithVersion values =
                    waitNext(results.getFuture());

                // release lock
                lock.release(values.first.expectedSize());

                if ((now() >= timeout) || (values.second != outVersion)) {
                    if (outFile) {
                        bool isFinished =
                            wait(taskBucket->isFinished(cx, task));
                        if (isFinished) {
                            Void _uvar = wait(truncateCloseFile(
                                cx, task->params[BackupAgent::keyErrors],
                                outFileName, outFile));
                            TraceEvent("BA_BackupRangeTaskFunc_executeFinished",
                                       randomID)
                                .detail("version", values.second)
                                .detail("count", values.first.size())
                                .detail("beginKey",
                                        printable(StringRef(
                                            task->params
                                                [BackupAgent::keyBeginKey])))
                                .detail(
                                    "endKey",
                                    printable(StringRef(
                                        task->params[BackupAgent::keyEndKey])))
                                .detail("outVersion", outVersion);
                            return Void();
                        }
                        Void _uvar = wait(
                            endFile(cx, task->params[BackupAgent::keyErrors],
                                    outFile, outFileName, path, beginKey,
                                    lastKey + '\x00', outVersion));
                        beginKey = lastKey + '\x00';
                    }

                    if (now() >= timeout) {
                        task->params["backupRangeBeginKey"] = beginKey;
                        TraceEvent("BA_BackupRangeTaskFunc_executeTimeout",
                                   randomID)
                            .detail("version", values.second)
                            .detail("count", values.first.size())
                            .detail(
                                "beginKey",
                                printable(StringRef(
                                    task->params[BackupAgent::keyBeginKey])))
                            .detail("endKey",
                                    printable(StringRef(
                                        task->params[BackupAgent::keyEndKey])))
                            .detail("outVersion", outVersion);
                        return Void();
                    }

                    outFileName =
                        joinPath(path, BackupAgent::getTempFilename());
                    Reference<IAsyncFile> f = wait(openBackupFile(
                        outFileName, task->params[BackupAgent::keyErrors], cx));
                    outFile = f;
                    outVersion = values.second;
                }

                // write kvData to file
                state size_t i = 0;
                state int64_t offset = wait(outFile->size());
                state KeyRef k;
                state KeyRef v;
                state uint32_t length;

                for (; i < values.first.size(); ++i) {
                    k = values.first[i].key;
                    v = values.first[i].value;
                    length = k.size();
                    Void _uvar =
                        wait(outFile->write(&length, sizeof(uint32_t), offset));
                    offset += sizeof(uint32_t);

                    length = v.size();
                    Void _uvar =
                        wait(outFile->write(&length, sizeof(uint32_t), offset));
                    offset += sizeof(uint32_t);

                    Void _uvar =
                        wait(outFile->write(k.begin(), k.size(), offset));
                    offset += k.size();

                    Void _uvar =
                        wait(outFile->write(v.begin(), v.size(), offset));
                    offset += v.size();
                }

                lastKey = k.toString();
            } catch (Error& e) {
                // TraceEvent("BA_BackupRangeTaskFunc_executeException",
                // randomID).detail("beginKey",
                // printable(StringRef(task->params[BackupAgent::keyBeginKey])))
                //	.detail("endKey",
                // printable(StringRef(task->params[BackupAgent::keyEndKey]))).detail("timeout",
                // timeout).error(e);
                state Error err = e;
                if (err.code() == error_code_actor_cancelled)
                    throw err;

                if (err.code() == error_code_end_of_stream) {
                    if (outFile) {
                        bool isFinished =
                            wait(taskBucket->isFinished(cx, task));
                        if (isFinished) {
                            Void _uvar = wait(truncateCloseFile(
                                cx, task->params[BackupAgent::keyErrors],
                                outFileName, outFile));
                            return Void();
                        }
                        TraceEvent("BA_BackupRangeTaskFunc_execute", randomID)
                            .detail("backupRangeFinishedAtVersion_1",
                                    outVersion);
                        try {
                            Void _uvar = wait(endFile(
                                cx, task->params[BackupAgent::keyErrors],
                                outFile, outFileName, path, beginKey,
                                task->params[BackupAgent::keyEndKey],
                                outVersion));
                        } catch (Error& e) {
                            state Error e2 = e;
                            if (e2.code() == error_code_actor_cancelled)
                                throw e2;

                            Void _uvar = wait(logError(
                                cx, task->params[BackupAgent::keyErrors],
                                format("ERROR: Failed to write to file `%s' "
                                       "because of error %s",
                                       outFileName.c_str(), e2.what())));

                            throw e2;
                        }
                    }
                    return Void();
                }

                Void _uvar =
                    wait(logError(cx, task->params[BackupAgent::keyErrors],
                                  format("ERROR: Failed to write to file `%s' "
                                         "because of error %s",
                                         outFileName.c_str(), err.what())));

                throw err;
            }
        }
    }

    ACTOR static Future<Void> startBackupRangeInternal(
        Reference<ReadYourWritesTransaction> tr,
        Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket,
        Reference<Task> task, Reference<TaskFuture> onDone)
    {
        ASSERT(taskBucket);
        ASSERT(futureBucket);
        ASSERT(task);
        ASSERT(onDone);

        state UID randomID = g_random->randomUniqueID();
        tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
        state std::string nextKey = task->params[BackupAgent::keyBeginKey];
        state Standalone<VectorRef<KeyRef>> keys = wait(
            getBlockOfShards(tr, nextKey, task->params[BackupAgent::keyEndKey],
                             CLIENT_KNOBS->BACKUP_SHARD_TASK_LIMIT));

        state size_t idx;
        state size_t count = 0;
        std::vector<Future<Void>> addTaskVector;
        for (idx = 0; idx < keys.size(); ++idx) {
            if (nextKey != keys[idx].toString()) {
                addTaskVector.push_back(
                    addTask(tr, taskBucket, onDone,
                            task->params[BackupAgent::keyFolderId], nextKey,
                            keys[idx].toString(),
                            task->params[BackupAgent::keyOutputDir],
                            task->params[BackupAgent::keyErrors], ""));
                count++;
            }
            nextKey = keys[idx].toString();
        }

        Void _uvar = wait(waitForAll(addTaskVector));

        if (nextKey != task->params[BackupAgent::keyEndKey]) {
            Void _uvar = wait(addTask(
                tr, taskBucket, onDone, task->params[BackupAgent::keyFolderId],
                nextKey, task->params[BackupAgent::keyEndKey],
                task->params[BackupAgent::keyOutputDir],
                task->params[BackupAgent::keyErrors], ""));
            count++;
        }

        // TraceEvent("BA_startBackupRangeInternal",
        // randomID).detail("Breaking_backup_range_beginKey",
        // printable(StringRef(task->params[BackupAgent::keyBeginKey])))
        //	.detail("endKey",
        // printable(StringRef(task->params[BackupAgent::keyEndKey])))
        //.detail("keySize", keys.size())
        //	.detail("into_pieces", count);

        return Void();
    }

    ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
                                      Reference<TaskBucket> taskBucket,
                                      Reference<FutureBucket> futureBucket,
                                      Reference<Task> task)
    {
        ASSERT(taskBucket);
        state Reference<TaskFuture> taskFuture =
            futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

        if (task->params.count("addBackupRangeTasks")) {
            Void _uvar = wait(startBackupRangeInternal(
                tr, taskBucket, futureBucket, task, taskFuture));
        } else if (task->params.count("backupRangeBeginKey")) {
            Void _uvar = wait(BackupRangeTaskFunc::addTask(
                tr, taskBucket, taskFuture,
                task->params[BackupAgent::keyFolderId],
                task->params[BackupAgent::keyBeginKey],
                task->params[BackupAgent::keyEndKey],
                task->params[BackupAgent::keyOutputDir],
                task->params[BackupAgent::keyErrors], taskFuture->key));
        } else {
            Void _uvar = wait(taskFuture->set(tr, taskBucket));
        }

        // finish this task itself
        Void _uvar = wait(taskBucket->finish(tr, task));

        return Void();
    }
};
const char* BackupRangeTaskFunc::name = "backup_range";
REGISTER_TASKFUNC(BackupRangeTaskFunc);

struct FinishFullBackupTaskFunc : TaskFuncBase
{
    static const char* name;
    static const uint32_t version = 1;

    ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
                                      Reference<TaskBucket> taskBucket,
                                      Reference<FutureBucket> futureBucket,
                                      Reference<Task> task)
    {
        ASSERT(taskBucket);
        ASSERT(futureBucket);
        ASSERT(task);

        // check task version
        uint32_t taskVersion = task->getVersion();
        if (taskVersion > FinishFullBackupTaskFunc::version) {
            TraceEvent(SevError, "BA_FinishFullBackupTaskFunc_finish")
                .detail("taskVersion", taskVersion)
                .detail("is_greater_than_the_FinishFullBackupTaskFunc_version",
                        FinishFullBackupTaskFunc::version);
            Void _uvar = wait(logError(
                tr, task->params[BackupAgent::keyErrors],
                format("ERROR: %s task version `%lu' is greater than supported "
                       "version `%lu'",
                       task->params[Task::reservedTaskParamKeyType].c_str(),
                       (unsigned long)taskVersion,
                       (unsigned long)FinishFullBackupTaskFunc::version)));
            throw task_invalid_version();
        }

        TraceEvent("BA_FinishFullBackupTaskFunc")
            .detail("stateStop", printable(StringRef(
                                     task->params[BackupAgent::keyStateStop])));

        tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

        // Enable the stop key
        tr->set(task->params[BackupAgent::keyStateStop], LiteralStringRef("1"));

        // finish this task itself
        Void _uvar = wait(taskBucket->finish(tr, task));

        return Void();
    }

    const char* getName() const override { return name; };
    Future<Void> execute(Database cx, Reference<TaskBucket> tb,
                         Reference<FutureBucket> fb,
                         Reference<Task> task) override
    {
        return Void();
    };
    Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
                        Reference<TaskBucket> tb, Reference<FutureBucket> fb,
                        Reference<Task> task) override
    {
        return _finish(tr, tb, fb, task);
    };
};
const char* FinishFullBackupTaskFunc::name = "finish_full_backup";
REGISTER_TASKFUNC(FinishFullBackupTaskFunc);

struct BackupLogRangeTaskFunc : TaskFuncBase
{
    static const char* name;
    static const uint32_t version = 1;

    const char* getName() const override { return name; };
    Future<Void> execute(Database cx, Reference<TaskBucket> tb,
                         Reference<FutureBucket> fb,
                         Reference<Task> task) override
    {
        return _execute(cx, tb, fb, task);
    };
    Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
                        Reference<TaskBucket> tb, Reference<FutureBucket> fb,
                        Reference<Task> task) override
    {
        return _finish(tr, tb, fb, task);
    };

    ACTOR static Future<Void> dumpData(Database cx, Reference<Task> task,
                                       PromiseStream<RCGroup> results,
                                       Reference<IAsyncFile> outFile,
                                       std::string fileName, int64_t* offset,
                                       FlowLock* lock)
    {
        loop
        {
            try {
                state RCGroup group = waitNext(results.getFuture());

                // release lock
                lock->release(group.items.expectedSize());

                state uint32_t len = 0;

                for (size_t j = 0; j < group.items.size(); ++j) {
                    len += group.items[j].value.size();
                    //					ASSERT(key.first ==
                    // group.groupKey);
                    //					ASSERT(key.second == j);
                }

                Void _uvar = wait(outFile->write(
                    &(group.groupKey), sizeof(group.groupKey), *offset));
                (*offset) += sizeof(group.groupKey);

                Void _uvar = wait(outFile->write(&len, sizeof(len), *offset));
                (*offset) += sizeof(len);

                state size_t k(0);
                state std::string valueString;
                for (; k < group.items.size(); ++k) {
                    Void _uvar = wait(
                        outFile->write(group.items[k].value.begin(),
                                       group.items[k].value.size(), (*offset)));
                    (*offset) += group.items[k].value.size();
                }
            } catch (Error& e) {
                if (e.code() == error_code_actor_cancelled)
                    throw e;

                if (e.code() == error_code_end_of_stream) {
                    TraceEvent("BackupLogRangeTaskFunc_dumpData")
                        .detail("error_code_end_of_stream", true);
                    return Void();
                }

                state Error err = e;
                Void _uvar =
                    wait(logError(cx, task->params[BackupAgent::keyErrors],
                                  format("ERROR: Failed to write to file "
                                         "`%s' because of error %s",
                                         fileName.c_str(), err.what())));

                throw err;
            }
        }
    }

    ACTOR static Future<Void> _execute(Database cx,
                                       Reference<TaskBucket> taskBucket,
                                       Reference<FutureBucket> futureBucket,
                                       Reference<Task> task)
    {
        state FlowLock lock(CLIENT_KNOBS->BACKUP_LOCK_BYTES);

        // check task version
        uint32_t taskVersion = task->getVersion();
        if (taskVersion > BackupLogRangeTaskFunc::version) {
            TraceEvent(SevError, "BA_BackupLogRangeTaskFunc_execute")
                .detail("taskVersion", taskVersion)
                .detail("is_greater_than_the_BackupLogRangeTaskFunc_version",
                        BackupLogRangeTaskFunc::version);
            Void _uvar = wait(logError(
                cx, task->params[BackupAgent::keyErrors],
                format("ERROR: %s task version `%lu' is greater than supported "
                       "version `%lu'",
                       task->params[Task::reservedTaskParamKeyType].c_str(),
                       (unsigned long)taskVersion,
                       (unsigned long)BackupLogRangeTaskFunc::version)));
            throw task_invalid_version();
        }

        state double timeout = now() + CLIENT_KNOBS->BACKUP_RANGE_TIMEOUT;
        state Version beginVersion =
            getTaskParamVersion(task, BackupAgent::keyBeginVersion);
        state Version endVersion =
            getTaskParamVersion(task, BackupAgent::keyEndVersion);
        state UID randomID = g_random->randomUniqueID();

        loop
        {
            state Reference<ReadYourWritesTransaction> tr(
                new ReadYourWritesTransaction(cx));

            // Wait for the read version to be 20M version ahead of the start
            // version
            try {
                Version currentVersion = wait(tr->getReadVersion());
                if (endVersion < currentVersion)
                    break;
                TraceEvent("BA_BackupLogsTaskFunc_executeWait")
                    .detail("currentVersion", currentVersion)
                    .detail("endVersion", endVersion);

                Void _uvar = wait(
                    delay(std::max(CLIENT_KNOBS->BACKUP_RANGE_MINWAIT,
                                   (double)(endVersion - currentVersion) /
                                       CLIENT_KNOBS->CORE_VERSIONSPERSECOND)));
            } catch (Error& e) {
                Void _uvar = wait(tr->onError(e));
            }
        }

        if (now() >= timeout) {
            task->params["nextBeginVersion"] =
                task->params[BackupAgent::keyBeginVersion];
            TraceEvent("BA_BackupLogRangeTaskFunc_executeTimeout", randomID)
                .detail("startVersion", beginVersion)
                .detail("keyEndVersion",
                        printable(StringRef(
                            task->params[BackupAgent::keyEndVersion])));
            return Void();
        }

        state Standalone<VectorRef<KeyRangeRef>> ranges =
            getLogRanges(beginVersion, endVersion,
                         task->params[BackupAgent::keyConfigLogUid]);
        if (ranges.size() > CLIENT_KNOBS->BACKUP_MAX_LOG_RANGES) {
            task->params["addBackupLogRangeTasks"] = "true";
            return Void();
        }

        state std::string path = wait(getAndMakePath(cx, task));
        state std::string tempFileName =
            joinPath(path, BackupAgent::getTempFilename());
        state Reference<IAsyncFile> outFile = wait(openBackupFile(
            tempFileName, task->params[BackupAgent::keyErrors], cx));

        state size_t idx;
        state int64_t offset = wait(outFile->size());
        state std::vector<PromiseStream<RCGroup>> results;
        state std::vector<Future<Void>> rc;
        state std::vector<Promise<Void>> active;
        state std::string logFileName =
            joinPath(path, BackupAgent::getLogFilename(
                               task->params[BackupAgent::keyBeginVersion],
                               task->params[BackupAgent::keyEndVersion]));

        for (int i = 0; i < ranges.size(); ++i) {
            results.push_back(PromiseStream<RCGroup>());
            active.push_back(Promise<Void>());
            rc.push_back(readCommitted(cx, results[i], active[i].getFuture(),
                                       &lock, ranges[i], decodeBKMutationLogKey,
                                       false, true, checkVersion));
        }

        for (idx = 0; idx < ranges.size(); ++idx) {
            active[idx].send(Void());

            Void _uvar = wait(dumpData(cx, task, results[idx], outFile,
                                       tempFileName, &offset, &lock));

            if (now() >= timeout) {
                task->params["nextBeginVersion"] = format(
                    "%lld",
                    std::min<Version>(
                        endVersion,
                        ((beginVersion / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE) +
                         idx + 1) *
                            CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE));
                logFileName = joinPath(
                    path, BackupAgent::getLogFilename(
                              task->params[BackupAgent::keyBeginVersion],
                              task->params["nextBeginVersion"]));
                break;
            }
        }

        Void _uvar = wait(endFile(cx, taskBucket, futureBucket, task, outFile,
                                  tempFileName, logFileName));

        TraceEvent("BA_BackupLogRangeTaskFunc_execute")
            .detail("tempfile", tempFileName)
            .detail("logFileName", logFileName)
            .detail("beginVersion", beginVersion)
            .detail("endVersion", endVersion)
            .detail("rangesSize", ranges.size())
            .detail("maxLocks", lock.available())
            .detail("logUid", systemDecompressUid(StringRef(
                                  task->params[BackupAgent::keyConfigLogUid])))
            .detail("logUidValue",
                    printable(
                        StringRef(task->params[BackupAgent::keyConfigLogUid])));

        return Void();
    }

    ACTOR static Future<Void> addTask(Reference<ReadYourWritesTransaction> tr,
                                      Reference<TaskBucket> taskBucket,
                                      Reference<Task> task,
                                      Reference<TaskFuture> taskFuture,
                                      std::string beginVersion,
                                      std::string endVersion,
                                      std::string joinedFutureKey)
    {

        ASSERT(taskBucket);
        ASSERT(task);
        ASSERT(taskFuture);

        // Create a joined future, if not specified
        if (joinedFutureKey.empty()) {
            Reference<TaskFuture> newOnDone =
                wait(taskFuture->joinedFuture(tr, taskBucket));
            // TraceEvent("BA_BackupLogsTaskFunc_addTaskCreatedFuture").detail("futureKey",
            // newOnDone->key);
            joinedFutureKey = newOnDone->key;
        }

        Reference<Task> newTask(new Task(BackupLogRangeTaskFunc::name,
                                         BackupLogRangeTaskFunc::version,
                                         joinedFutureKey));

        newTask->params[BackupAgent::keyFolderId] =
            task->params[BackupAgent::keyFolderId];
        newTask->params[BackupAgent::keyBeginVersion] = beginVersion;
        newTask->params[BackupAgent::keyEndVersion] = endVersion;
        newTask->params[BackupAgent::keyOutputDir] =
            task->params[BackupAgent::keyOutputDir];
        newTask->params[BackupAgent::keyConfigBackupTag] =
            task->params[BackupAgent::keyConfigBackupTag];
        newTask->params[BackupAgent::keyConfigLogUid] =
            task->params[BackupAgent::keyConfigLogUid];
        newTask->params[BackupAgent::keyErrors] =
            task->params[BackupAgent::keyErrors];
        std::string key = taskBucket->addTask(tr, newTask);

        // TraceEvent("BA_BackupLogsTaskFunc_addTask")
        //	.detail("type",
        // printable(KeyRef(newTask->params[Task::reservedTaskParamKeyType])))
        //	.detail("onDone", joinedFutureKey)
        //	.detail("folder_id",
        // printable(KeyRef(newTask->params[BackupAgent::keyFolderId])))
        //	.detail("logUid",
        // systemDecompressUid(newTask->params[BackupAgent::keyConfigLogUid]))
        //	.detail("logUidValue",
        // printable(StringRef(newTask->params[BackupAgent::keyConfigLogUid])))
        //	.detail("beginVersion",
        // printable(StringRef(newTask->params[BackupAgent::keyBeginVersion])))
        //	.detail("endVersion",
        // printable(StringRef(newTask->params[BackupAgent::keyEndVersion])));

        return Void();
    }

    ACTOR static Future<Void> startBackupLogRangeInternal(
        Reference<ReadYourWritesTransaction> tr,
        Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket,
        Reference<Task> task, Reference<TaskFuture> taskFuture,
        Version beginVersion, Version endVersion)
    {
        tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

        std::vector<Future<Void>> addTaskVector;
        int tasks = 0;
        for (int64_t vblock = beginVersion / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE;
             vblock < (endVersion + CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE - 1) /
                          CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE;
             vblock += CLIENT_KNOBS->BACKUP_MAX_LOG_RANGES) {
            Version bv = std::max(beginVersion,
                                  vblock * CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE);

            if (tasks >= CLIENT_KNOBS->BACKUP_SHARD_TASK_LIMIT) {
                addTaskVector.push_back(addTask(
                    tr, taskBucket, task, taskFuture, format("%lld", bv),
                    format("%lld", endVersion), ""));
                break;
            }

            Version ev = std::min(
                endVersion, (vblock + CLIENT_KNOBS->BACKUP_MAX_LOG_RANGES) *
                                CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE);
            addTaskVector.push_back(addTask(tr, taskBucket, task, taskFuture,
                                            format("%lld", bv),
                                            format("%lld", ev), ""));
            tasks++;
        }

        Void _uvar = wait(waitForAll(addTaskVector));

        return Void();
    }

    ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
                                      Reference<TaskBucket> taskBucket,
                                      Reference<FutureBucket> futureBucket,
                                      Reference<Task> task)
    {
        ASSERT(taskBucket);
        ASSERT(futureBucket);
        ASSERT(task);

        // TraceEvent("BA_BackupLogRangeTaskFunc_finish
        // start").detail("beginVersion", getTaskParamVersion(task,
        // BackupAgent::keyBeginVersion))
        //	.detail("endVersion", getTaskParamVersion(task,
        // BackupAgent::keyEndVersion))
        //	.detail("logUid",
        // systemDecompressUid(StringRef(task->params[BackupAgent::keyConfigLogUid])))
        //	.detail("logUidValue",
        // printable(StringRef(task->params[BackupAgent::keyConfigLogUid])));

        tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
        Void _uvar = wait(checkVersion(tr));

        state Version beginVersion =
            getTaskParamVersion(task, BackupAgent::keyBeginVersion);
        state Version endVersion =
            getTaskParamVersion(task, BackupAgent::keyEndVersion);
        state Reference<TaskFuture> taskFuture =
            futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

        if (task->params.count("addBackupLogRangeTasks")) {
            Void _uvar = wait(startBackupLogRangeInternal(
                tr, taskBucket, futureBucket, task, taskFuture, beginVersion,
                endVersion));
            endVersion = beginVersion;
        } else if (task->params.count("nextBeginVersion")) {
            Void _uvar = wait(BackupLogRangeTaskFunc::addTask(
                tr, taskBucket, task, taskFuture,
                task->params["nextBeginVersion"],
                task->params[BackupAgent::keyEndVersion], taskFuture->key));
            endVersion = getTaskParamVersion(task, "nextBeginVersion");
        } else {
            Void _uvar = wait(taskFuture->set(tr, taskBucket));
        }

        if (endVersion > beginVersion) {
            Standalone<VectorRef<KeyRangeRef>> ranges =
                getLogRanges(beginVersion, endVersion,
                             task->params[BackupAgent::keyConfigLogUid]);
            for (auto& rng : ranges)
                tr->clear(rng);
        }

        TraceEvent("BA_BackupLogRangeTaskFunc_finish")
            .detail("beginVersion", beginVersion)
            .detail("endVersion", endVersion)
            .detail("logUid", systemDecompressUid(StringRef(
                                  task->params[BackupAgent::keyConfigLogUid])))
            .detail("logUidValue",
                    printable(
                        StringRef(task->params[BackupAgent::keyConfigLogUid])));

        // finish this task itself
        Void _uvar = wait(taskBucket->finish(tr, task));

        return Void();
    }

    ACTOR static Future<Void> endFile(Database cx,
                                      Reference<TaskBucket> taskBucket,
                                      Reference<FutureBucket> futureBucket,
                                      Reference<Task> task,
                                      Reference<IAsyncFile> tempFile,
                                      std::string tempFileName,
                                      std::string logFileName)
    {
        try {
            if (tempFile) {
                Void _uvar = wait(
                    truncateCloseFile(cx, task->params[BackupAgent::keyErrors],
                                      logFileName, tempFile));
            }

            bool isFinished = wait(taskBucket->isFinished(cx, task));
            if (isFinished)
                return Void();

            // rename file
            TraceEvent("BA_BackupLogRangeTaskFunc_endFile")
                .detail("Rename_file_from", tempFileName)
                .detail("to_new_file", logFileName);
            renameFile(tempFileName, logFileName);
        } catch (Error& e) {
            TraceEvent(SevError, "BA_BackupLogRangeTaskFunc_endFileError")
                .error(e)
                .detail("Rename_file_from", tempFileName);
            throw;
        }

        return Void();
    }
};
const char* BackupLogRangeTaskFunc::name = "backup_log_range";
REGISTER_TASKFUNC(BackupLogRangeTaskFunc);

struct BackupLogsTaskFunc : TaskFuncBase
{
    static const char* name;
    static const uint32_t version = 1;

    ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
                                      Reference<TaskBucket> taskBucket,
                                      Reference<FutureBucket> futureBucket,
                                      Reference<Task> task)
    {
        ASSERT(taskBucket);
        ASSERT(futureBucket);
        ASSERT(task);

        // check task version
        uint32_t taskVersion = task->getVersion();
        if (taskVersion > BackupLogsTaskFunc::version) {
            TraceEvent(SevError, "BA_BackupLogsTaskFunc_finish")
                .detail("taskVersion", taskVersion)
                .detail("is_greater_than_the_BackupLogsTaskFunc_version",
                        BackupLogsTaskFunc::version);
            Void _uvar = wait(logError(
                tr, task->params[BackupAgent::keyErrors],
                format("ERROR: %s task version `%lu' is greater than supported "
                       "version `%lu'",
                       task->params[Task::reservedTaskParamKeyType].c_str(),
                       (unsigned long)taskVersion,
                       (unsigned long)BackupLogsTaskFunc::version)));
            throw task_invalid_version();
        }

        state Reference<TaskFuture> onDone =
            futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

        tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

        state Optional<Value> stopValue =
            wait(tr->get(KeyRef(task->params[BackupAgent::keyStateStop])));
        Optional<Value> stopWhenDone = wait(tr->get(
            StringRef(task->params[BackupAgent::keyConfigStopWhenDoneKey])));
        bool stop(false);

        TraceEvent("BA_BackupLogsTaskFunc_finish")
            .detail("stopValue", printable(stopValue))
            .detail("stopWhenDone", printable(stopWhenDone))
            .detail(
                "stopValueKey",
                printable(StringRef(task->params[BackupAgent::keyStateStop])))
            .detail("stopWhenDoneKey",
                    printable(StringRef(
                        task->params[BackupAgent::keyConfigStopWhenDoneKey])));

        // Only consider stopping, if the stop key is set
        if (stopValue.present() &&
            (stopValue.get().compare(LiteralStringRef("1")) == 0)) {
            // Stop, if stopWhenDone is enabled (not performing differential
            // backups)
            if (stopWhenDone.present() &&
                (stopWhenDone.get().compare(LiteralStringRef("1")) == 0)) {
                TraceEvent("BA_BackupLogsTaskFunc_finish stopping")
                    .detail("stopValue", printable(stopValue))
                    .detail("stopWhenDone", printable(stopValue));
                stop = true;
            }
            // Set the status to differential
            else {
                tr->set(task->params[BackupAgent::keyStateStatus],
                        StringRef(BackupAgent::getStateText(
                            BackupAgent::STATE_DIFFERENTIAL)));
                tr->set(task->params[BackupAgent::keyStateGlobalStatus],
                        StringRef(BackupAgent::getStateText(
                            BackupAgent::STATE_DIFFERENTIAL)));
            }
        }

        // TraceEvent("BA_BackupLogsTaskFunc").detail("getStopKey",
        // printable(KeyRef(states.getItem(BackupAgent::keyStateStop).key()))).detail("stop",
        // stop);
        Version beginVersion =
            getTaskParamVersion(task, BackupAgent::keyBeginVersion);
        state Version endVersion = std::max<Version>(
            tr->getReadVersion().get() + 1,
            beginVersion + (CLIENT_KNOBS->BACKUP_MAX_LOG_RANGES - 1) *
                               CLIENT_KNOBS->CORE_VERSIONSPERSECOND);

        state Reference<TaskFuture> allPartsDone;
        if (stop) {
            TraceEvent("BA_BackupLogsTaskFuncStop")
                .detail("Log_backup_stopping_at_version", endVersion)
                .detail("keyStateStopVersion",
                        printable(StringRef(
                            task->params[BackupAgent::keyStateStopVersion])));
            allPartsDone = onDone;
            FDBTuple t;
            t.append(endVersion - 1);
            tr->set(task->params[BackupAgent::keyStateStopVersion], t.get());
        } else {
            TraceEvent("BA_BackupLogsTaskFuncContinue")
                .detail("beginVersion",
                        task->params[BackupAgent::keyBeginVersion])
                .detail("endVersion", endVersion);
            allPartsDone = futureBucket->future(tr);

            Void _uvar = wait(BackupLogsTaskFunc::addTask(
                tr, taskBucket, allPartsDone,
                task->params[BackupAgent::keyFolderId],
                task->params[BackupAgent::keyStateStatus],
                task->params[BackupAgent::keyStateGlobalStatus],
                task->params[BackupAgent::keyStateStop],
                task->params[BackupAgent::keyStateStopVersion],
                task->params[BackupAgent::keyOutputDir],
                task->params[BackupAgent::keyConfigBackupTag],
                task->params[BackupAgent::keyConfigLogUid],
                task->params[BackupAgent::keyConfigStopWhenDoneKey],
                task->params[BackupAgent::keyErrors],
                format("%lld", endVersion), onDone->key, false));
        }

        Void _uvar = wait(BackupLogRangeTaskFunc::addTask(
            tr, taskBucket, task, allPartsDone,
            task->params[BackupAgent::keyBeginVersion],
            format("%lld", endVersion), ""));

        // TraceEvent("BA_BackupLogsTaskFunc_finish").detail("type",
        // task->params[Task::reservedTaskParamKeyType]).detail("key",
        // task->key).detail("beginVersion",
        // task->params[BackupAgent::keyBeginVersion]);
        // finish this task itself
        Void _uvar = wait(taskBucket->finish(tr, task));

        return Void();
    }

    ACTOR static Future<Void> addTask(
        Reference<ReadYourWritesTransaction> tr,
        Reference<TaskBucket> taskBucket, Reference<TaskFuture> taskFuture,
        std::string folderId, std::string statusKey,
        std::string globalStatusKey, std::string stopKey,
        std::string stopVersionKey, std::string outputDirKey,
        std::string backupTag, std::string logUidValue,
        std::string stopWhenDoneKey, std::string errorsKey,
        std::string beginVersion, std::string joinedFutureKey,
        bool addImmediately)
    {

        ASSERT(taskBucket);
        ASSERT(taskFuture);

        // Create a joined future, if not specified
        if (joinedFutureKey.empty()) {
            Reference<TaskFuture> newOnDone =
                wait(taskFuture->joinedFuture(tr, taskBucket));
            // TraceEvent("BA_BackupLogRangeTaskFunc_addTaskCreatedFuture").detail("futureKey",
            // newOnDone->key);
            joinedFutureKey = newOnDone->key;
        }

        state std::string key;
        state Reference<Task> task(new Task(BackupLogsTaskFunc::name,
                                            BackupLogsTaskFunc::version,
                                            joinedFutureKey));

        task->params[BackupAgent::keyFolderId] = folderId;
        task->params[BackupAgent::keyStateStatus] = statusKey;
        task->params[BackupAgent::keyStateGlobalStatus] = globalStatusKey;
        task->params[BackupAgent::keyStateStop] = stopKey;
        task->params[BackupAgent::keyStateStopVersion] = stopVersionKey;
        task->params[BackupAgent::keyOutputDir] = outputDirKey;
        task->params[BackupAgent::keyConfigBackupTag] = backupTag;
        task->params[BackupAgent::keyConfigLogUid] = logUidValue;
        task->params[BackupAgent::keyConfigStopWhenDoneKey] = stopWhenDoneKey;
        task->params[BackupAgent::keyErrors] = errorsKey;
        task->params[BackupAgent::keyBeginVersion] = beginVersion;

        if (addImmediately) {
            key = taskBucket->addTask(tr, task);
        } else {
            Void _uvar = wait(taskFuture->onSetAddTask(tr, taskBucket, task));
            key = "OnSetAddTask";
        }

        // TraceEvent("BA_BackupLogRangeTaskFunc_addTask").detail("type",
        // task->params[Task::reservedTaskParamKeyType])
        //	.detail("key", key)
        //	.detail("onDone", joinedFutureKey)
        //	.detail("folderId", folderId)
        //	.detail("logUid", systemDecompressUid(logUidValue))
        //	.detail("output_dir", outputDirKey)
        //	.detail("tagName", printable(StringRef(backupTag)))
        //	.detail("stopVersionKey", printable(StringRef(stopVersionKey)))
        //	.detail("beginVersion", printable(StringRef(beginVersion)))
        //	.detail("logUidValue", printable(StringRef(logUidValue)));

        return Void();
    }

    const char* getName() const override { return name; };
    Future<Void> execute(Database cx, Reference<TaskBucket> tb,
                         Reference<FutureBucket> fb,
                         Reference<Task> task) override
    {
        return Void();
    };
    Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
                        Reference<TaskBucket> tb, Reference<FutureBucket> fb,
                        Reference<Task> task) override
    {
        return _finish(tr, tb, fb, task);
    };
};
const char* BackupLogsTaskFunc::name = "backup_logs";
REGISTER_TASKFUNC(BackupLogsTaskFunc);

struct FinishedFullBackupTaskFunc : TaskFuncBase
{
    static const char* name;
    static const uint32_t version = 1;

    const char* getName() const override { return name; };

    Future<Void> execute(Database cx, Reference<TaskBucket> tb,
                         Reference<FutureBucket> fb,
                         Reference<Task> task) override
    {
        return Void();
    };

    ACTOR static Future<Version> getStoppedVersion(
        Reference<ReadYourWritesTransaction> tr, Reference<Task> task)
    {
        tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
        Optional<Value> stopVersion = wait(
            tr->get(KeyRef(task->params[BackupAgent::keyStateStopVersion])));
        if (stopVersion.present()) {
            FDBTuple t(stopVersion.get().toString());
            return t.get_int(0);
        }
        return -1;
    }

    ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
                                      Reference<TaskBucket> taskBucket,
                                      Reference<FutureBucket> futureBucket,
                                      Reference<Task> task)
    {
        ASSERT(taskBucket);
        ASSERT(futureBucket);
        ASSERT(task);

        tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

        // check task version
        uint32_t taskVersion = task->getVersion();
        if (taskVersion > FinishedFullBackupTaskFunc::version) {
            TraceEvent(SevError, "BA_FinishedFullBackupTaskFunc")
                .detail("taskVersion", taskVersion)
                .detail(
                    "is_greater_than_the_FinishedFullBackupTaskFunc_version",
                    FinishedFullBackupTaskFunc::version);
            Void _uvar = wait(logError(
                tr, task->params[BackupAgent::keyErrors],
                format("ERROR: %s task version `%lu' is greater than supported "
                       "version `%lu'",
                       task->params[Task::reservedTaskParamKeyType].c_str(),
                       (unsigned long)taskVersion,
                       (unsigned long)FinishedFullBackupTaskFunc::version)));
            throw task_invalid_version();
        }

        state Version stopVersion = wait(getStoppedVersion(tr, task));
        state std::string path = wait(getAndMakePath(tr, task)); // fileExists
        state std::string filename = joinPath(path, "restorable");
        state std::string tempFile =
            joinPath(path, BackupAgent::getTempFilename());
        state Reference<IAsyncFile> f = wait(
            openBackupFile(tempFile, task->params[BackupAgent::keyErrors], tr));
        state UID logUid = systemDecompressUid(
            StringRef(task->params[BackupAgent::keyConfigLogUid]));

        if (f) {
            state int64_t offset = wait(f->size());
            state std::string msg;

            TraceEvent(SevInfo, "BA_FinishFileWrite")
                .detail("tempfile", tempFile);

            msg += format("%-15s %ld\n", "fdbbackupver:", backupVersion);
            msg += format("%-15s %lld\n", "completever:", stopVersion);
            msg += format(
                "%-15s %s\n", "tag:",
                printable(
                    StringRef(task->params[BackupAgent::keyConfigBackupTag]))
                    .c_str());
            msg += format("%-15s %s\n", "logUid:", logUid.toString().c_str());
            msg += format(
                "%-15s %s\n", "logUidValue:",
                printable(StringRef(task->params[BackupAgent::keyConfigLogUid]))
                    .c_str());

            // Deserialize the backup ranges
            state Standalone<VectorRef<KeyRangeRef>> backupRanges;
            BinaryReader br(task->params[BackupAgent::keyConfigBackupRanges],
                            IncludeVersion());
            serializer(br, v1(backupRanges));

            msg += format("%-15s %d\n", "ranges:", backupRanges.size());

            for (auto& backupRange : backupRanges) {
                msg +=
                    format("%-15s %s\n",
                           "rangebegin:", printable(backupRange.begin).c_str());
                msg += format("%-15s %s\n",
                              "rangeend:", printable(backupRange.end).c_str());
            }

            msg += format("%-15s %s\n",
                          "time:", BackupAgent::getCurrentTime().c_str());
            msg += format("%-15s %lld\n", "timesecs:", (long long)now());

            // Variable boundary
            msg += "\n\n----------------------------------------------------"
                   "\n\n\n";

            msg += format(
                "This backup can be restored beginning at version %lld.\n",
                stopVersion);

            // Write the message to the file
            try {
                f->write(msg.c_str(), msg.size(), offset);
            } catch (Error& e) {
                if (e.code() == error_code_actor_cancelled)
                    throw;

                state Error err = e;
                Void _uvar = wait(logError(
                    tr->getDatabase(), task->params[BackupAgent::keyErrors],
                    format("ERROR: Failed to write to file "
                           "`%s' because of error %s",
                           filename.c_str(), err.what())));

                throw err;
            }
            Void _uvar = wait(truncateCloseFile(
                tr->getDatabase(), task->params[BackupAgent::keyErrors],
                filename, f, offset + msg.size()));
        }
        renameFile(tempFile, filename);

        // Clear the backup configuration and log ranges within the system keys
        state Key configPath = uidPrefixKey(logRangesRange.begin, logUid);
        state Key logsPath = uidPrefixKey(backupLogKeys.begin, logUid);

        // Clear the backup configuration
        tr->clear(KeyRangeRef(configPath, strinc(configPath)));

        // Clear the backup log mutations
        tr->clear(KeyRangeRef(logsPath, strinc(logsPath)));

        // Update the status
        tr->set(
            task->params[BackupAgent::keyStateStatus],
            StringRef(BackupAgent::getStateText(BackupAgent::STATE_COMPLETED)));
        tr->set(
            task->params[BackupAgent::keyStateGlobalStatus],
            StringRef(BackupAgent::getStateText(BackupAgent::STATE_COMPLETED)));

        // finish this task itself
        Void _uvar = wait(taskBucket->finish(tr, task));

        TraceEvent("BA_FinishedFullBackupTaskFunc_keyClear")
            .detail("Full_backup_completed", true)
            .detail("stopVersion", stopVersion)
            .detail("logId", logUid)
            .detail("configRangeBegin", printable(configPath))
            .detail("configRangeEnd", printable(strinc(configPath)))
            .detail("logsRangeBegin", printable(logsPath))
            .detail("logRangeEnd", printable(strinc(logsPath)));

        return Void();
    }

    Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
                        Reference<TaskBucket> tb, Reference<FutureBucket> fb,
                        Reference<Task> task) override
    {
        return _finish(tr, tb, fb, task);
    };
};
const char* FinishedFullBackupTaskFunc::name = "finished_full_backup";
REGISTER_TASKFUNC(FinishedFullBackupTaskFunc);

struct StartFullBackupTaskFunc : TaskFuncBase
{
    static const char* name;
    static const uint32_t version = 1;

    ACTOR static Future<Void> _execute(Database cx,
                                       Reference<TaskBucket> taskBucket,
                                       Reference<FutureBucket> futureBucket,
                                       Reference<Task> task)
    {
        state FlowLock lock(CLIENT_KNOBS->BACKUP_LOCK_BYTES);
        ASSERT(taskBucket);
        ASSERT(futureBucket);
        ASSERT(task);

        state UID randomID = g_random->randomUniqueID();

        // check task version
        uint32_t taskVersion = task->getVersion();
        if (taskVersion > StartFullBackupTaskFunc::version) {
            TraceEvent(SevError, "BA_StartFullBackupTaskFunc")
                .detail("taskVersion", taskVersion)
                .detail("is_greater_than_the_StartFullBackupTaskFunc_version",
                        StartFullBackupTaskFunc::version);
            Void _uvar = wait(logError(
                cx, task->params[BackupAgent::keyErrors],
                format("ERROR: %s task version `%lu' is greater than supported "
                       "version `%lu'",
                       task->params[Task::reservedTaskParamKeyType].c_str(),
                       (unsigned long)taskVersion,
                       (unsigned long)StartFullBackupTaskFunc::version)));
            throw task_invalid_version();
        }

        loop
        {
            state Reference<ReadYourWritesTransaction> tr(
                new ReadYourWritesTransaction(cx));

            // Set the start version
            try {
                Version startVersion = wait(tr->getReadVersion());

                task->params[BackupAgent::keyBeginVersion] =
                    format("%lld", startVersion);
                TraceEvent("BA_StartFullBackupTaskFunc_execute")
                    .detail("beginVersion", startVersion);
                break;
            } catch (Error& e) {
                Void _uvar = wait(tr->onError(e));
            }
        }

        return Void();
    }

    ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
                                      Reference<TaskBucket> taskBucket,
                                      Reference<FutureBucket> futureBucket,
                                      Reference<Task> task)
    {
        ASSERT(taskBucket);
        ASSERT(futureBucket);
        ASSERT(task);

        tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

        // Deserialize the backup ranges
        state Standalone<VectorRef<KeyRangeRef>> backupRanges;
        state StringRef logUidValue =
            StringRef(task->params[BackupAgent::keyConfigLogUid]);
        state UID logUid = systemDecompressUid(logUidValue);
        state Key logUidDest = uidPrefixKey(backupLogKeys.begin, logUid);

        TraceEvent("BA_StartFullBackupTaskFuncBegin")
            .detail("RunningTask", task->params[Task::reservedTaskParamKeyType])
            .detail("version", task->params[Task::reservedTaskParamKeyVersion])
            .detail("logUid", logUid)
            .detail("logUidValue", printable(logUidValue))
            .detail("logUidDest", printable(logUidDest))
            .detail("backupTag",
                    printable(StringRef(
                        task->params[BackupAgent::keyConfigBackupTag])))
            .detail(
                "backup_output",
                printable(StringRef(task->params[BackupAgent::keyOutputDir])));

        BinaryReader br(task->params[BackupAgent::keyConfigBackupRanges],
                        IncludeVersion());
        serializer(br, v1(backupRanges));

        // Start logging the mutations for the specified ranges of the tag
        for (auto& backupRange : backupRanges) {
            TraceEvent("BackupMutationSet")
                .detail("rangeBegin", printable(backupRange.begin))
                .detail("rangeEnd", printable(backupRange.end));

            tr->set(logRangesEncodeKey(backupRange.begin, logUid),
                    logRangesEncodeValue(backupRange.end, logUidDest));
        }

        // Update the status
        tr->set(
            task->params[BackupAgent::keyStateStatus],
            StringRef(BackupAgent::getStateText(BackupAgent::STATE_BACKUP)));
        tr->set(
            task->params[BackupAgent::keyStateGlobalStatus],
            StringRef(BackupAgent::getStateText(BackupAgent::STATE_BACKUP)));

        state Reference<TaskFuture> kvBackupRangeComplete =
            futureBucket->future(tr);
        state Reference<TaskFuture> kvBackupComplete = futureBucket->future(tr);
        state int rangeCount = 0;

        // Backup each of the data ranges
        for (; rangeCount < backupRanges.size(); ++rangeCount) {
            Void _uvar = wait(BackupRangeTaskFunc::addTask(
                tr, taskBucket, kvBackupRangeComplete,
                task->params[BackupAgent::keyFolderId],
                backupRanges[rangeCount].begin.toString(),
                backupRanges[rangeCount].end.toString(),
                task->params[BackupAgent::keyOutputDir],
                task->params[BackupAgent::keyErrors], ""));
        }

        // Backup the logs which will create BackupLogRange tasks
        Void _uvar = wait(BackupLogsTaskFunc::addTask(
            tr, taskBucket, kvBackupComplete,
            task->params[BackupAgent::keyFolderId],
            task->params[BackupAgent::keyStateStatus],
            task->params[BackupAgent::keyStateGlobalStatus],
            task->params[BackupAgent::keyStateStop],
            task->params[BackupAgent::keyStateStopVersion],
            task->params[BackupAgent::keyOutputDir],
            task->params[BackupAgent::keyConfigBackupTag],
            task->params[BackupAgent::keyConfigLogUid],
            task->params[BackupAgent::keyConfigStopWhenDoneKey],
            task->params[BackupAgent::keyErrors],
            task->params[BackupAgent::keyBeginVersion], "", true));

        // After the BackupRangeTask completes, set the stop key which will stop
        // the BackupLogsTask
        Reference<Task> taskFinishFullBackup(new Task(
            FinishFullBackupTaskFunc::name, FinishFullBackupTaskFunc::version));
        taskFinishFullBackup->params[BackupAgent::keyStateStop] =
            task->params[BackupAgent::keyStateStop];
        taskFinishFullBackup->params[BackupAgent::keyConfigBackupTag] =
            task->params[BackupAgent::keyConfigBackupTag];
        taskFinishFullBackup->params[BackupAgent::keyConfigLogUid] =
            task->params[BackupAgent::keyConfigLogUid];
        taskFinishFullBackup->params[BackupAgent::keyFolderId] =
            task->params[BackupAgent::keyFolderId];
        taskFinishFullBackup->params[BackupAgent::keyErrors] =
            task->params[BackupAgent::keyErrors];
        Void _uvar = wait(kvBackupRangeComplete->onSetAddTask(
            tr, taskBucket, taskFinishFullBackup));

        Reference<Task> taskFinishedFullBackup(
            new Task(FinishedFullBackupTaskFunc::name,
                     FinishedFullBackupTaskFunc::version));
        taskFinishedFullBackup->params[BackupAgent::keyStateStatus] =
            task->params[BackupAgent::keyStateStatus];
        taskFinishedFullBackup->params[BackupAgent::keyStateGlobalStatus] =
            task->params[BackupAgent::keyStateGlobalStatus];
        taskFinishedFullBackup->params[BackupAgent::keyStateStopVersion] =
            task->params[BackupAgent::keyStateStopVersion];
        taskFinishedFullBackup->params[BackupAgent::keyOutputDir] =
            task->params[BackupAgent::keyOutputDir];
        taskFinishedFullBackup->params[BackupAgent::keyConfigBackupTag] =
            task->params[BackupAgent::keyConfigBackupTag];
        taskFinishedFullBackup->params[BackupAgent::keyConfigLogUid] =
            task->params[BackupAgent::keyConfigLogUid];
        taskFinishedFullBackup->params[BackupAgent::keyConfigBackupRanges] =
            task->params[BackupAgent::keyConfigBackupRanges];
        taskFinishedFullBackup->params[BackupAgent::keyFolderId] =
            task->params[BackupAgent::keyFolderId];
        taskFinishedFullBackup->params[BackupAgent::keyErrors] =
            task->params[BackupAgent::keyErrors];
        Void _uvar = wait(kvBackupComplete->onSetAddTask(
            tr, taskBucket, taskFinishedFullBackup));

        // finish this task itself
        Void _uvar = wait(taskBucket->finish(tr, task));

        TraceEvent("BA_StartFullBackupTaskFuncEnd")
            .detail("logUid", logUid)
            .detail("backupTag",
                    printable(StringRef(
                        task->params[BackupAgent::keyConfigBackupTag])))
            .detail("keyFolderId", task->params[BackupAgent::keyFolderId])
            .detail(
                "backup_output",
                printable(StringRef(task->params[BackupAgent::keyOutputDir])));

        return Void();
    }

    const char* getName() const override { return name; };
    Future<Void> execute(Database cx, Reference<TaskBucket> tb,
                         Reference<FutureBucket> fb,
                         Reference<Task> task) override
    {
        return _execute(cx, tb, fb, task);
    };
    Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
                        Reference<TaskBucket> tb, Reference<FutureBucket> fb,
                        Reference<Task> task) override
    {
        return _finish(tr, tb, fb, task);
    };
};
const char* StartFullBackupTaskFunc::name = "start_full_backup";
REGISTER_TASKFUNC(StartFullBackupTaskFunc);

struct LogInfo : public ReferenceCounted<LogInfo>
{
    std::string logPath;
    Reference<IAsyncFile> logFile;
    Version beginVersion;
    Version endVersion;
    int64_t offset;

    LogInfo()
      : offset(0){};
};

class BackupAgentImpl
{
public:
    ACTOR static Future<Void> run(BackupAgent* backupAgent, Database cx)
    {

        loop
        {
            bool oneTaskDone = wait(
                backupAgent->taskBucket->doOne(cx, backupAgent->futureBucket));
            if (!oneTaskDone) {
                bool isEmpty = wait(backupAgent->taskBucket->isEmpty(cx));
                if (isEmpty) {
                    Void _uvar = wait(delay(CLIENT_KNOBS->BACKUP_EMPTY_DELAY));
                    if (backupAgent->exitWhenNoTask) {
                        state bool isFutureEmpty =
                            wait(backupAgent->futureBucket->isEmpty(cx));
                        if (isFutureEmpty)
                            break;
                    }
                } else {
                    Void _uvar =
                        wait(delay(CLIENT_KNOBS->BACKUP_NON_EMPTY_DELAY));
                }
            }
        }

        return Void();
    }

    // This method will return the final status of the backup
    ACTOR static Future<int> waitBackup(BackupAgent* backupAgent, Database cx,
                                        Key tagName, bool stopWhenDone)
    {
        ASSERT(backupAgent);
        state std::string backTrace;
        state UID randomID = g_random->randomUniqueID();

        state UID logUid = wait(backupAgent->getLogUid(cx, tagName));

        state Key statusKey = StringRef(
            backupAgent->getStatesKey(logUid, BackupAgent::keyStateStatus));

        // TraceEvent("BA_waitBackupBegin", randomID).detail("stopWhenDone",
        // stopWhenDone).detail("tag", printable(tagName)).detail("logUid",
        // logUid);

        loop
        {
            state Reference<ReadYourWritesTransaction> tr(
                new ReadYourWritesTransaction(cx));

            tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

            try {
                state int status = wait(backupAgent->getStateValue(tr, logUid));

                // Break, if no longer runnable
                if (!BackupAgent::isRunnable((BackupAgent::enumState)status)) {
                    TraceEvent("BA_waitBackup_return", randomID)
                        .detail("stopWhenDone", stopWhenDone)
                        .detail("tag", printable(tagName))
                        .detail("status", BackupAgent::getStateText(
                                              (BackupAgent::enumState)status))
                        .detail("statusCode", status);
                    return status;
                }

                // Break, if in differential mode (restorable) and stopWhenDone
                // is not enabled
                if ((!stopWhenDone) &&
                    (BackupAgent::STATE_DIFFERENTIAL == status)) {
                    TraceEvent("BA_waitBackup_return", randomID)
                        .detail("stopWhenDone", stopWhenDone)
                        .detail("tag", printable(tagName))
                        .detail("status", BackupAgent::getStateText(
                                              (BackupAgent::enumState)status))
                        .detail("statusCode", status);
                    return status;
                }

                state Future<Void> watchFuture = tr->watch(statusKey);
                Void _uvar = wait(tr->commit());

                TraceEvent("BA_waitBackup_watch", randomID)
                    .detail("stopWhenDone", stopWhenDone)
                    .detail("tag", printable(tagName))
                    .detail("status", BackupAgent::getStateText(
                                          (BackupAgent::enumState)status))
                    .detail("statusCode", status)
                    .detail("statusKey", printable(statusKey))
                    .detail("logUid", logUid);

                Void _uvar = wait(watchFuture);
            } catch (Error& e) {
                Void _uvar = wait(tr->onError(e));
            }
        }
    }

    ACTOR static Future<Void> submitBackup(
        BackupAgent* backupAgent, Reference<ReadYourWritesTransaction> tr,
        Key outPath, Key tagName,
        Standalone<VectorRef<KeyRangeRef>> backupRanges, bool stopWhenDone)
    {
        state UID logUid = g_random->randomUniqueID();
        state Key logUidValue = systemCompressUid(logUid);
        state UID logUidCurrent = wait(backupAgent->getLogUid(tr, tagName));

        ASSERT(backupAgent);

        tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

        // We will use the global status for now to ensure that multiple backups
        // do not start place with different tags
        state int globalStatus = wait(backupAgent->getGlobalStateValue(tr));
        state int status = wait(backupAgent->getStateValue(tr, logUidCurrent));

        TraceEvent("BA_SubmitBackupBegin", logUid)
            .detail("statusCode", status)
            .detail("status",
                    BackupAgent::getStateText((BackupAgent::enumState)status))
            .detail("globalStatusCode", globalStatus)
            .detail("globalStatus", BackupAgent::getStateText(
                                        (BackupAgent::enumState)globalStatus))
            .detail("tag", printable(tagName))
            .detail("outPath", printable(outPath))
            .detail("stopWhenDone", stopWhenDone)
            .detail("logUid", logUid)
            .detail("logUidCurrent", logUidCurrent);

        if (BackupAgent::isRunnable((BackupAgent::enumState)globalStatus)) {
            throw backup_duplicate();
        }

        // Use the existing logUid, if present
        if (logUidCurrent.isValid()) {
            TraceEvent("BA_SubmitBackupReuseLogUid", logUid)
                .detail("logUid", logUid)
                .detail("logUidCurrent", logUidCurrent)
                .detail("statusCode", status)
                .detail("status", BackupAgent::getStateText(
                                      (BackupAgent::enumState)status));
            logUid = logUidCurrent;
            logUidValue = systemCompressUid(logUid);
        }

        // This check will ensure that current backupUid is later than the last
        // backup Uid
        state std::string backupUid = BackupAgent::getCurrentTime();
        Optional<Value> uidOld = wait(tr->get(StringRef(
            backupAgent->config.getItem(BackupAgent::keyLastUid).key())));

        if ((uidOld.present()) && (uidOld.get().toString() >= backupUid)) {
            fprintf(stderr,
                    "ERROR: The last backup `%s' happened in the future.\n",
                    printable(uidOld.get()).c_str());
            throw backup_error();
        }

        // Simplify the backup ranges
        KeyRangeMap<int> backupRangeSet;
        for (auto& backupRange : backupRanges) {
            backupRangeSet.insert(backupRange, 1);
        }

        // Compact the entire range
        backupRangeSet.coalesce(allKeys);

        // Clear the current backup ranges
        backupRanges = Standalone<VectorRef<KeyRangeRef>>();

        // Add the non-empty coalesced ranges to the key range array
        for (auto& backupRange : backupRangeSet.ranges()) {
            if (backupRange.value()) {
                backupRanges.push_back_deep(backupRanges.arena(),
                                            backupRange.range());
                TraceEvent("BA_startBackup_RangePost", logUid)
                    .detail("rangeBegin", printable(backupRange.begin()))
                    .detail("rangeEnd", printable(backupRange.end()))
                    .detail("tagName", printable(KeyRef(tagName)))
                    .detail("index", backupRanges.size())
                    .detail("logUid", logUid);
            }
        }

        // TODO: for parallel backup the following clears should be limitted to
        // the UID range

        // Clear the task bucket
        backupAgent->taskBucket->clear(tr);
        backupAgent->futureBucket->clear(tr);

        // Clear the backup ranges for the tag
        tr->clear(backupAgent->states.range());
        tr->clear(backupAgent->config.range());
        tr->clear(backupAgent->errors.range());

        // Map the logUid to the tagname
        tr->set(backupAgent->tagNames.getItem(tagName.toString()).key(),
                logUidValue.toString());

        // Set the global keys
        tr->set(backupAgent->states.getItem(BackupAgent::keyStateGlobalStatus)
                    .key(),
                std::string(
                    BackupAgent::getStateText(BackupAgent::STATE_SUBMITTED)));
        tr->set(backupAgent->config.getItem(BackupAgent::keyLastUid).key(),
                backupUid);

        // Set the backup keys
        tr->set(backupAgent->getConfigKey(logUid, BackupAgent::keyOutputDir),
                outPath.toString());
        tr->set(
            backupAgent->getConfigKey(logUid, BackupAgent::keyConfigBackupTag),
            tagName.toString());
        tr->set(backupAgent->getConfigKey(logUid, BackupAgent::keyConfigLogUid),
                logUidValue.toString());
        tr->set(backupAgent->getConfigKey(logUid, BackupAgent::keyFolderId),
                backupUid);
        tr->set(backupAgent->getConfigKey(
                    logUid, BackupAgent::keyConfigStopWhenDoneKey),
                stopWhenDone ? LiteralStringRef("1") : LiteralStringRef("0"));
        tr->set(backupAgent->getConfigKey(logUid,
                                          BackupAgent::keyConfigBackupRanges),
                BinaryWriter::toValue(backupRanges, IncludeVersion()));
        tr->set(backupAgent->getStatesKey(logUid, BackupAgent::keyStateStatus),
                std::string(
                    BackupAgent::getStateText(BackupAgent::STATE_SUBMITTED)));
        tr->set(backupAgent->getStatesKey(logUid, BackupAgent::keyStateStop),
                LiteralStringRef("0"));

        state Reference<Task> task(new Task(StartFullBackupTaskFunc::name,
                                            StartFullBackupTaskFunc::version));
        task->params[BackupAgent::keyStateStatus] =
            backupAgent->getStatesKey(logUid, BackupAgent::keyStateStatus);
        task->params[BackupAgent::keyStateGlobalStatus] =
            backupAgent->states.getItem(BackupAgent::keyStateGlobalStatus)
                .key();
        task->params[BackupAgent::keyStateStop] =
            backupAgent->getStatesKey(logUid, BackupAgent::keyStateStop);
        task->params[BackupAgent::keyStateStopVersion] =
            backupAgent->getStatesKey(logUid, BackupAgent::keyStateStopVersion);
        task->params[BackupAgent::keyFolderId] = backupUid;
        task->params[BackupAgent::keyOutputDir] = outPath.toString();
        task->params[BackupAgent::keyConfigLogUid] = logUidValue.toString();
        task->params[BackupAgent::keyConfigBackupTag] = tagName.toString();
        task->params[BackupAgent::keyConfigBackupRanges] =
            BinaryWriter::toValue(backupRanges, IncludeVersion()).toString();
        task->params[BackupAgent::keyConfigStopWhenDoneKey] =
            backupAgent->getConfigKey(logUid,
                                      BackupAgent::keyConfigStopWhenDoneKey);
        task->params[BackupAgent::keyErrors] = backupAgent->getErrorKey(logUid);
        std::string taskKey = backupAgent->taskBucket->addTask(tr, task);

        TraceEvent("BA_SubmitBackupEnd", logUid)
            .detail("task", task->params[Task::reservedTaskParamKeyType])
            .detail("taskKey", taskKey)
            .detail("logUid", logUid)
            .detail("logUidValue", printable(logUidValue))
            .detail("statusCode", status)
            .detail("status",
                    BackupAgent::getStateText((BackupAgent::enumState)status))
            .detail("globalStatusCode", globalStatus)
            .detail("globalStatus", BackupAgent::getStateText(
                                        (BackupAgent::enumState)globalStatus))
            .detail("stopWhenDone", stopWhenDone);

        return Void();
    }

    ACTOR static Future<Void> discontinueBackup(
        BackupAgent* backupAgent, Reference<ReadYourWritesTransaction> tr,
        Key tagName)
    {
        state UID randomID = g_random->randomUniqueID();

        ASSERT(backupAgent);

        state UID logUid = wait(backupAgent->getLogUid(tr, tagName));
        int status = wait(backupAgent->getStateValue(tr, logUid));

        // TraceEvent("BA_DiscontinueBackupBegin",
        // randomID).detail("statusCode", status).detail("status",
        // BackupAgent::getStateText((BackupAgent::enumState)status));

        // Ensure that the backup is in a runnable state
        if (!BackupAgent::isRunnable((BackupAgent::enumState)status)) {
            Optional<Value> status =
                wait(tr->get(StringRef(backupAgent->getStatesKey(
                    logUid, BackupAgent::keyStateStatus))));
            TraceEvent("BA_DiscontinueBackupAlreadySet", randomID)
                .detail("status", printable(status));
            throw backup_unneeded();
        }

        // Get the current value of the stopWhenDone key
        state Optional<Value> stopWhenDoneValue =
            wait(tr->get(StringRef(backupAgent->getConfigKey(
                logUid, BackupAgent::keyConfigStopWhenDoneKey))));

        // If already enabled, do nothing
        if ((stopWhenDoneValue.present()) &&
            (stopWhenDoneValue.get().compare(LiteralStringRef("1")) == 0)) {
            TraceEvent("BA_DiscontinueBackupAlreadySet", randomID)
                .detail("stopWhenDone", printable(stopWhenDoneValue));
            throw backup_duplicate();
        }

        // Enable the stopwhenDone key
        tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
        tr->set(backupAgent->getConfigKey(
                    logUid, BackupAgent::keyConfigStopWhenDoneKey),
                LiteralStringRef("1"));

        TraceEvent("BA_DiscontinueBackupEnd", randomID)
            .detail("stopWhenDone", printable(stopWhenDoneValue));

        return Void();
    }

    ACTOR static Future<Void> abortBackup(
        BackupAgent* backupAgent, Reference<ReadYourWritesTransaction> tr,
        Key tagName)
    {
        ASSERT(backupAgent);

        state UID randomID = g_random->randomUniqueID();
        state UID logUid = wait(backupAgent->getLogUid(tr, tagName));
        int status = wait(backupAgent->getStateValue(tr, logUid));

        TraceEvent("BA_abortBackupBegin", randomID)
            .detail("statusCode", status)
            .detail("status",
                    BackupAgent::getStateText((BackupAgent::enumState)status))
            .detail("backupTag", printable(tagName));

        // Do nothing, if the backup is not currently running
        if (!backupAgent->isRunnable((BackupAgent::enumState)status)) {
            throw backup_unneeded();
        }

        // Clear the current and future tasks
        Void _uvar = wait(backupAgent->taskBucket->clear(tr));
        Void _uvar = wait(backupAgent->futureBucket->clear(tr));

        // Clear the backup tag ranges within the system keys
        state Key configPath = uidPrefixKey(logRangesRange.begin, logUid);
        state Key logsPath = uidPrefixKey(backupLogKeys.begin, logUid);

        // Clear the backup tags
        tr->clear(KeyRangeRef(configPath, strinc(configPath)));

        // Clear the backup values
        tr->clear(KeyRangeRef(logsPath, strinc(logsPath)));

        Key statusKey = StringRef(
            backupAgent->getStatesKey(logUid, BackupAgent::keyStateStatus));
        Key globalStatusKey = StringRef(
            backupAgent->states.getItem(BackupAgent::keyStateGlobalStatus)
                .key());

        tr->set(statusKey, StringRef(BackupAgent::getStateText(
                               BackupAgent::STATE_ABORTED)));
        tr->set(globalStatusKey, StringRef(BackupAgent::getStateText(
                                     BackupAgent::STATE_ABORTED)));

        TraceEvent("BA_abortBackup", randomID)
            .detail("tag", printable(tagName))
            .detail("logUid", logUid)
            .detail("configRangeBegin", printable(configPath))
            .detail("configRangeEnd", printable(strinc(configPath)))
            .detail("logsRangeBegin", printable(logsPath))
            .detail("logRangeEnd", printable(strinc(logsPath)))
            .detail("statusKey", printable(statusKey));

        return Void();
    }

    // Backup status
    ACTOR static Future<std::string> getStatus(BackupAgent* backupAgent,
                                               Database cx, int errorLimit,
                                               Key tagName)
    {
        state Reference<ReadYourWritesTransaction> tr(
            new ReadYourWritesTransaction(cx));
        state std::string statusText;

        loop
        {
            try {
                state UID randomID = g_random->randomUniqueID();
                state std::string backupStatus;
                state BackupAgent::enumState backupState;
                statusText = "";

                // Get the logUid for the default tag
                state UID logUid = wait(backupAgent->getLogUid(tr, tagName));

                tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
                state Optional<Value> status =
                    wait(tr->get(StringRef(backupAgent->getStatesKey(
                        logUid, BackupAgent::keyStateStatus))));

                // Determine the backup state
                if (status.present()) {
                    backupStatus = status.get().toString();
                }

                // Get the backup state
                backupState = BackupAgent::getState(backupStatus);

                // If no state, no previous backups
                if (backupState == BackupAgent::STATE_NEVERRAN) {
                    statusText += "No previous backups found.\n";
                } else {
                    // Get the last backup folder
                    state std::string outpath =
                        wait(backupAgent->getLastBackupFolder(tr, logUid));

                    // Get the last tag name
                    state std::string tagNameDisplay;
                    Optional<Key> tagName =
                        wait(tr->get(StringRef(backupAgent->getConfigKey(
                            logUid, BackupAgent::keyConfigBackupTag))));

                    // Define the display tag name
                    if (tagName.present()) {
                        tagNameDisplay = tagName.get().toString().c_str();
                    }

                    // Get the backup uid
                    state Optional<Value> uid =
                        wait(tr->get(StringRef(backupAgent->getConfigKey(
                            logUid, BackupAgent::keyFolderId))));

                    // Get the stop key for completed status
                    state Optional<Value> stopVersionKey =
                        wait(tr->get(StringRef(backupAgent->getStatesKey(
                            logUid, BackupAgent::keyStateStopVersion))));

                    // Get the backup ranges (though not using it yet)
                    state Standalone<VectorRef<KeyRangeRef>> backupRanges;
                    Optional<Key> backupKeysPacked =
                        wait(tr->get(StringRef(backupAgent->getConfigKey(
                            logUid, BackupAgent::keyConfigBackupRanges))));
                    ;

                    // Unpack the backup keys, if present
                    if (backupKeysPacked.present()) {
                        BinaryReader br(backupKeysPacked.get(),
                                        IncludeVersion());
                        serializer(br, v1(backupRanges));
                    }

                    switch (backupState) {
                    case BackupAgent::STATE_SUBMITTED:
                        statusText += "The backup on tag `" + tagNameDisplay +
                                      "' is in progress (just started) to " +
                                      outpath + ".\n";
                        break;
                    case BackupAgent::STATE_BACKUP:
                        statusText += "The backup on tag `" + tagNameDisplay +
                                      "' is in progress to " + outpath + ".\n";
                        break;
                    case BackupAgent::STATE_DIFFERENTIAL:
                        statusText += "The backup on tag `" + tagNameDisplay +
                                      "' is restorable but continuing to " +
                                      outpath + ".\n";
                        break;
                    case BackupAgent::STATE_COMPLETED: {
                        int stopVersion = -1;

                        if (stopVersionKey.present()) {
                            FDBTuple t(stopVersionKey.get().toString());
                            stopVersion = t.get_int(0);
                        }

                        statusText += "The previous backup on tag `" +
                                      tagNameDisplay + "' at " + outpath +
                                      " completed at version " +
                                      format("%lld", stopVersion) + ".\n";
                    } break;
                    default:
                        statusText += "The previous backup on tag `" +
                                      tagNameDisplay + "' at " + outpath + " " +
                                      backupStatus + ".\n";
                        break;
                    }
                }

                // Append the errors, if requested
                if (errorLimit > 0) {
                    std::string errorKey = backupAgent->getErrorKey(logUid);
                    Standalone<RangeResultRef> values = wait(tr->getRange(
                        KeyRangeRef(errorKey + '\x00', errorKey + '\xff'),
                        errorLimit, false, true));

                    // Display the errors, if any
                    if (values.size() > 0) {
                        // Inform the user that the list of errors is complete
                        // or partial
                        statusText += (values.size() < errorLimit)
                                          ? "WARNING: Some backup agents have "
                                            "reported issues:\n"
                                          : "WARNING: Some backup agents have "
                                            "reported issues (printing " +
                                                std::to_string(errorLimit) +
                                                "):\n";

                        for (auto& s : values) {
                            statusText += "   " + printable(s.value) + "\n";
                        }
                    }
                }
                break;
            } catch (Error& e) {
                Void _uvar = wait(tr->onError(e));
            }
        }

        // Check if a backup agent is running
        bool agentRunning = wait(backupAgent->taskBucket->checkActive(cx));
        if (!agentRunning) {
            statusText += "WARNING: No backup agents are responding.\n";
        }

        return statusText;
    }

    // Backup global status value
    ACTOR static Future<int> getGlobalStateValue(
        BackupAgent* backupAgent, Reference<ReadYourWritesTransaction> tr)
    {
        ASSERT(backupAgent);

        tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
        state Key statusKey = StringRef(
            backupAgent->states.getItem(BackupAgent::keyStateGlobalStatus)
                .key());
        Optional<Value> status = wait(tr->get(statusKey));

        // TraceEvent("BA_getGlobalStateValue").detail("status",
        // printable(status)).detail("statusKey", printable(statusKey));

        return (!status.present())
                   ? BackupAgent::STATE_NEVERRAN
                   : BackupAgent::getState(status.get().toString());
    }

    // Backup status value
    ACTOR static Future<int> getStateValue(
        BackupAgent* backupAgent, Reference<ReadYourWritesTransaction> tr,
        UID logUid)
    {
        ASSERT(backupAgent);

        tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
        state Key statusKey = StringRef(
            backupAgent->getStatesKey(logUid, BackupAgent::keyStateStatus));
        Optional<Value> status = wait(tr->get(statusKey));

        // TraceEvent("BA_getStateValue").detail("logUid",
        // logUid).detail("status", printable(status)).detail("statusKey",
        // printable(statusKey));

        return (!status.present())
                   ? BackupAgent::STATE_NEVERRAN
                   : BackupAgent::getState(status.get().toString());
    }

    // Backup log Uid from tag
    ACTOR static Future<UID> getLogUid(BackupAgent* backupAgent,
                                       Reference<ReadYourWritesTransaction> tr,
                                       Key tagName)
    {
        ASSERT(backupAgent);

        tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
        state Optional<Value> logUid = wait(tr->get(StringRef(
            backupAgent->tagNames.getItem(tagName.toString()).key())));

        // TraceEvent("BA_getLogUid").detail("logUid", (logUid.present()) ?
        // systemDecompressUid(logUid.get()) : UID())
        //	.detail("logUidValue", printable(logUid)).detail("tagName",
        // printable(tagName));

        return (logUid.present()) ? systemDecompressUid(logUid.get()) : UID();
    }

    //
    // File Information
    //
    static bool getFileInfo(
        std::multimap<int64_t, std::tuple<std::string, std::string, std::string,
                                          std::string>>& fileInfo,
        std::set<Version>& dataInfo,
        std::multimap<int64_t, std::tuple<Version, std::string>>& logInfo,
        std::string filename, std::string folder, std::string prefix,
        int32_t weight, std::string suffix)
    {
        if (filename.find(prefix) != 0)
            return false;

        if (filename.rfind(suffix) != (filename.size() - suffix.size()))
            return false;

        std::string info = filename.substr(0, filename.size() - suffix.size());
        std::tuple<std::string, std::string, std::string, std::string> tuple;
        size_t offset = info.find_first_of(',', 0);
        if (offset != std::string::npos) {
            std::get<0>(tuple) = info.substr(0, offset);
            offset += 1;
            size_t offset1 = info.find_first_of(',', offset);
            if (offset1 != std::string::npos) {
                std::string value = info.substr(offset, offset1 - offset);
                std::get<1>(tuple) = value;
                Version version = getVersionFromString(value);

                offset = offset1 + 1;
                std::get<2>(tuple) = info.substr(offset);
                std::get<3>(tuple) = joinPath(folder, filename);
                fileInfo.insert(
                    std::pair<int64_t, std::tuple<std::string, std::string,
                                                  std::string, std::string>>(
                        version * 2 + weight, tuple));
                if (prefix == "backup") {
                    // TraceEvent("BA_getFileInfo").detail("filename",
                    // filename).detail("version", version);
                    dataInfo.insert(version);
                } else if (prefix == "log")
                    logInfo.insert(
                        std::pair<int64_t, std::tuple<Version, std::string>>(
                            version * 2 + weight,
                            std::make_tuple(version, info.substr(offset))));
                return true;
            }
        }

        return false;
    }

    static Future<Void> clearRange(Reference<ReadYourWritesTransaction> tr,
                                   KeyRangeRef keyRangeRef)
    {
        tr->clear(keyRangeRef);
        return Void();
    }

    static StringRef read(StringRef& data, int bytes)
    {
        if (bytes > data.size())
            throw restore_error();
        StringRef r = data.substr(0, bytes);
        data = data.substr(bytes);
        return r;
    }

    ACTOR static Future<Void> writeBackupData(Database cx,
                                              RangeResultRef result,
                                              KeyRangeRef range,
                                              Future<Void> previous)
    {
        state Transaction tr(cx);
        Void _uvar = wait(previous);
        loop
        {
            try {
                tr.clear(range);
                for (int i = 0; i < result.size(); i++)
                    tr.set(result[i].key, result[i].value, false);
                Void _uvar = wait(tr.commit());
                return Void();
            } catch (Error& e) {
                Void _uvar = wait(tr.onError(e));
            }
        }
    }

    static int64_t getRestoreFlags()
    {
#if defined(_WIN32)
        return IAsyncFile::OPEN_READONLY;
#else
        return IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED |
               IAsyncFile::OPEN_NO_AIO;
#endif
    }

    ACTOR static Future<Void> restoreBackupData(BackupAgent* backupAgent,
                                                Database cx,
                                                std::string fileName,
                                                Future<Void> previous)
    {
        // TraceEvent("BA_restoreBackupData").detail("Restore_KV_data_from_file",
        // fileName);

        state Reference<IAsyncFile> file =
            wait(g_network->open(fileName, getRestoreFlags(), 0644));
        state int64_t fileSize = wait(file->size());
        state Standalone<StringRef> buffer = makeString(fileSize);
        int _ = wait(file->read(mutateString(buffer), fileSize, 0));
        // close file
        file = Reference<IAsyncFile>();

        StringRef footer = buffer.substr(fileSize - BackupAgent::dataFooterSize,
                                         BackupAgent::dataFooterSize);
        int32_t lenBeginKey = *((int32_t*)(footer.begin()));
        int32_t lenEndKey = *((int32_t*)(footer.begin() + 4));
        int64_t kvTotalLength =
            fileSize - BackupAgent::dataFooterSize - lenBeginKey - lenEndKey;
        state KeyRangeRef range =
            KeyRangeRef(buffer.substr(kvTotalLength, lenBeginKey),
                        buffer.substr(kvTotalLength + lenBeginKey, lenEndKey));
        state StringRef kvData = buffer.substr(0, kvTotalLength);

        // TraceEvent("BA_restoreBackupData").detail("Restore_KV_data_from_file",
        // fileName).detail("range", printable(range)).detail("kvTotalLength",
        // kvTotalLength);
        state Arena arena;
        state RangeResultRef result;
        state int totalSize = 0;
        state std::vector<Future<Void>> writeActors;
        state KeyRef beginKey = range.begin;
        while (kvData.size()) {
            StringRef header = read(kvData, 8);
            int32_t keyLength = *((int32_t*)(header.begin()));
            int32_t valueLength = *((int32_t*)(header.begin() + 4));
            KeyRef key = read(kvData, keyLength);
            ValueRef value = read(kvData, valueLength);
            result.push_back(arena, KeyValueRef(key, value));
            totalSize += keyLength + valueLength;
            if (totalSize > 100000) {
                KeyRef endKey = kvData.size()
                                    ? keyAfter(result.end()[-1].key, arena)
                                    : range.end;
                writeActors.push_back(writeBackupData(
                    cx, result, KeyRangeRef(beginKey, endKey), previous));
                beginKey = endKey;
                result = Standalone<RangeResultRef>();
                totalSize = 0;
            }

            Void _uvar = wait(yield());
        }

        if (result.size())
            writeActors.push_back(writeBackupData(
                cx, result, KeyRangeRef(beginKey, range.end), previous));

        Void _uvar = wait(waitForAll(writeActors));
        return Void();
    }

    // value is an iterable representing all of the transaction log data for
    // a given version.Returns an iterable(generator) yielding a tuple for
    // each mutation in the log.At present, all mutations are represented as
    // (type, param1, param2) where type is an integer and param1 and param2 are
    // byte strings
    static Standalone<VectorRef<MutationRef>> decodeBackupLogValue(
        StringRef value)
    {
        try {
            uint64_t offset(0);
            uint64_t protocolVersion = 0;
            memcpy(&protocolVersion, value.begin(), sizeof(Protocol));
            offset += sizeof(uint64_t);
            if (protocolVersion <= 0x0FDB00A200090001) {
                TraceEvent(SevError, "decodeBackupLogValue")
                    .detail("incompatible_protocol_version", protocolVersion)
                    .detail("valueSize", value.size())
                    .detail("value", printable(value));
                throw incompatible_protocol_version();
            }

            Standalone<VectorRef<MutationRef>> result;
            uint32_t totalBytes = 0;
            memcpy(&totalBytes, value.begin() + offset, sizeof(uint32_t));
            offset += sizeof(uint32_t);
            uint32_t consumed = 0;

            while (consumed < totalBytes) {
                uint32_t type = 0;
                memcpy(&type, value.begin() + offset, sizeof(uint32_t));
                offset += sizeof(uint32_t);
                uint32_t len1 = 0;
                memcpy(&len1, value.begin() + offset, sizeof(uint32_t));
                offset += sizeof(uint32_t);
                uint32_t len2 = 0;
                memcpy(&len2, value.begin() + offset, sizeof(uint32_t));
                offset += sizeof(uint32_t);

                MutationRef logValue;
                logValue.type = type;
                logValue.param1 = value.substr(offset, len1);
                offset += len1;
                logValue.param2 = value.substr(offset, len2);
                offset += len2;
                result.push_back_deep(result.arena(), logValue);

                consumed += BackupAgent::logHeaderSize + len1 + len2;
            }

            ASSERT(consumed == totalBytes);
            if (value.size() != offset) {
                TraceEvent(SevError, "BA_decodeBackupLogValue")
                    .detail("unexpected_extra_data_size", value.size())
                    .detail("offset", offset);
                throw restore_corrupted_data();
            }

            return result;
        } catch (Error& e) {
            TraceEvent(SevError, "BA_decodeBackupLogValue")
                .error(e)
                .GetLastError()
                .detail("valueSize", value.size())
                .detail("value", printable(value));
            throw;
        }
    }

    ACTOR static Future<Standalone<VectorRef<MutationRef>>> readLogFile(
        Reference<LogInfo> logInfo, Version logBeginVersion, Version endVersion)
    {
        ASSERT(logInfo);

        state Standalone<VectorRef<MutationRef>> logValues;
        loop
        {
            state Key header = makeString(BackupAgent::logHeaderSize);
            uint8_t* headerPtr = mutateString(header);
            int length = wait(logInfo->logFile->read(
                headerPtr, BackupAgent::logHeaderSize, logInfo->offset));
            if (length == 0)
                return logValues;
            else if (length < BackupAgent::logHeaderSize)
                throw restore_corrupted_data();

            logInfo->offset += length;
            state int64_t version = 0;
            memcpy(&version, header.begin(), sizeof(int64_t));
            state int32_t size = 0;
            memcpy(&size, header.begin() + sizeof(int64_t), sizeof(int32_t));
            TraceEvent("BA_readLogFile")
                .detail("version", version)
                .detail("logBeginVersion", logBeginVersion)
                .detail("endVersion", endVersion)
                .detail("dataSize", size)
                .detail("fileOffset", logInfo->offset)
                .detail("logFile", logInfo->logPath);

            ASSERT(version >= logBeginVersion);
            state Key data = makeString(size);
            uint8_t* dataPtr = mutateString(data);
            if (version < endVersion) {
                int length = wait(
                    logInfo->logFile->read(dataPtr, size, logInfo->offset));
                if (length < size)
                    throw restore_corrupted_data();

                logInfo->offset += length;
                Standalone<VectorRef<MutationRef>> vec =
                    decodeBackupLogValue(data);

                /*
                for (auto it : vec) {
                        TraceEvent("BA_readLogFileMutations").detail("version_less_than_endVersion",
                "").detail("version", version)
                                .detail("endVersion", endVersion).detail("type",
                it.type).detail("p1",
                printable(StringRef(it.param1))).detail("p2",
                printable(StringRef(it.param2)));
                }
                */

                if (vec.size() > 0)
                    logValues.append_deep(logValues.arena(), vec.begin(),
                                          vec.size());
            } else {
                TraceEvent("BA_readLogFile")
                    .detail("version_greater_than_endVersion", true)
                    .detail("version", version)
                    .detail("endVersion", endVersion);

                /*
                int length = wait(logInfo->logFile->read((void*)(data.c_str()),
                size, logInfo->offset));
                logInfo->offset += length;
                std::vector<LogValueRef> vec = decodeBackupLogValue(data);

                for (auto it : vec) {
                        TraceEvent("BA_readLogFile").detail("version_greater_than_endVersion",
                "").detail("version", version).detail("endVersion",
                endVersion).detail("type", it->type).detail("p1",
                printable(StringRef(it->val1))).detail("p2",
                printable(StringRef(it->val2)));
                }
                */

                logInfo->offset -= BackupAgent::logHeaderSize;

                return logValues;
            }
        }
    }

    ACTOR static Future<Void> writeLogData(
        Database cx, Standalone<VectorRef<MutationRef>> logValues)
    {
        state uint64_t dataSize(0);
        state ReadYourWritesTransaction tr(cx);
        state int idx(0);
        state int startIndex(0);
        state UID randomID = g_random->randomUniqueID();
        state std::vector<int> atomicLocations;
        state std::vector<Future<Optional<Key>>> atomicValues;

        // TraceEvent("BA_writeLogData_start", randomID).detail("logValues",
        // logValues.size());

        for (; idx < logValues.size(); ++idx) {
            loop
            {
                try {
                    // TraceEvent("BA_writeLogData_mutation",
                    // randomID).detail("type",
                    // logValues[idx]->type).detail("p1",
                    // printable(StringRef(logValues[idx]->val1))).detail("p2",
                    // printable(StringRef(logValues[idx]->val2))).detail("size",
                    // logValues.size());
                    if (!logValues[idx].param1.startsWith(systemKeys.begin)) {
                        // TraceEvent("BA_writeLogData_normalmutation",
                        // randomID).detail("type",
                        // logValues[idx]->type).detail("p1",
                        // printable(StringRef(logValues[idx]->val1))).detail("p2",
                        // printable(StringRef(logValues[idx]->val2))).detail("size",
                        // logValues.size());
                        dataSize +=
                            logValues[idx].param1.size() +
                            logValues[idx].param2.size() +
                            CLIENT_KNOBS->BACKUP_OPERATION_COST_OVERHEAD;
                        if (logValues[idx].type == 0) {
                            tr.set(logValues[idx].param1,
                                   logValues[idx].param2);
                        } else if (logValues[idx].type == 1) {
                            KeyRangeRef range(logValues[idx].param1,
                                              std::min(logValues[idx].param2,
                                                       systemKeys.begin));
                            // TraceEvent("BA_writeLogData",
                            // randomID).detail("ClearRangeBegin",
                            // printable(range.begin)).detail("ClearRangeEnd",
                            // printable(range.end));
                            tr.clear(range);
                        } else if (isAtomicOp((MutationRef::Type)logValues[idx]
                                                  .type)) {
                            // TraceEvent("RestoringAtomicOp",
                            // randomID).detail("type",
                            // logValues[idx].type).detail("p1",
                            // printable(logValues[idx].param1)).detail("p2",
                            // printable(logValues[idx].param2));
                            tr.atomicOp(logValues[idx].param1,
                                        logValues[idx].param2,
                                        logValues[idx].type);
                            atomicLocations.push_back(idx);
                            atomicValues.push_back(
                                tr.get(logValues[idx].param1));
                        } else {
                            TraceEvent(SevError, "RestoringUnknownMutationType",
                                       randomID)
                                .detail("type", logValues[idx].type)
                                .detail("p1", printable(logValues[idx].param1))
                                .detail("p2", printable(logValues[idx].param2));
                        }
                    }

                    if (dataSize >
                            (CLIENT_KNOBS->BACKUP_LOG_WRITE_BATCH_MAX_SIZE) ||
                        idx == logValues.size() - 1 ||
                        atomicValues.size() >
                            (CLIENT_KNOBS->BACKUP_LOG_ATOMIC_OPS_SIZE)) {
                        Void _uvar = wait(waitForAll(atomicValues));
                        for (int i = 0; i < atomicLocations.size(); i++) {
                            logValues[atomicLocations[i]].type =
                                MutationRef::SetValue;
                            logValues[atomicLocations[i]].param2 =
                                atomicValues[i].get().get();
                            logValues.arena().dependsOn(
                                atomicValues[i].get().get().arena());
                        }
                        atomicLocations = std::vector<int>();
                        atomicValues = std::vector<Future<Optional<Key>>>();

                        // TraceEvent("BA_writeLogData",
                        // randomID).detail("dataSize", dataSize).detail("idx",
                        // idx).detail("logValues", logValues.size());
                        Void _uvar = wait(tr.commit());

                        startIndex = idx + 1;
                        tr = ReadYourWritesTransaction(cx);
                        dataSize = 0;
                    }
                    break;
                } catch (Error& e) {
                    Void _uvar = wait(tr.onError(e));
                    atomicLocations = std::vector<int>();
                    atomicValues = std::vector<Future<Optional<Key>>>();
                    idx = startIndex;
                    dataSize = 0;
                }
            }
        }

        return Void();
    }

    ACTOR static Future<Reference<LogInfo>> restoreLogData(
        BackupAgent* backupAgent, Database cx, Reference<LogInfo> logInfo,
        Version endVersion, std::string filename, Future<Void> prev)
    {
        ASSERT(backupAgent);
        ASSERT(logInfo);

        TraceEvent("BA_restoreLogData")
            .detail("beginVersion", logInfo->beginVersion)
            .detail("endVersion", endVersion)
            .detail("logFile", logInfo->logPath)
            .detail("logEndVersion", logInfo->endVersion);

        state Reference<LogInfo> newLogInfo(new LogInfo());

        if (!logInfo) {
            TraceEvent(SevError, "BA_restoreLogData_Error")
                .detail("MissingLogDataBeforeVersion", endVersion);
            fprintf(stderr, "ERROR: Restore file `%s' is missing log data\n",
                    filename.c_str());
            throw restore_missing_data();
        }

        if (logInfo->endVersion < endVersion) {
            TraceEvent(SevError, "BA_restoreLogData_Error")
                .detail("MissingLogDataBeforeVersion", endVersion)
                .detail("LogBeginVersion", logInfo->beginVersion)
                .detail("LogEndVersion", logInfo->endVersion);
            fprintf(stderr,
                    "ERROR: Restore file `%s' are missing log data "
                    "before version %lld\n",
                    filename.c_str(), (long long)endVersion);
            throw restore_missing_data();
        }

        ASSERT(logInfo->beginVersion < endVersion);

        state Standalone<VectorRef<MutationRef>> logValues =
            wait(readLogFile(logInfo, logInfo->beginVersion, endVersion));

        Void _uvar = wait(prev);
        Void _uvar = wait(writeLogData(cx, logValues));

        if (logInfo->endVersion == endVersion) {
            // close log file
            logInfo->logFile = Reference<IAsyncFile>();
            return Reference<LogInfo>();
        }

        newLogInfo->logPath = logInfo->logPath;
        newLogInfo->logFile = logInfo->logFile;
        newLogInfo->beginVersion = endVersion;
        newLogInfo->endVersion = logInfo->endVersion;
        newLogInfo->offset = logInfo->offset;

        return newLogInfo;
    }

    ACTOR static Future<Version> restore(BackupAgent* backupAgent, Database cx,
                                         std::vector<std::string> folders,
                                         Version targetVersion,
                                         bool displayInfo)
    {

        state std::multimap<int64_t, std::tuple<std::string, std::string,
                                                std::string, std::string>>
            fileInfo;
        Version minRestoreVersion(LLONG_MAX);
        Version maxRestoreVersion = -1;

        // Get the folder information
        getFolderMetaInfo(folders, &minRestoreVersion, &maxRestoreVersion,
                          &fileInfo, NULL);

        if (targetVersion <= 0)
            targetVersion = maxRestoreVersion;

        if (targetVersion < minRestoreVersion) {
            TraceEvent(SevError, "BackupAgentRestore")
                .detail("targetVersion", targetVersion)
                .detail("less_than_minRestoreVersion", minRestoreVersion);
            fprintf(stderr,
                    "ERROR: Restore version %lld is smaller than "
                    "minimum version %lld\n",
                    (long long)targetVersion, (long long)minRestoreVersion);
            throw restore_invalid_version();
        }

        if (targetVersion > maxRestoreVersion) {
            TraceEvent(SevError, "BackupAgentRestore")
                .detail("targetVersion", targetVersion)
                .detail("greater_than_maxRestoreVersion", maxRestoreVersion);
            fprintf(stderr,
                    "ERROR: Restore version %lld is larger than "
                    "maximum version %lld\n",
                    (long long)targetVersion, (long long)maxRestoreVersion);
            throw restore_invalid_version();
        }

        TraceEvent("BA_restore_start")
            .detail("targetVersion", targetVersion)
            .detail("minRestoreVersion", minRestoreVersion)
            .detail("maxRestoreVersion", maxRestoreVersion);

        // Display the restore information, if requested
        if (displayInfo) {
            printf("Restoring backup to version: %lld\n",
                   (long long)targetVersion);
            printf("%s\n", BackupAgent::getFolderInfo(folders).c_str());
        }

        state Reference<LogInfo> lastLog;
        state std::string filename;
        // type(backup/log); beginVersion; endVersion; filename;
        state std::multimap<int64_t,
                            std::tuple<std::string, std::string, std::string,
                                       std::string>>::iterator it =
            fileInfo.begin();
        state Future<Void> backupRangeFuture = Void();
        for (; it != fileInfo.end(); ++it) {
            if (std::get<0>(it->second) == "backup") {
                Version version = getVersionFromString(std::get<1>(it->second));
                if ((lastLog) && (lastLog->beginVersion != (version + 1))) {
                    Reference<LogInfo> theLog = wait(restoreLogData(
                        backupAgent, cx, lastLog, (version + 1),
                        std::get<3>(it->second), backupRangeFuture));
                    lastLog = theLog;
                }
                filename = std::get<3>(it->second);
                TraceEvent("BA_restoring_backupfile")
                    .detail("filename", filename);
                Future<Void> previous = backupRangeFuture;
                backupRangeFuture =
                    restoreBackupData(backupAgent, cx, filename, previous);
                Void _uvar = wait(previous);
                TraceEvent("BA_restored_backupfile")
                    .detail("filename", filename);
            } else if (std::get<0>(it->second) == "log") {
                state Version beginVer =
                    getVersionFromString(std::get<1>(it->second));
                state Version endVer =
                    getVersionFromString(std::get<2>(it->second));
                if (beginVer > targetVersion)
                    break;
                if (lastLog) {
                    Reference<LogInfo> theLog = wait(restoreLogData(
                        backupAgent, cx, lastLog, beginVer,
                        std::get<3>(it->second), backupRangeFuture));
                    lastLog = theLog;
                }

                ASSERT(!lastLog);
                lastLog = Reference<LogInfo>(new LogInfo());
                lastLog->logPath = std::get<3>(it->second);
                Reference<IAsyncFile> file = wait(g_network->open(
                    lastLog->logPath,
                    IAsyncFile::OPEN_CACHED_READ_ONLY | IAsyncFile::OPEN_NO_AIO,
                    0644));
                lastLog->logFile = file;
                lastLog->beginVersion = beginVer;
                lastLog->endVersion = endVer;
            }
        }

        if (lastLog) {
            Reference<LogInfo> theLog =
                wait(restoreLogData(backupAgent, cx, lastLog, targetVersion + 1,
                                    "aaa", backupRangeFuture));
            lastLog = theLog;
        }

        Void _uvar = wait(backupRangeFuture);

        TraceEvent("BA_restore_complete")
            .detail("Restored_to_version", targetVersion);
        return targetVersion;
    }

    static const int MAX_RESTORABLE_FILE_METASECTION_BYTES = 1024 * 8;

    static std::string getFolderInfo(std::vector<std::string> folders)
    {
        std::string folderInfo, restorableFile, metaRestoreData;
        Version minRestoreVersion(LLONG_MAX);
        Version maxRestoreVersion = -1;
        size_t metaEnd;

        // Get the folder information
        getFolderMetaInfo(folders, &minRestoreVersion, &maxRestoreVersion, NULL,
                          &restorableFile);

        // Read the retore meta information into string
        folderInfo = readFileBytes(restorableFile,
                                   MAX_RESTORABLE_FILE_METASECTION_BYTES);

        // Search for the end of the meta section
        metaEnd = folderInfo.find("\n\n");

        // Truncate the meta to the end of the meta section
        if (metaEnd != std::string::npos) {
            folderInfo.erase(metaEnd);
        }

        // Display the min and max version
        folderInfo += format("\n%-15s %lld\n%-15s %lld\n",
                             "MinRestoreVer:", minRestoreVersion,
                             "MaxRestoreVer:", maxRestoreVersion);

        return folderInfo;
    }

    static Void getFolderMetaInfo(
        std::vector<std::string> folders, Version* pMinRestoreVersion,
        Version* pMaxRestoreVersion,
        std::multimap<int64_t, std::tuple<std::string, std::string, std::string,
                                          std::string>>* pFileInfoArg,
        std::string* pRestorableFile)
    {
        std::multimap<int64_t, std::tuple<std::string, std::string, std::string,
                                          std::string>>
            fileInfo;
        std::multimap<int64_t, std::tuple<std::string, std::string, std::string,
                                          std::string>>* pFileInfo = &fileInfo;
        std::set<Version> dataInfo;
        std::multimap<int64_t, std::tuple<Version, std::string>> logInfo;
        bool foundRestorable = false;
        std::string versionText, fileVerInfo;

        // Use the passed argument, if defined
        if (pFileInfoArg) {
            pFileInfoArg->clear();
            pFileInfo = pFileInfoArg;
        }

        for (auto& folder : folders) {

            // TraceEvent(SevInfo, "BA_getVersions_search").detail("folder",
            // folder);

            vector<std::string> existingFiles = listFiles(folder, "");

            std::map<Version, Version> end_begin;
            for (auto& filename : existingFiles) {
                if (filename.find("log") == 0 &&
                    filename.rfind(".trlog") == (filename.size() - 6)) {
                    fileVerInfo = filename.substr(4, filename.size() - 10);
                    size_t offset = fileVerInfo.find_first_of(',', 0);

                    versionText = fileVerInfo.substr(0, offset);
                    Version beginVer = getVersionFromString(versionText);

                    versionText = fileVerInfo.substr(offset + 1);
                    Version endVer = getVersionFromString(versionText);

                    if (end_begin.count(endVer))
                        beginVer = std::min(beginVer, end_begin[endVer]);
                    end_begin[endVer] = beginVer;
                }
            }

            Version lastBegin = std::numeric_limits<Version>::max();
            std::vector<Version> removals;
            for (auto ver = end_begin.rbegin(); !(ver == end_begin.rend());
                 ver++) {
                if (ver->first > lastBegin)
                    removals.push_back(ver->first);
                else
                    lastBegin = ver->second;
            }

            for (auto ver : removals)
                end_begin.erase(ver);

            std::string logVerInfo, verInfo;
            for (auto& filename : existingFiles) {
                if (filename == "restorable") {
                    foundRestorable = true;

                    // Store the restorable filename, if defined
                    if (pRestorableFile) {
                        *pRestorableFile = joinPath(folder, filename);
                    }
                }

                if (!getFileInfo(*pFileInfo, dataInfo, logInfo, filename,
                                 folder, "backup", 1, ".kvdata")) {
                    if (filename.find("log") == 0 &&
                        filename.rfind(".trlog") == (filename.size() - 6)) {
                        logVerInfo = filename.substr(4, filename.size() - 10);
                        size_t offset = logVerInfo.find_first_of(',', 0);
                        versionText = logVerInfo.substr(0, offset);
                        Version beginVer = getVersionFromString(versionText);

                        versionText = logVerInfo.substr(offset + 1);
                        Version endVer = getVersionFromString(versionText);

                        if (beginVer == end_begin[endVer])
                            getFileInfo(*pFileInfo, dataInfo, logInfo, filename,
                                        folder, "log", 0, ".trlog");
                    }
                }
            }
        }

        if (!foundRestorable) {
            TraceEvent(SevError, "BA_restore")
                .detail(
                    "foundRestorable",
                    "This backup does not contain all of the necessary files")
                .detail("folders",
                        std::accumulate(folders.begin(), folders.end(),
                                        std::string(", ")));
            fprintf(stderr, "ERROR: The backup does not contain all of the "
                            "necessary files.\n");
            throw restore_missing_data();
        }

        Version minRestoreVersion(LLONG_MAX);
        if (!(dataInfo.rbegin() == dataInfo.rend()))
            minRestoreVersion = *(dataInfo.rbegin());

        Version maxRestoreVersion = -1;
        if (!(logInfo.rbegin() == logInfo.rend())) {
            std::string value = std::get<1>(logInfo.rbegin()->second);
            int64_t version = getVersionFromString(value);
            maxRestoreVersion = version - 1;
        }

        Version firstDataVersion = -1;
        if (dataInfo.begin() != dataInfo.end())
            firstDataVersion = *(dataInfo.begin());

        Version firstLogVersion = LLONG_MAX;
        if (logInfo.begin() != logInfo.end()) {
            firstLogVersion = std::get<0>(logInfo.begin()->second);
        }

        TraceEvent("BA_getVersions")
            .detail("minRestoreVersion", minRestoreVersion)
            .detail("maxRestoreVersion", maxRestoreVersion)
            .detail("firstLogVersion", firstLogVersion)
            .detail("firstDataVersion", firstDataVersion);

        if (firstLogVersion > firstDataVersion) {
            TraceEvent(SevError, "BA_restore")
                .detail("firstLogVersion", firstLogVersion)
                .detail("firstDataVersion", firstDataVersion);
            fprintf(stderr,
                    "ERROR: The first log version %lld is greater than "
                    "the first data version %lld\n",
                    (long long)firstLogVersion, (long long)firstDataVersion);
            throw restore_invalid_version();
        }

        if (minRestoreVersion > maxRestoreVersion) {
            TraceEvent(SevError, "BA_restore")
                .detail("minRestoreVersion", minRestoreVersion)
                .detail("maxRestoreVersion", maxRestoreVersion)
                .detail("firstLogVersion", firstLogVersion)
                .detail("firstDataVersion", firstDataVersion);
            fprintf(stderr,
                    "ERROR: The last data version %lld is greater than "
                    "last log version %lld\n",
                    (long long)minRestoreVersion, (long long)maxRestoreVersion);
            throw restore_invalid_version();
        }

        // Set the passed arguments, if defined
        if (pMinRestoreVersion) {
            *pMinRestoreVersion = minRestoreVersion;
        }
        if (pMaxRestoreVersion) {
            *pMaxRestoreVersion = maxRestoreVersion;
        }

        return Void();
    }
};

const std::string BackupAgent::logUidDelim("/");
const KeyRef BackupAgent::defaultTag = LiteralStringRef("default");
const int BackupAgent::logHeaderSize = 12;
const int BackupAgent::dataFooterSize = 20;

BackupAgent::BackupAgent(bool exit_when_no_task)
  : subspace(Subspace(FDBTuple(), serverBackupPrefixRange.begin.toString()))
  , states(subspace.getItem("state"))
  , config(subspace.getItem("config"))
  , errors(subspace.getItem("errors"))
  , ranges(subspace.getItem("ranges"))
  , tagNames(Subspace(FDBTuple(), serverBackupPrefixRange.begin.toString() +
                                      std::string("/tagname")))
  , taskBucket(new TaskBucket(subspace.getItem("tasks"), true, true))
  , futureBucket(new FutureBucket(subspace.getItem("futures"), true))
  , exitWhenNoTask(exit_when_no_task)
{}

BackupAgent::~BackupAgent() {}

void
BackupAgent::BackupRange()
{}

Future<Void>
BackupAgent::run(Database cx)
{
    return BackupAgentImpl::run(this, cx);
}

Future<Void>
BackupAgent::submitBackup(Reference<ReadYourWritesTransaction> tr, Key outPath,
                          Key tagName,
                          Standalone<VectorRef<KeyRangeRef>> backupRanges,
                          bool stopWhenDone)
{
    return BackupAgentImpl::submitBackup(this, tr, outPath, tagName,
                                         backupRanges, stopWhenDone);
}

Future<Void>
BackupAgent::discontinueBackup(Reference<ReadYourWritesTransaction> tr,
                               Key tagName)
{
    return BackupAgentImpl::discontinueBackup(this, tr, tagName);
}

Future<Void>
BackupAgent::abortBackup(Reference<ReadYourWritesTransaction> tr, Key tagName)
{
    return BackupAgentImpl::abortBackup(this, tr, tagName);
}

std::string
BackupAgent::getCurrentTime()
{
    double t = now();
    time_t curTime = t;
    char buffer[128];
    struct tm* timeinfo;
    timeinfo = localtime(&curTime);
    strftime(buffer, 128, "%Y-%m-%d-%H-%M-%S", timeinfo);

    std::string time(buffer);
    return time + format(".%06d", (int)(1e6 * (t - curTime)));
}

Future<std::string>
BackupAgent::getStatus(Database cx, int errorLimit, Key tagName)
{
    return BackupAgentImpl::getStatus(this, cx, errorLimit, tagName);
}

Future<int>
BackupAgent::getStateValue(Reference<ReadYourWritesTransaction> tr, UID logUid)
{
    return BackupAgentImpl::getStateValue(this, tr, logUid);
}

Future<int>
BackupAgent::getGlobalStateValue(Reference<ReadYourWritesTransaction> tr)
{
    return BackupAgentImpl::getGlobalStateValue(this, tr);
}

Future<UID>
BackupAgent::getLogUid(Reference<ReadYourWritesTransaction> tr, Key tagName)
{
    return BackupAgentImpl::getLogUid(this, tr, tagName);
}

Future<int>
BackupAgent::waitBackup(Database cx, Key tagName, bool stopWhenDone)
{
    return BackupAgentImpl::waitBackup(this, cx, tagName, stopWhenDone);
}

std::string
BackupAgent::getTempFilename()
{
    return "temp." + g_random->randomUniqueID().toString() + ".part";
}

std::string
BackupAgent::getDataFilename(std::string version)
{
    return "backup," + version + "," + g_random->randomUniqueID().toString() +
           ".kvdata";
}

std::string
BackupAgent::getLogFilename(std::string beginVer, std::string endVer)
{
    return "log," + beginVer + "," + endVer + ".trlog";
}

Future<Version>
BackupAgent::restore(Database cx, std::vector<std::string> folders,
                     Version targetVersion, bool displayInfo)
{
    return BackupAgentImpl::restore(this, cx, folders, targetVersion,
                                    displayInfo);
}

std::string
BackupAgent::getFolderInfo(std::vector<std::string> folders)
{
    return BackupAgentImpl::getFolderInfo(folders);
}

Future<std::string>
BackupAgent::getLastBackupFolder(Reference<ReadYourWritesTransaction> tr,
                                 UID logUid)
{
    // TraceEvent("BA_getLastBackupFolder").detail("logUid", logUid)
    //	.detail("keyOutputDir", printable(StringRef(getConfigKey(logUid,
    // BackupAgent::keyOutputDir))))
    //	.detail("keyFolderId", printable(StringRef(getConfigKey(logUid,
    // BackupAgent::keyFolderId))));

    return getPath(tr, getConfigKey(logUid, BackupAgent::keyOutputDir),
                   getConfigKey(logUid, BackupAgent::keyFolderId));
}


