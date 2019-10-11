#ifndef FDBCLIENT_BACKUP_AGENT_H
#define FDBCLIENT_BACKUP_AGENT_H
#pragma once

#include "flow/flow.h"
#include "NativeAPI.actor.h"
#include "TaskBucket.h"

class BackupAgent : NonCopyable
{
public:
    // Type of program being executed
    enum enumActionResult
    {
        RESULT_SUCCESSFUL = 0,
        RESULT_ERRORED = 1,
        RESULT_DUPLICATE = 2,
        RESULT_UNNEEDED = 3
    };

    enum enumState
    {
        STATE_ERRORED = 0,
        STATE_SUBMITTED = 1,
        STATE_BACKUP = 2,
        STATE_DIFFERENTIAL = 3,
        STATE_COMPLETED = 4,
        STATE_NEVERRAN = 5,
        STATE_ABORTED = 6
    };

    BackupAgent(bool exit_when_no_task = false);
    ~BackupAgent();

    BackupAgent& operator=(const BackupAgent& r)
    {
        subspace = r.subspace;
        states = r.states;
        config = r.config;
        errors = r.errors;
        ranges = r.ranges;
        tagNames = r.tagNames;
        taskBucket = r.taskBucket;
        futureBucket = r.futureBucket;
        return *this;
    }

    void BackupRange();

    Future<Void> run(Database cx);
    Future<Version> restore(Database cx, std::vector<std::string> folders,
                            Version targetVersion = -1,
                            bool displayInfo = true);

    Future<Void> submitBackup(Reference<ReadYourWritesTransaction> tr,
                              Key outPath, Key tagName,
                              Standalone<VectorRef<KeyRangeRef>> backupRanges,
                              bool stopWhenDone = true);
    Future<Void> submitBackup(Database cx, Key outPath, Key tagName,
                              Standalone<VectorRef<KeyRangeRef>> backupRanges,
                              bool stopWhenDone = true)
    {
        return runRYWTransaction(
            cx, [=](Reference<ReadYourWritesTransaction> tr) {
                return submitBackup(tr, outPath, tagName, backupRanges,
                                    stopWhenDone);
            });
    }

    Future<Void> discontinueBackup(Reference<ReadYourWritesTransaction> tr,
                                   Key tagName);
    Future<Void> discontinueBackup(Database cx, Key tagName)
    {
        return runRYWTransaction(cx,
                                 [=](Reference<ReadYourWritesTransaction> tr) {
                                     return discontinueBackup(tr, tagName);
                                 });
    }

    Future<Void> abortBackup(Reference<ReadYourWritesTransaction> tr,
                             Key tagName);
    Future<Void> abortBackup(Database cx, Key tagName)
    {
        return runRYWTransaction(cx,
                                 [=](Reference<ReadYourWritesTransaction> tr) {
                                     return abortBackup(tr, tagName);
                                 });
    }

    Future<std::string> getStatus(Database cx, int errorLimit, Key tagName);

    Future<int> getStateValue(Reference<ReadYourWritesTransaction> tr,
                              UID logUid);
    Future<int> getStateValue(Database cx, UID logUid)
    {
        return runRYWTransaction(cx,
                                 [=](Reference<ReadYourWritesTransaction> tr) {
                                     return getStateValue(tr, logUid);
                                 });
    }

    Future<int> getGlobalStateValue(Reference<ReadYourWritesTransaction> tr);
    Future<int> getGlobalStateValue(Database cx)
    {
        return runRYWTransaction(cx,
                                 [=](Reference<ReadYourWritesTransaction> tr) {
                                     return getGlobalStateValue(tr);
                                 });
    }

    Future<UID> getLogUid(Reference<ReadYourWritesTransaction> tr, Key tagName);
    Future<UID> getLogUid(Database cx, Key tagName)
    {
        return runRYWTransaction(cx,
                                 [=](Reference<ReadYourWritesTransaction> tr) {
                                     return getLogUid(tr, tagName);
                                 });
    }

    // stopWhenDone will return when the backup is stopped, if enabled.
    // Otherwise, it
    // will return when the backup directory is restorable.
    Future<int> waitBackup(Database cx, Key tagName, bool stopWhenDone = true);

    Future<std::string> getLastBackupFolder(
        Reference<ReadYourWritesTransaction> tr, UID logUid);
    Future<std::string> getLastBackupFolder(Database cx, UID logUid)
    {
        return runRYWTransaction(cx,
                                 [=](Reference<ReadYourWritesTransaction> tr) {
                                     return getLastBackupFolder(tr, logUid);
                                 });
    }

    Future<int64_t> getTaskCount(Reference<ReadYourWritesTransaction> tr)
    {
        return taskBucket->getTaskCount(tr);
    }
    Future<int64_t> getTaskCount(Database cx)
    {
        return taskBucket->getTaskCount(cx);
    }

    static std::string getFolderInfo(std::vector<std::string> folders);

    static std::string getCurrentTime();
    static std::string getTempFilename();
    static std::string getDataFilename(std::string version);
    static std::string getLogFilename(std::string beginVer, std::string endVer);

    static const std::string keyFolderId;
    static const std::string keyBeginVersion;
    static const std::string keyEndVersion;
    static const std::string keyOutputDir;
    static const std::string keyConfigBackupTag;
    static const std::string keyConfigLogUid;
    static const std::string keyConfigBackupRanges;
    static const std::string keyConfigStopWhenDoneKey;
    static const std::string keyStateStatus;
    static const std::string keyStateGlobalStatus;
    static const std::string keyStateStop;
    static const std::string keyStateStopVersion;
    static const std::string keyErrors;
    static const std::string keyLastUid;
    static const std::string keyBeginKey;
    static const std::string keyEndKey;

    // The following function will return the textual name of the
    // start status
    static const char* getResultText(enumActionResult enResult)
    {
        switch (enResult) {
        case RESULT_SUCCESSFUL:
            return "action was successful";
            break;
        case RESULT_ERRORED:
            return "error received during action process";
            break;
        case RESULT_DUPLICATE:
            return "requested action has already been performed";
            break;
        case RESULT_UNNEEDED:
            return "requested action is not needed";
            break;
        }
        return "<undefined>";
    }

    // Convert the status text to an enumerated value
    static enumState getState(std::string stateText)
    {
        enumState enState = STATE_ERRORED;

        if (stateText.empty()) {
            enState = STATE_NEVERRAN;
        }

        else if (!stateText.compare("has been submitted")) {
            enState = STATE_SUBMITTED;
        }

        else if (!stateText.compare("has been started")) {
            enState = STATE_BACKUP;
        }

        else if (!stateText.compare("is differential")) {
            enState = STATE_DIFFERENTIAL;
        }

        else if (!stateText.compare("has been completed")) {
            enState = STATE_COMPLETED;
        }

        else if (!stateText.compare("has been aborted")) {
            enState = STATE_ABORTED;
        }

        return enState;
    }

    // Convert the status text to an enumerated value
    static const char* getStateText(enumState enState)
    {
        const char* stateText;

        switch (enState) {
        case STATE_ERRORED:
            stateText = "has errored";
            break;
        case STATE_NEVERRAN:
            stateText = "has never been started";
            break;
        case STATE_SUBMITTED:
            stateText = "has been submitted";
            break;
        case STATE_BACKUP:
            stateText = "has been started";
            break;
        case STATE_DIFFERENTIAL:
            stateText = "is differential";
            break;
        case STATE_COMPLETED:
            stateText = "has been completed";
            break;
        case STATE_ABORTED:
            stateText = "has been aborted";
            break;
        default:
            stateText = "<undefined>";
            break;
        }

        return stateText;
    }

    // Determine if the specified state is runnable
    static bool isRunnable(enumState enState)
    {
        bool isRunnable = false;

        switch (enState) {
        case STATE_SUBMITTED:
        case STATE_BACKUP:
        case STATE_DIFFERENTIAL:
            isRunnable = true;
            break;
        default:
            break;
        }

        return isRunnable;
    }

    // Determine if the current state is runnable
    static bool isRunnable(std::string stateText)
    {
        return isRunnable(getState(stateText));
    }

    // Determine if the current state is runnable
    static const KeyRef getDefaultTag() { return defaultTag; }

private:
    friend class BackupAgentImpl;

    Subspace subspace;
    Subspace states;
    Subspace config;
    Subspace errors;
    Subspace ranges;
    Subspace tagNames;

    Reference<TaskBucket> taskBucket;
    Reference<FutureBucket> futureBucket;

    // This method will return the key for the specified state option and uid
    std::string getStatesKey(std::string logUid, const std::string& keyType)
    {
        return states.getItem(logUid + logUidDelim + keyType).key();
    }

    // This method will return the key for the specified state option and uid
    std::string getStatesKey(UID logUid, const std::string& keyType)
    {
        return getStatesKey(logUid.toString(), keyType);
    }

    // This method will return the key for the specified config option and uid
    std::string getConfigKey(std::string logUid, const std::string& keyType)
    {
        return config.getItem(logUid + logUidDelim + keyType).key();
    }

    // This method will return the key for the specified config option and uid
    std::string getConfigKey(UID logUid, const std::string& keyType)
    {
        return getConfigKey(logUid.toString(), keyType);
    }

    // This method will return the error key for the specified uid
    std::string getErrorKey(std::string logUid)
    {
        return errors.getItem(logUid).key();
    }

    // This method will return the error key for the specified uid
    std::string getErrorKey(UID logUid)
    {
        return getErrorKey(logUid.toString());
    }

    bool exitWhenNoTask;

    static const KeyRef defaultTag;
    static const int logHeaderSize;
    static const int dataFooterSize;

    static const std::string logUidDelim;
};

#endif
