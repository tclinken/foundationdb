#define BOOST_DATE_TIME_NO_LIB
#include <boost/interprocess/managed_shared_memory.hpp>

#include "flow/flow.h"
#include "flow/FastAlloc.h"
#include "flow/serialize.h"
#include "flow/IRandom.h"
#include "flow/genericactors.actor.h"

#include <flow/ProtocolVersion.h>
#include "fdbclient/FDBTypes.h"
#include "fdbclient/BackupAgent.h"
//#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/StatusClient.h"

#include "fdbclient/ThreadSafeTransaction.h"

#include <stdarg.h>
#include <stdio.h>

#include <signal.h>

#include <algorithm> // std::transform
#include <string>
#include <iostream>

using std::cout;
using std::endl;

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <Windows.h>
#undef min
#undef max
#endif
#include <time.h>

#ifdef __linux__
#include <execinfo.h>
#include <signal.h>
#ifdef ALLOC_INSTRUMENTATION
#include <cxxabi.h>
#endif
#endif

#ifndef WIN32
#include "versions.h"
#endif

#include "flow/SimpleOpt.h"

#include "flow/SystemMonitor.h"

extern uint8_t* g_extra_memory;

// Type of program being executed
enum enumProgramExe
{
    EXE_AGENT,
    EXE_BACKUP,
    EXE_RESTORE,
    EXE_UNDEFINED
};

// Type of program being executed
enum enumBackupType
{
    BACKUP_UNDEFINED = 0,
    BACKUP_START,
    BACKUP_STATUS,
    BACKUP_ABORT,
    BACKUP_WAIT,
    BACKUP_DISCONTINUE
};

//
enum
{
    // Backup constants
    OPT_DESTDIR,
    OPT_ERRORLIMIT,
    OPT_TAGNAME,
    OPT_BACKUPKEYS,
    OPT_WAITFORDONE,
    OPT_NOSTOPWHENDONE,
    OPT_DRYRUN,

    // Restore constants
    OPT_RESTOREFOLDERS,
    OPT_DBVERSION,

    // Shared constants
    OPT_CLUSTERFILE,
    OPT_QUIET,
    OPT_HELP,
    OPT_DEVHELP,
    OPT_VERSION,
    OPT_PARENTPID,
    OPT_NEWCONSOLE,
    OPT_NOBOX,
    OPT_CRASHONERROR,
    OPT_NOBUFSTDOUT,
    OPT_BUFSTDOUTERR,
    OPT_TRACE,
    OPT_TRACE_DIR,
};

CSimpleOpt::SOption g_rgAgentOptions[] = {
#ifdef _WIN32
    { OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
    { OPT_NEWCONSOLE, "-n", SO_NONE },
    { OPT_NOBOX, "-q", SO_NONE },
#endif
    { OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
    { OPT_CLUSTERFILE, "--cluster_file", SO_REQ_SEP },
    { OPT_VERSION, "--version", SO_NONE },
    { OPT_VERSION, "-v", SO_NONE },
    { OPT_QUIET, "-q", SO_NONE },
    { OPT_QUIET, "--quiet", SO_NONE },
    { OPT_TRACE, "--log", SO_NONE },
    { OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
    { OPT_CRASHONERROR, "--crash", SO_NONE },
    { OPT_HELP, "-?", SO_NONE },
    { OPT_HELP, "-h", SO_NONE },
    { OPT_HELP, "--help", SO_NONE },
    { OPT_DEVHELP, "--dev-help", SO_NONE },

    SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupStartOptions[] = {
#ifdef _WIN32
    { OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
    { OPT_NEWCONSOLE, "-n", SO_NONE },
    { OPT_NOBOX, "-q", SO_NONE },
#endif
    { OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
    { OPT_CLUSTERFILE, "--cluster_file", SO_REQ_SEP },
    { OPT_WAITFORDONE, "-w", SO_NONE },
    { OPT_WAITFORDONE, "--waitfordone", SO_NONE },
    { OPT_NOSTOPWHENDONE, "-z", SO_NONE },
    { OPT_NOSTOPWHENDONE, "--no-stop-when-done", SO_NONE },
    { OPT_DESTDIR, "-d", SO_REQ_SEP },
    { OPT_DESTDIR, "--destinatondir", SO_REQ_SEP },
    { OPT_TAGNAME, "-t", SO_REQ_SEP },
    { OPT_TAGNAME, "--tagname", SO_REQ_SEP },
    { OPT_BACKUPKEYS, "-k", SO_REQ_SEP },
    { OPT_BACKUPKEYS, "--keys", SO_REQ_SEP },
    { OPT_DRYRUN, "-n", SO_NONE },
    { OPT_DRYRUN, "--dryrun", SO_NONE },
    { OPT_TRACE, "--log", SO_NONE },
    { OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
    { OPT_QUIET, "-q", SO_NONE },
    { OPT_QUIET, "--quiet", SO_NONE },
    { OPT_VERSION, "--version", SO_NONE },
    { OPT_VERSION, "-v", SO_NONE },
    { OPT_CRASHONERROR, "--crash", SO_NONE },
    { OPT_HELP, "-?", SO_NONE },
    { OPT_HELP, "-h", SO_NONE },
    { OPT_HELP, "--help", SO_NONE },
    { OPT_DEVHELP, "--dev-help", SO_NONE },

    SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupStatusOptions[] = {
#ifdef _WIN32
    { OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
    { OPT_NEWCONSOLE, "-n", SO_NONE },
    { OPT_NOBOX, "-q", SO_NONE },
#endif
    { OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
    { OPT_CLUSTERFILE, "--cluster_file", SO_REQ_SEP },
    { OPT_ERRORLIMIT, "-e", SO_REQ_SEP },
    { OPT_ERRORLIMIT, "--errorlimit", SO_REQ_SEP },
    { OPT_TAGNAME, "-t", SO_REQ_SEP },
    { OPT_TAGNAME, "--tagname", SO_REQ_SEP },
    { OPT_TRACE, "--log", SO_NONE },
    { OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
    { OPT_VERSION, "--version", SO_NONE },
    { OPT_VERSION, "-v", SO_NONE },
    { OPT_QUIET, "-q", SO_NONE },
    { OPT_QUIET, "--quiet", SO_NONE },
    { OPT_TRACE, "--log", SO_NONE },
    { OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
    { OPT_CRASHONERROR, "--crash", SO_NONE },
    { OPT_HELP, "-?", SO_NONE },
    { OPT_HELP, "-h", SO_NONE },
    { OPT_HELP, "--help", SO_NONE },
    { OPT_DEVHELP, "--dev-help", SO_NONE },

    SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupAbortOptions[] = {
#ifdef _WIN32
    { OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
    { OPT_NEWCONSOLE, "-n", SO_NONE },
    { OPT_NOBOX, "-q", SO_NONE },
#endif
    { OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
    { OPT_CLUSTERFILE, "--cluster_file", SO_REQ_SEP },
    { OPT_TAGNAME, "-t", SO_REQ_SEP },
    { OPT_TAGNAME, "--tagname", SO_REQ_SEP },
    { OPT_TRACE, "--log", SO_NONE },
    { OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
    { OPT_QUIET, "-q", SO_NONE },
    { OPT_QUIET, "--quiet", SO_NONE },
    { OPT_VERSION, "--version", SO_NONE },
    { OPT_VERSION, "-v", SO_NONE },
    { OPT_TRACE, "--log", SO_NONE },
    { OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
    { OPT_CRASHONERROR, "--crash", SO_NONE },
    { OPT_HELP, "-?", SO_NONE },
    { OPT_HELP, "-h", SO_NONE },
    { OPT_HELP, "--help", SO_NONE },
    { OPT_DEVHELP, "--dev-help", SO_NONE },

    SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupDiscontinueOptions[] = {
#ifdef _WIN32
    { OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
    { OPT_NEWCONSOLE, "-n", SO_NONE },
    { OPT_NOBOX, "-q", SO_NONE },
#endif
    { OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
    { OPT_CLUSTERFILE, "--cluster_file", SO_REQ_SEP },
    { OPT_TAGNAME, "-t", SO_REQ_SEP },
    { OPT_TAGNAME, "--tagname", SO_REQ_SEP },
    { OPT_WAITFORDONE, "-w", SO_NONE },
    { OPT_WAITFORDONE, "--waitfordone", SO_NONE },
    { OPT_TRACE, "--log", SO_NONE },
    { OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
    { OPT_QUIET, "-q", SO_NONE },
    { OPT_QUIET, "--quiet", SO_NONE },
    { OPT_VERSION, "--version", SO_NONE },
    { OPT_VERSION, "-v", SO_NONE },
    { OPT_TRACE, "--log", SO_NONE },
    { OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
    { OPT_CRASHONERROR, "--crash", SO_NONE },
    { OPT_HELP, "-?", SO_NONE },
    { OPT_HELP, "-h", SO_NONE },
    { OPT_HELP, "--help", SO_NONE },
    { OPT_DEVHELP, "--dev-help", SO_NONE },

    SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupWaitOptions[] = {
#ifdef _WIN32
    { OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
    { OPT_NEWCONSOLE, "-n", SO_NONE },
    { OPT_NOBOX, "-q", SO_NONE },
#endif
    { OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
    { OPT_CLUSTERFILE, "--cluster_file", SO_REQ_SEP },
    { OPT_TAGNAME, "-t", SO_REQ_SEP },
    { OPT_TAGNAME, "--tagname", SO_REQ_SEP },
    { OPT_TRACE, "--log", SO_NONE },
    { OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
    { OPT_QUIET, "-q", SO_NONE },
    { OPT_QUIET, "--quiet", SO_NONE },
    { OPT_VERSION, "--version", SO_NONE },
    { OPT_VERSION, "-v", SO_NONE },
    { OPT_TRACE, "--log", SO_NONE },
    { OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
    { OPT_CRASHONERROR, "--crash", SO_NONE },
    { OPT_HELP, "-?", SO_NONE },
    { OPT_HELP, "-h", SO_NONE },
    { OPT_HELP, "--help", SO_NONE },
    { OPT_DEVHELP, "--dev-help", SO_NONE },

    SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgRestoreOptions[] = {
    { OPT_RESTOREFOLDERS, "-f", SO_REQ_SEP },
#ifdef _WIN32
    { OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
    { OPT_NEWCONSOLE, "-n", SO_NONE },
    { OPT_NOBOX, "-q", SO_NONE },
#endif
    { OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
    { OPT_CLUSTERFILE, "--cluster_file", SO_REQ_SEP },
    { OPT_DBVERSION, "--version", SO_REQ_SEP },
    { OPT_DBVERSION, "-v", SO_REQ_SEP },
    { OPT_TRACE, "--log", SO_NONE },
    { OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
    { OPT_QUIET, "-q", SO_NONE },
    { OPT_QUIET, "--quiet", SO_NONE },
    { OPT_DRYRUN, "-n", SO_NONE },
    { OPT_DRYRUN, "--dryrun", SO_NONE },
    { OPT_TRACE, "--log", SO_NONE },
    { OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
    { OPT_CRASHONERROR, "--crash", SO_NONE },
    { OPT_HELP, "-?", SO_NONE },
    { OPT_HELP, "-h", SO_NONE },
    { OPT_HELP, "--help", SO_NONE },
    { OPT_DEVHELP, "--dev-help", SO_NONE },

    SO_END_OF_OPTIONS
};

#ifdef _WIN32
const KeyRef exeAgent = LiteralStringRef("backup_agent.exe");
const KeyRef exeBackup = LiteralStringRef("fdbbackup.exe");
const KeyRef exeRestore = LiteralStringRef("fdbrestore.exe");
#else
const KeyRef exeAgent = LiteralStringRef("backup_agent");
const KeyRef exeBackup = LiteralStringRef("fdbbackup");
const KeyRef exeRestore = LiteralStringRef("fdbrestore");
#endif

extern void flushTraceFileVoid();

#ifdef __linux__
void
crashHandler(int sig)
{
    // Pretty much all of this handler is risking undefined behavior and hangs,
    //  but the idea is that we're about to crash anyway...
    //std::string backtrace = get_backtrace();

    bool error = (sig != SIGUSR2);

    fflush(stdout);
    TraceEvent(error ? SevError : SevInfo,
               error ? "Crash" : "ProcessTerminated")
        .detail("signal", sig)
        .detail("name", strsignal(sig))
        .detail("trace", backtrace);
    flushTraceFileVoid();

    fprintf(stderr, "SIGNAL: %s (%d)\n", strsignal(sig), sig);
    fprintf(stderr, "Trace: %s\n", backtrace.c_str());

    _exit(128 + sig);
}
#endif

#ifdef _WIN32
void
parentWatcher(void* parentHandle)
{
    HANDLE parent = (HANDLE)parentHandle;
    int signal = WaitForSingleObject(parent, INFINITE);
    CloseHandle(parentHandle);
    if (signal == WAIT_OBJECT_0)
        criticalError(FDB_EXIT_SUCCESS, "ParentProcessExited",
                      "Parent process exited");
    TraceEvent(SevError, "ParentProcessWaitFailed")
        .detail("RetCode", signal)
        .GetLastError();
}

#endif

ThreadSingleAssignmentVarBase* toCancel = NULL;
bool printCancel = true;

void
int_handler(int sig)
{
    printf("\n");
    if (toCancel) {
        if (printCancel)
            fprintf(stderr, "Cancelling operation\n");
        toCancel->addref();
        toCancel->cancel();
        toCancel = NULL;
    }
}

#ifdef __unixish__
#define SIGNAL_ENV struct sigaction
#elif defined(_WIN32)
#define SIGNAL_ENV int
#endif

SIGNAL_ENV
registerSignalHandler()
{
#ifdef __unixish__
    struct sigaction act, oact;

    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    act.sa_handler = int_handler;
    sigaction(SIGINT, &act, &oact);
    return oact;
#elif defined(_WIN32)
    signal(SIGINT, int_handler);
    return 0; // dummy
#endif
}

void
unregisterSignalHandler(SIGNAL_ENV oact)
{
#ifdef __unixish__
    sigaction(SIGINT, &oact, NULL);
#elif defined(_WIN32)
    signal(SIGINT, SIG_DFL);
#endif
}

template <class T>
T
makeInterruptable(ThreadFuture<T> f)
{
    if (f.isError())
        throw f.getError();
    if (f.isReady())
        return f.get();

    toCancel = f.getPtr();
    SIGNAL_ENV oldSignal = registerSignalHandler();

    T ret;
    try {
        ret = f.getBlocking();
    } catch (Error&) {
        unregisterSignalHandler(oldSignal);
        toCancel = NULL;
        throw;
    }

    unregisterSignalHandler(oldSignal);
    toCancel = NULL;
    return ret;
}

static void
printVersion()
{
    printf("FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
    printf("protocol %llx\n", (long long)minValidProtocolVersion);
}

static void
printHelpTeaser(const char* name)
{
    fprintf(stderr, "Try `%s --help' for more information.\n", name);
}

static void
printAgentUsage(bool devhelp)
{
    printf("FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
    printf("Usage: %s [OPTIONS]\n\n", exeAgent.toString().c_str());
    printf("  -C CONNFILE    The path of a file containing the connection "
           "string for the\n"
           "                 FoundationDB cluster. The default is first the "
           "value of the\n"
           "                 FDB_CLUSTER_FILE environment variable, then "
           "`./fdb.cluster',\n"
           "                 then `%s'.\n",
           getDefaultClusterFilePath().c_str());
    printf(
        "  --log          Enables trace file logging for the CLI session.\n"
        "  --log-dir PATH Specifes the output directory for trace files. If\n"
        "                 unspecified, defaults to the current directory. Has\n"
        "                 no effect unless --log is specified.\n");
    printf("  -v, --version  Print version information and exit.\n");
    printf("  -h, --help     Display this help and exit.\n");
    if (devhelp) {
#ifdef _WIN32
        printf("  -n             Create a new console.\n");
        printf("  -q             Disable error dialog on crash.\n");
        printf("  --parentpid PID\n");
        printf("                 Specify a process after whose termination to "
               "exit.\n");
#endif
    }

    return;
}

static void
printBackupUsage(bool devhelp)
{
    printf("FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
    printf(
        "Usage: %s (start | status | abort | wait | discontinue) [OPTIONS]\n\n",
        exeBackup.toString().c_str());
    printf("  -C CONNFILE    The path of a file containing the connection "
           "string for the\n"
           "                 FoundationDB cluster. The default is first the "
           "value of the\n"
           "                 FDB_CLUSTER_FILE environment variable, then "
           "`./fdb.cluster',\n"
           "                 then `%s'.\n",
           getDefaultClusterFilePath().c_str());
    printf("  -d DESTINATION Full/absolute path to the backup destination "
           "folder).\n");
    printf("  -e ERRORLIMIT  The maximum number of errors printed by status "
           "(default is 10).\n");
    printf("  -k KEYS        List of key ranges to backup.\n"
           "                 If not specified, the entire database will be "
           "backed up.\n");
    printf("  -v, --version  Print version information and exit.\n");
    printf("  -w, --wait     Wait for the backup to complete (allowed with "
           "`start' and `discontinue').\n");
    printf("  -z, --no-stop-when-done\n"
           "                 Do not stop backup when complete.\n");
    printf("  -h, --help     Display this help and exit.\n");
    printf("\n"
           "  KEYS FORMAT:   \"<BEGINKEY> <ENDKEY>\" [...]\n");

    if (devhelp) {
#ifdef _WIN32
        printf("  -n             Create a new console.\n");
        printf("  -q             Disable error dialog on crash.\n");
        printf("  --parentpid PID\n");
        printf("                 Specify a process after whose termination to "
               "exit.\n");
#endif
    }

    return;
}

static void
printRestoreUsage(bool devhelp)
{
    printf("FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
    printf("Usage: %s FOLDERS [OPTIONS]\n\n", exeRestore.toString().c_str());
    printf("  FOLDERS        The path to the folders containing the backup "
           "files\n");
    printf("  -C CONNFILE    The path of a file containing the connection "
           "string for the\n"
           "                 FoundationDB cluster. The default is first the "
           "value of the\n"
           "                 FDB_CLUSTER_FILE environment variable, then "
           "`./fdb.cluster',\n"
           "                 then `%s'.\n",
           getDefaultClusterFilePath().c_str());
    printf("  -v DBVERSION   The version at which the database will be "
           "restored.\n");
    printf("  -h, --help     Display this help and exit.\n");
    if (devhelp) {
#ifdef _WIN32
        printf("  -n             Create a new console.\n");
        printf("  -q             Disable error dialog on crash.\n");
        printf("  --parentpid PID\n");
        printf("                 Specify a process after whose termination to "
               "exit.\n");
#endif
    }

    return;
}

static void
printUsage(enumProgramExe programExe, bool devhelp)
{

    switch (programExe) {
    case EXE_AGENT:
        printAgentUsage(devhelp);
        break;
    case EXE_BACKUP:
        printBackupUsage(devhelp);
        break;
    case EXE_RESTORE:
        printRestoreUsage(devhelp);
        break;
    case EXE_UNDEFINED:
    default:
        break;
    }

    return;
}

extern bool g_crashOnError;

// Return the type of program executable based on the name of executable file
enumProgramExe
getProgramType(std::string programExe)
{
    enumProgramExe enProgramExe = EXE_UNDEFINED;

    // lowercase the string
    std::transform(programExe.begin(), programExe.end(), programExe.begin(),
                   ::tolower);

    // Check if backup agent
    if ((programExe.length() >= exeAgent.size()) &&
        (programExe.compare(programExe.length() - exeAgent.size(),
                            exeAgent.size(),
                            (const char*)exeAgent.begin()) == 0)) {
        enProgramExe = EXE_AGENT;
    }

    // Check if backup
    else if ((programExe.length() >= exeBackup.size()) &&
             (programExe.compare(programExe.length() - exeBackup.size(),
                                 exeBackup.size(),
                                 (const char*)exeBackup.begin()) == 0)) {
        enProgramExe = EXE_BACKUP;
    }

    // Check if restore
    else if ((programExe.length() >= exeRestore.size()) &&
             (programExe.compare(programExe.length() - exeRestore.size(),
                                 exeRestore.size(),
                                 (const char*)exeRestore.begin()) == 0)) {
        enProgramExe = EXE_RESTORE;
    }

    return enProgramExe;
}

// Return the type of program executable based on the name of executable file
enumBackupType
getBackupType(std::string backupType)
{
    enumBackupType enBackupType = BACKUP_UNDEFINED;

    // lowercase the string
    std::transform(backupType.begin(), backupType.end(), backupType.begin(),
                   ::tolower);

    // Check if backup agent
    if (backupType.compare("start") == 0) {
        enBackupType = BACKUP_START;
    }

    // Check if status
    else if (backupType.compare("status") == 0) {
        enBackupType = BACKUP_STATUS;
    }

    // Check if abort
    else if (backupType.compare("abort") == 0) {
        enBackupType = BACKUP_ABORT;
    }

    // Check if wait
    else if (backupType.compare("wait") == 0) {
        enBackupType = BACKUP_WAIT;
    }

    // Check if info
    else if (backupType.compare("discontinue") == 0) {
        enBackupType = BACKUP_DISCONTINUE;
    }

    return enBackupType;
}

ACTOR Future<Void>
runRestore(Database db, std::vector<std::string> folders, Version dbVersion,
           bool displayInfo)
{
    try {
        state BackupAgent backupAgent;

        Version restoreVersion =
            wait(backupAgent.restore(db, folders, dbVersion));

        // Display the restored version, if not in error
        printf("Restored to version %lld\n", restoreVersion);
    } catch (Error& e) {
        if (e.code() == error_code_actor_cancelled)
            throw;
        fprintf(stderr, "ERROR: %s\n", e.what());
        throw;
    }

    return Void();
}

static Future<Void>
myStopNetwork()
{
    if (!g_network)
        throw network_not_setup();

    g_network->stop();
    closeTraceFile();
    return Void();
}

THREAD_FUNC
flowThread(void* p)
{
    try {
        runNetwork();
    } catch (Error& e) {
        fprintf(stderr, "ERROR from API thread: %s\n", e.what());
        _exit(1);
    } catch (std::exception& e) {
        fprintf(stderr, "ERROR from API thread: %s\n", e.what());
        _exit(1);
    } catch (...) {
        fprintf(stderr, "ERROR from API thread: %s\n", unknown_error().what());
        _exit(1);
    }
    THREAD_RETURN;
}

static std::vector<std::vector<StringRef>>
parseLine(std::string& line, bool& err, bool& partial)
{
    err = false;
    partial = false;

    bool quoted = false;
    std::vector<StringRef> buf;
    std::vector<std::vector<StringRef>> ret;

    size_t i = line.find_first_not_of(' ');
    size_t offset = i;

    bool forcetoken = false;

    while (i <= line.length()) {
        switch (line[i]) {
        case ';':
            if (!quoted) {
                if (i > offset)
                    buf.push_back(StringRef((uint8_t*)(line.data() + offset),
                                            i - offset));
                ret.push_back(std::move(buf));
                offset = i = line.find_first_not_of(' ', i + 1);
            } else
                i++;
            break;
        case '"':
            quoted = !quoted;
            line.erase(i, 1);
            if (quoted)
                forcetoken = true;
            break;
        case ' ':
            if (!quoted) {
                buf.push_back(
                    StringRef((uint8_t*)(line.data() + offset), i - offset));
                offset = i = line.find_first_not_of(' ', i);
                forcetoken = false;
            } else
                i++;
            break;
        case '\\':
            if (i + 2 > line.length()) {
                err = true;
                ret.push_back(std::move(buf));
                return ret;
            }
            switch (line[i + 1]) {
                char ent, save;
            case '"':
            case '\\':
            case ' ':
            case ';':
                line.erase(i, 1);
                break;
            case 'x':
                if (i + 4 > line.length()) {
                    err = true;
                    ret.push_back(std::move(buf));
                    return ret;
                }
                char* pEnd;
                save = line[i + 4];
                line[i + 4] = 0;
                ent = char(strtoul(line.data() + i + 2, &pEnd, 16));
                if (*pEnd) {
                    err = true;
                    ret.push_back(std::move(buf));
                    return ret;
                }
                line[i + 4] = save;
                line.replace(i, 4, 1, ent);
                break;
            default:
                err = true;
                ret.push_back(std::move(buf));
                return ret;
            }
        default:
            i++;
        }
    }

    i -= 1;
    if (i > offset || forcetoken)
        buf.push_back(StringRef((uint8_t*)(line.data() + offset), i - offset));

    ret.push_back(std::move(buf));

    if (quoted)
        partial = true;

    return ret;
}

static void
addKeyRange(std::string optionValue,
            Standalone<VectorRef<KeyRangeRef>>& keyRanges)
{
    bool err = false, partial = false;
    int tokenArray = 0, tokenIndex = 0;

    auto parsed = parseLine(optionValue, err, partial);

    for (auto tokens : parsed) {
        tokenArray++;
        tokenIndex = 0;

        /*
        for (auto token : tokens)
        {
                tokenIndex++;

                printf("%4d token #%2d: %s\n", tokenArray, tokenIndex,
        printable(token).c_str());
        }
        */

        // Process the keys
        // <begin> [end]
        switch (tokens.size()) {
        // empty
        case 0:
            break;

        // single key range
        case 1:
            keyRanges.push_back_deep(
                keyRanges.arena(),
                KeyRangeRef(tokens.at(0), strinc(tokens.at(0))));
            break;

        // full key range
        case 2:
            try {
                keyRanges.push_back_deep(
                    keyRanges.arena(), KeyRangeRef(tokens.at(0), tokens.at(1)));
            } catch (Error& e) {
                fprintf(stderr,
                        "ERROR: Invalid key range `%s %s' reported error %s\n",
                        tokens.at(0).toString().c_str(),
                        tokens.at(1).toString().c_str(), e.what());
                throw invalid_option_value();
            }
            break;

        // Too many keys
        default:
            fprintf(stderr, "ERROR: Invalid key range identified with %ld keys",
                    tokens.size());
            throw invalid_option_value();
            break;
        }
    }

    return;
}

int
main(int argc, char* argv[])
{
    platformInit();

    THREAD_HANDLE network_thread = 0;
    ThreadFuture<Void> futureThreadCommand;
    int status = FDB_EXIT_SUCCESS;

    try {
#ifdef ALLOC_INSTRUMENTATION
        g_extra_memory = new uint8_t[1000000];
#endif
#ifdef __linux__
        // For these otherwise fatal errors, attempt to log a trace of
        // what was happening and then exit
        struct sigaction action;
        action.sa_handler = crashHandler;
        sigemptyset(&action.sa_mask);
        action.sa_flags = 0;

        sigaction(SIGILL, &action, NULL);
        sigaction(SIGFPE, &action, NULL);
        sigaction(SIGSEGV, &action, NULL);
        sigaction(SIGBUS, &action, NULL);
        sigaction(SIGUSR2, &action, NULL);

#if SLOW_TASK_PROFILE
        profileThread = true;

        action.sa_handler = profileHandler;
        sigaction(SIGPROF, &action, NULL);
#endif
#endif

        // Set default of line buffering standard out and error
        setvbuf(stdout, NULL, _IONBF, 0);
        setvbuf(stderr, NULL, _IONBF, 0);

        enumProgramExe programExe = getProgramType(argv[0]);
        enumBackupType backupType = BACKUP_UNDEFINED;

        CSimpleOpt* args = NULL;

        switch (programExe) {
        case EXE_AGENT:
            args = new CSimpleOpt(argc, argv, g_rgAgentOptions, SO_O_EXACT);
            break;
        case EXE_BACKUP:
            // Display backup help, if no arguments
            if (argc < 2) {
                printBackupUsage(false);
                return FDB_EXIT_ERROR;
            } else {
                // Get the backup type
                backupType = getBackupType(argv[1]);

                // Create the appropriate simple opt
                switch (backupType) {
                case BACKUP_START:
                    args = new CSimpleOpt(argc - 1, &argv[1],
                                          g_rgBackupStartOptions, SO_O_EXACT);
                    break;
                case BACKUP_STATUS:
                    args = new CSimpleOpt(argc - 1, &argv[1],
                                          g_rgBackupStatusOptions, SO_O_EXACT);
                    break;
                case BACKUP_ABORT:
                    args = new CSimpleOpt(argc - 1, &argv[1],
                                          g_rgBackupAbortOptions, SO_O_EXACT);
                    break;
                case BACKUP_WAIT:
                    args = new CSimpleOpt(argc - 1, &argv[1],
                                          g_rgBackupWaitOptions, SO_O_EXACT);
                    break;
                case BACKUP_DISCONTINUE:
                    args = new CSimpleOpt(argc - 1, &argv[1],
                                          g_rgBackupDiscontinueOptions,
                                          SO_O_EXACT);
                    break;
                case BACKUP_UNDEFINED:
                default:
                    // Display help, if requested
                    if ((strcmp(argv[1], "-h") == 0) ||
                        (strcmp(argv[1], "--help") == 0)) {
                        printBackupUsage(false);
                        return FDB_EXIT_ERROR;
                    } else {
                        fprintf(stderr, "ERROR: Unsupported backup action %s\n",
                                argv[1]);
                        printHelpTeaser(argv[0]);
                        return FDB_EXIT_ERROR;
                    }
                    break;
                }
            }
            break;
        case EXE_RESTORE:
            args = new CSimpleOpt(argc, argv, g_rgRestoreOptions, SO_O_EXACT);
            break;
        case EXE_UNDEFINED:
        default:
            fprintf(stderr, "FoundationDB " FDB_VT_PACKAGE_NAME
                            " (v" FDB_VT_VERSION ")\n");
            fprintf(stderr,
                    "ERROR: Unable to determine program type based on "
                    "executable `%s'\n",
                    argv[0]);
            return FDB_EXIT_ERROR;
            break;
        }

        std::string destinationDir;
        std::string clusterFile;
        std::string tagName = BackupAgent::getDefaultTag().toString();
        std::vector<std::string> restoreFolders;
        Standalone<VectorRef<KeyRangeRef>> backupKeys;
        int maxErrors = 20;
        Version dbVersion = 0;
        bool waitForDone = false;
        bool stopWhenDone = true;
        bool trace = false;
        bool quietDisplay = false;
        std::string traceDir = "";
        ESOError lastError;

        if (argc == 1) {
            printUsage(programExe, false);
            return FDB_EXIT_ERROR;
        }

#ifdef _WIN32
        // Windows needs a gentle nudge to format floats correctly
        _set_output_format(_TWO_DIGIT_EXPONENT);
#endif

        while (args->Next()) {
            lastError = args->LastError();

            switch (lastError) {
            case SO_SUCCESS:
                break;

            case SO_ARG_INVALID_DATA:
                fprintf(stderr, "ERROR: invalid argument to option `%s'\n",
                        args->OptionText());
                printHelpTeaser(argv[0]);
                return FDB_EXIT_ERROR;
                break;

            case SO_ARG_INVALID:
                fprintf(stderr, "ERROR: argument given for option `%s'\n",
                        args->OptionText());
                printHelpTeaser(argv[0]);
                return FDB_EXIT_ERROR;
                break;

            case SO_ARG_MISSING:
                fprintf(stderr, "ERROR: missing argument for option `%s'\n",
                        args->OptionText());
                printHelpTeaser(argv[0]);
                return FDB_EXIT_ERROR;

            case SO_OPT_INVALID:
                fprintf(stderr, "ERROR: unknown option `%s'\n",
                        args->OptionText());
                printHelpTeaser(argv[0]);
                return FDB_EXIT_ERROR;
                break;

            default:
                fprintf(stderr, "ERROR: argument given for option `%s'\n",
                        args->OptionText());
                printHelpTeaser(argv[0]);
                return FDB_EXIT_ERROR;
                break;
            }

            switch (args->OptionId()) {
            case OPT_HELP:
                printUsage(programExe, false);
                return FDB_EXIT_SUCCESS;
                break;
            case OPT_DEVHELP:
                printUsage(programExe, true);
                return FDB_EXIT_SUCCESS;
                break;
            case OPT_VERSION:
                printVersion();
                return FDB_EXIT_SUCCESS;
                break;
            case OPT_NOBUFSTDOUT:
                setvbuf(stdout, NULL, _IONBF, 0);
                setvbuf(stderr, NULL, _IONBF, 0);
                break;
            case OPT_BUFSTDOUTERR:
                setvbuf(stdout, NULL, _IOFBF, BUFSIZ);
                setvbuf(stderr, NULL, _IOFBF, BUFSIZ);
                break;
            case OPT_QUIET:
                quietDisplay = true;
                break;
            case OPT_TRACE:
                trace = true;
                break;
            case OPT_TRACE_DIR:
                trace = true;
                traceDir = args->OptionArg();
                break;
            case OPT_CLUSTERFILE:
                clusterFile = args->OptionArg();
                break;
            case OPT_BACKUPKEYS:
                try {
                    addKeyRange(args->OptionArg(), backupKeys);
                } catch (Error& e) {
                    printHelpTeaser(argv[0]);
                    return FDB_EXIT_ERROR;
                }
                break;
            case OPT_DESTDIR:
                destinationDir = args->OptionArg();
                break;
            case OPT_WAITFORDONE:
                waitForDone = true;
                break;
            case OPT_NOSTOPWHENDONE:
                stopWhenDone = false;
                break;
            case OPT_RESTOREFOLDERS:
                restoreFolders.push_back(args->OptionArg());
                break;
            case OPT_ERRORLIMIT: {
                const char* a = args->OptionArg();
                if (!sscanf(a, "%d", &maxErrors)) {
                    fprintf(
                        stderr,
                        "ERROR: Could not parse max number of errors `%s'\n",
                        a);
                    printHelpTeaser(argv[0]);
                    return FDB_EXIT_ERROR;
                }
                break;
            }
            case OPT_DBVERSION: {
                const char* a = args->OptionArg();
                long long dbVersionValue = 0;
                if (!sscanf(a, "%lld", &dbVersionValue)) {
                    fprintf(stderr,
                            "ERROR: Could not parse database version `%s'\n",
                            a);
                    printHelpTeaser(argv[0]);
                    return FDB_EXIT_ERROR;
                }
                dbVersion = dbVersionValue;
                break;
            }
#ifdef _WIN32
            case OPT_PARENTPID: {
                auto pid_str = args->OptionArg();
                int parent_pid = atoi(pid_str);
                auto pHandle = OpenProcess(SYNCHRONIZE, FALSE, parent_pid);
                if (!pHandle) {
                    TraceEvent("ParentProcessOpenError").GetLastError();
                    fprintf(
                        stderr,
                        "Could not open parent process at pid %d (error %d)",
                        parent_pid, GetLastError());
                    throw platform_error();
                }
                startThread(&parentWatcher, pHandle);
                break;
            }
            case OPT_NEWCONSOLE:
                FreeConsole();
                AllocConsole();
                freopen("CONIN$", "rb", stdin);
                freopen("CONOUT$", "wb", stdout);
                freopen("CONOUT$", "wb", stderr);
                break;
            case OPT_NOBOX:
                SetErrorMode(SetErrorMode(0) | SEM_NOGPFAULTERRORBOX);
                break;
#endif
            case OPT_TAGNAME:
                tagName = args->OptionArg();
                break;
            case OPT_CRASHONERROR:
                g_crashOnError = true;
                break;
            }
        }

        // Process the extra arguments
        for (int argLoop = 0; argLoop < args->FileCount(); argLoop++) {
            switch (programExe) {
            case EXE_AGENT:
                fprintf(stderr,
                        "ERROR: Backup Agent does not support argument "
                        "value `%s'\n",
                        args->File(argLoop));
                printHelpTeaser(argv[0]);
                return FDB_EXIT_ERROR;
                break;

            // Add the backup key range
            case EXE_BACKUP:
                // Error, if the keys option was not specified
                if (backupKeys.size() == 0) {
                    fprintf(stderr, "ERROR: Unknown backup option value `%s'\n",
                            args->File(argLoop));
                    printHelpTeaser(argv[0]);
                    return FDB_EXIT_ERROR;
                }
                // Otherwise, assume the item is a key range
                else {
                    try {
                        addKeyRange(args->File(argLoop), backupKeys);
                    } catch (Error& e) {
                        printHelpTeaser(argv[0]);
                        return FDB_EXIT_ERROR;
                    }
                }
                break;

            // Add the restore folder
            case EXE_RESTORE:
                restoreFolders.push_back(args->File(argLoop));
                break;

            case EXE_UNDEFINED:
            default:
                return FDB_EXIT_ERROR;
            }
        }

        // Delete the simple option object, if defined
        if (args) {
            delete args;
            args = NULL;
        }

        double start = timer();

        std::string commandLine;
        for (int a = 0; a < argc; a++) {
            if (a)
                commandLine += ' ';
            commandLine += argv[a];
        }

        if (trace) {
            if (traceDir.empty())
                setNetworkOption(FDBNetworkOptions::TRACE_ENABLE);
            else
                setNetworkOption(FDBNetworkOptions::TRACE_ENABLE,
                                 StringRef(traceDir));
        }

        TraceEvent("ProgramStart")
            .detail("Version", FDB_VT_VERSION)
            .detail("PackageName", FDB_VT_PACKAGE_NAME)
            .detailf("ActualTime", "{}", DEBUG_DETERMINISM ? 0 : time(NULL))
            .detail("CommandLine", commandLine);

        std::set_new_handler(&outOfMemory);

        TraceEvent("ElapsedTime").detail("RealTime", timer() - start);

        ThreadFuture<Reference<ThreadSafeDatabase>> dbf;
        Reference<ThreadSafeCluster> cluster;
        Reference<ClusterConnectionFile> ccf;
        bool connected = false;
        bool opened = false;
        bool initialStatusCheck = false;
        const char* exec = NULL;
        const KeyRef databaseKey = LiteralStringRef("DB");

        try {
            setupNetwork(Protocol{ minValidProtocolVersion });
            network_thread = startThread(flowThread, NULL);
        } catch (Error& e) {
            fprintf(stderr, "ERROR: %s\n", e.what());
            fprintf(stderr, "Unable to start network thread\n");
            return 1;
        }

        if (trace) {
            systemMonitor();
            uncancellable(recurring(&systemMonitor,
                                    CLIENT_KNOBS->SYSTEM_MONITOR_INTERVAL,
                                    TaskFlushTrace));
        }

        auto resolvedClusterFile =
            ClusterConnectionFile::lookupClusterFileName(clusterFile);
        try {
            ccf = Reference<ClusterConnectionFile>(
                new ClusterConnectionFile(resolvedClusterFile.first));
        } catch (Error& e) {
            fprintf(
                stderr, "%s\n",
                ClusterConnectionFile::getErrorString(resolvedClusterFile, e)
                    .c_str());
            return 1;
        }

        try {
            cluster = makeInterruptable(
                ThreadSafeCluster::create(ccf->getFilename().c_str()));
            connected = true;
        } catch (Error& e) {
            fprintf(stderr, "ERROR: %s\n", e.what());
            fprintf(stderr, "ERROR: Unable to connect to cluster from `%s'\n",
                    ccf->getFilename().c_str());
            return 1;
        }

        dbf = cluster->createDatabase(databaseKey);
        auto db = makeInterruptable(dbf);

        switch (programExe) {
        case EXE_RESTORE:
            futureThreadCommand =
                onMainThread([db, restoreFolders, dbVersion,
                              quietDisplay]() -> Future<Void> {
                    return runRestore(db->unsafeGetDatabase(), restoreFolders,
                                      dbVersion, !quietDisplay);
                });

            // Make the thread interruptable
            makeInterruptable(futureThreadCommand);
            futureThreadCommand.blockUntilReady();
            break;

        case EXE_UNDEFINED:
        default:
            return FDB_EXIT_ERROR;
        }

#ifdef ALLOC_INSTRUMENTATION
        {
            cout << "Page Counts: " << FastAllocator<16>::pageCount << " "
                 << FastAllocator<32>::pageCount << " "
                 << FastAllocator<64>::pageCount << " "
                 << FastAllocator<128>::pageCount << " "
                 << FastAllocator<256>::pageCount << " "
                 << FastAllocator<512>::pageCount << " "
                 << FastAllocator<1024>::pageCount << " "
                 << FastAllocator<2048>::pageCount << " "
                 << FastAllocator<4096>::pageCount << " "
                 << FastAllocator<8192>::pageCount << endl;

            vector<std::pair<std::string, const char*>> typeNames;
            for (auto i = allocInstr.begin(); i != allocInstr.end(); ++i) {
                std::string s;

#ifdef __linux__
                char* demangled =
                    abi::__cxa_demangle(i->first, NULL, NULL, NULL);
                if (demangled) {
                    s = demangled;
                    if (StringRef(s).startsWith(
                            LiteralStringRef("(anonymous namespace)::")))
                        s = s.substr(
                            LiteralStringRef("(anonymous namespace)::").size());
                    free(demangled);
                } else
                    s = i->first;
#else
                s = i->first;
                if (StringRef(s).startsWith(
                        LiteralStringRef("class `anonymous namespace'::")))
                    s = s.substr(
                        LiteralStringRef("class `anonymous namespace'::")
                            .size());
                else if (StringRef(s).startsWith(LiteralStringRef("class ")))
                    s = s.substr(LiteralStringRef("class ").size());
                else if (StringRef(s).startsWith(LiteralStringRef("struct ")))
                    s = s.substr(LiteralStringRef("struct ").size());
#endif

                typeNames.push_back(std::make_pair(s, i->first));
            }
            std::sort(typeNames.begin(), typeNames.end());
            for (int i = 0; i < typeNames.size(); i++) {
                const char* n = typeNames[i].second;
                auto& f = allocInstr[n];
                printf("%+d\t%+d\t%d\t%d\t%s\n", f.allocCount, -f.deallocCount,
                       f.allocCount - f.deallocCount, f.maxAllocated,
                       typeNames[i].first.c_str());
            }

            // We're about to exit and clean up data structures, this will wreak
            // havoc on allocation recording
            memSample_entered = true;
        }
#endif
    } catch (Error& e) {
        TraceEvent(SevError, "MainError").error(e);
        status = FDB_EXIT_MAIN_ERROR;
    } catch (std::exception& e) {
        TraceEvent(SevError, "MainError")
            .error(unknown_error())
            .detail("std::exception", e.what());
        status = FDB_EXIT_MAIN_EXCEPTION;
    }

    onMainThreadVoid([]() { stopNetwork(); }, NULL);
    waitThread(network_thread);

    return status;
}
