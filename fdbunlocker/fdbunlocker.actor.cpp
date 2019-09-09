/*
 * fdbunlocker.actor.cpp
 */

#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ManagementAPI.actor.h"

#include "flow/flow.h"
#include "flow/genericactors.actor.h"
#include "flow/IRandom.h"
#include "flow/Platform.h"
#include "flow/network.h"
#include "flow/SignalSafeUnwind.h"

#include "flow/actorcompiler.h" // This must be the last #include.

void printUsage(const char* name) {
	printf("usage: %s --connfile <CONNFILE> (--lockuid <LOCKUID> | --force)\n", name);
}

Database getDB(std::string clusterFileName) {
	std::pair<std::string, bool> resolvedClusterFile = ClusterConnectionFile::lookupClusterFileName(clusterFileName);
	Reference<ClusterConnectionFile> ccf(new ClusterConnectionFile(resolvedClusterFile.first));
	return Database::createDatabase(ccf, -1, false);
}

ACTOR Future<Void> forceUnlockDB(std::string clusterFileName) {
	try {
		// does not retry if transaction fails
		Database db = getDB(clusterFileName);
		state Transaction tr(db);
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		Optional<Value> val = wait(tr.get(databaseLockedKey));
		if (val.present()) {
			auto lockUID = BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned());
			wait(timeoutError(unlockDatabase(&tr, lockUID), 5.0));
			wait(tr.commit());
		}
		printf("Database unlocked.\n");
		g_network->stop();
		return Void();
	} catch (Error& e) {
		printf("Error occurred while attempting to unlock database: %s (%d).\n", e.what(), e.code());
		g_network->stop();
		throw e;
	}
}

ACTOR Future<Void> unlockDB(std::string clusterFileName, UID lockUID) {
	try {
		Database db = getDB(clusterFileName);
		wait(timeoutError(unlockDatabase(db, lockUID), 5.0));
		printf("Databse unlocked.\n");
		g_network->stop();
		return Void();
	} catch (Error& e) {
		printf("Error occurred while attempting to unlock database: %s (%d).\n", e.what(), e.code());
		g_network->stop();
		throw e;
	}
}

int main(int argc, char** argv) {
	Optional<std::string> clusterFileName;
	Optional<UID> lockUID;
	bool force = false;

	for (int i = 1; i < argc; ++i) {
		std::string arg(argv[i]);
		if (arg == "-C" || arg == "--connfile") {
			if (clusterFileName.present()) {
				printUsage(argv[0]);
				return 1;
			}
			if (i + 1 >= argc) {
				printf("Expecting an argument after %s\n", argv[i]);
				return 1;
			}
			clusterFileName = std::string(argv[++i]);
		} else if (arg == "-L" || arg == "--lockuid") {
			if (lockUID.present() || force) {
				printUsage(argv[0]);
				return 1;
			}
			if (i + 1 >= argc) {
				printf("Expecting an argument after %s\n", argv[i]);
				return 1;
			}
			std::string lockUIDString(argv[++i]);
			if (lockUIDString.size() != 32 || !all_of(lockUIDString.begin(), lockUIDString.end(), &isxdigit)) {
				printf("LockUID is not a valid UID.\n");
				return 1;
			}
			lockUID = UID::fromString(lockUIDString);
		} else if (arg == "-f" || arg == "--force") {
			if (lockUID.present() || force) {
				printUsage(argv[0]);
				return 1;
			}
			force = true;
		} else {
			printf("Unexpected argument: %s\n", argv[i]);
		}
	}
	if (!(force ^ lockUID.present()) || !clusterFileName.present()) {
		printUsage(argv[0]);
		return 1;
	}

	try {
		platformInit();
		initSignalSafeUnwind();
		registerCrashHandler();
		setupNetwork();
		auto f = force ? forceUnlockDB(clusterFileName.get()) : unlockDB(clusterFileName.get(), lockUID.get());
		runNetwork();
	} catch (Error& e) {
		printf("ERROR: %s (%d)\n", e.what(), e.code());
	}
}
