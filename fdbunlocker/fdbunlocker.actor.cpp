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
	printf("usage: %s -C CONNFILE -L LOCKUID\n", name);
}

ACTOR Future<Void> unlockDB(std::string clusterFileName, UID lockUID) {
	try {
		state std::pair<std::string, bool> resolvedClusterFile =
		    ClusterConnectionFile::lookupClusterFileName(clusterFileName);
		state Reference<ClusterConnectionFile> ccf(new ClusterConnectionFile(resolvedClusterFile.first));
		state Database db = Database::createDatabase(ccf, -1, false);
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
	std::string clusterFileName;
	UID lockUID;

	for (int i = 1; i < argc; ++i) {
		std::string arg(argv[i]);
		if (arg == "-C" || arg == "--connfile") {
			if (i + 1 >= argc) {
				printf("Expecting an argument after %s\n", argv[i]);
				return 1;
			}
			clusterFileName = std::string(argv[++i]);
		} else if (arg == "-L" || arg == "--lockuid") {
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
		} else {
			printf("Unexpected argument: %s\n", argv[i]);
		}
	}
	if (!lockUID.isValid() || clusterFileName.empty()) {
		printUsage(argv[0]);
		return 1;
	}

	try {
		platformInit();
		initSignalSafeUnwind();
		registerCrashHandler();
		setupNetwork();
		auto f = unlockDB(clusterFileName, lockUID);
		runNetwork();
	} catch (Error& e) {
		printf("ERROR: %s (%d)\n", e.what(), e.code());
	}
}
