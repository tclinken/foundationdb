/*
 * LockWatcher.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct LockWatcherWorkload : TestWorkload {

	Database db1;
	Database db2;
	double lockAfter;
	double writeFor;
	UID lockUid;
	Future<Void> lockWatcher;
	static Key k1;
	static Key k2;

	LockWatcherWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		ASSERT(g_simulator.extraDB != nullptr);
		Reference<ClusterConnectionFile> extraFile(new ClusterConnectionFile(*g_simulator.extraDB));
		db2 = Database::createDatabase(extraFile, -1);
		lockAfter = getOption(options, LiteralStringRef("lockAfter"), 30.0);
		writeFor = getOption(options, LiteralStringRef("writeFor"), 60.0);
		lockUid = deterministicRandom()->randomUniqueID();
		lockWatcher = Never();
		k1 = LiteralStringRef("k1");
		k2 = LiteralStringRef("k2");
	}

	ACTOR static Future<Void> locker(LockWatcherWorkload* self) {
		wait(delay(self->lockAfter));
		wait(lockDatabase(self->db1, self->lockUid));
		wait(unlockDatabase(self->db2, self->lockUid));
		return Void();
	}

	ACTOR static Future<Void> lockWatcherActor(LockWatcherWorkload* self) {
		loop {
			try {
				state Transaction tr(self->db2);
				state Version v = wait(tr.getReadVersion());
				return Void();
			} catch (Error& e) {
				if (e.code() != error_code_database_locked) {
					TraceEvent(SevWarnAlways, "LockWatcherUnexpectedError").error(e);
				}
				wait(delay(1.0));
			}
		}
	}

	ACTOR static Future<Void> increment(Key k, Database db) {
		state Transaction tr(db);
		state int val = 0;
		loop {
			try {
				Optional<Value> value = wait(tr.get(k));
				if (value.present()) {
					BinaryReader r(value.get(), Unversioned());
					serializer(r, val);
				}
				++val;
				BinaryWriter w(Unversioned());
				serializer(w, val);
				tr.set(k, w.toValue());
				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				if (e.code() == error_code_database_locked) {
					return Void();
				}
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> writer(LockWatcherWorkload* self) {
		state Key k = self->k1;
		state bool writeToDb2 = false;
		loop {
			choose {
				when(wait(delay(0.1 * deterministicRandom()->random01()))) {
					wait(increment(k, self->db1));
					if (writeToDb2) wait(increment(k, self->db2));
				}
				when(wait(self->lockWatcher)) {
					k = self->k2;
					writeToDb2 = true;
					self->lockWatcher = Never();
				}
			}
		}
	}

	virtual Future<Void> setup(Database const& cx) {
		if (clientId > 0) return Void();
		db1 = cx;
		return lockDatabase(db2, lockUid);
	}

	virtual Future<Void> start(Database const& cx) {
		if (clientId > 0) return Void();
		lockWatcher = lockWatcherActor(this);
		return timeout(waitForAll(vector<Future<Void>>{ locker(this), writer(this) }), writeFor, Void());
	}

	ACTOR static Future<bool> checkDb(Database db, Key shouldBeSet, Key shouldBeUnset) {
		state Transaction tr(db);
		loop {
			try {
				Optional<Value> value1 = wait(tr.get(shouldBeSet));
				if (!value1.present()) return false;
				state int val1;
				BinaryReader r(value1.get(), Unversioned());
				serializer(r, val1);
				if (val1 == 0) return false;
				Optional<Value> value2 = wait(tr.get(shouldBeUnset));
				if (value2.present()) return false;
				return true;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<bool> _check(LockWatcherWorkload* self) {
		wait(unlockDatabase(self->db1, self->lockUid));
		state bool res1 = wait(checkDb(self->db1, self->k1, self->k2));
		state bool res2 = wait(checkDb(self->db2, self->k2, self->k1));
		return res1 && res2;
	}

	virtual Future<bool> check(Database const& cx) {
		if (clientId > 0) return true;
		return _check(this);
	}
	void getMetrics(vector<PerfMetric>& m) {}
	std::string description() { return "LockWatcher"; }
};

Key LockWatcherWorkload::k1 = LiteralStringRef("k1");
Key LockWatcherWorkload::k2 = LiteralStringRef("k2");

WorkloadFactory<LockWatcherWorkload> LockWatcherWorkloadFactory("LockWatcher");
