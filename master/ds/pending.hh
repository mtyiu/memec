#ifndef __MASTER_DS_PENDING_HH__
#define __MASTER_DS_PENDING_HH__

#include <cstring>
#include <pthread.h>
#include <netinet/in.h>
#include "stats.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/ds/pending.hh"
#include "../../common/lock/lock.hh"

#define GIGA ( 1000 * 1000 * 1000 )

enum PendingType {
	PT_COORDINATOR_DEGRADED_LOCK_DATA,
	PT_APPLICATION_GET,
	PT_APPLICATION_SET,
	PT_APPLICATION_UPDATE,
	PT_APPLICATION_DEL,
	PT_SLAVE_GET,
	PT_SLAVE_SET,
	PT_SLAVE_REMAPPING_SET,
	PT_SLAVE_UPDATE,
	PT_SLAVE_DEL
};

class DegradedLockData : public DegradedLock {
public:
	uint8_t opcode;
	uint32_t valueUpdateSize;
	uint32_t valueUpdateOffset;
	char *valueUpdate;

	DegradedLockData() : DegradedLock() {
		this->opcode = 0;
		this->valueUpdateSize = 0;
		this->valueUpdateOffset = 0;
		this->valueUpdate = 0;
	}

	void set( uint32_t listId, uint32_t chunkId, uint8_t keySize, char *key ) {
		DegradedLock::set( listId, chunkId, keySize, key );
	}

	void set( uint32_t listId, uint32_t chunkId, uint8_t keySize, char *key, uint32_t valueUpdateSize, uint32_t valueUpdateOffset, char *valueUpdate, bool dup ) {
		this->set( listId, chunkId, keySize, key );
		this->valueUpdateSize = valueUpdateSize;
		this->valueUpdateOffset = valueUpdateOffset;
		if ( dup ) {
			this->valueUpdate = ( char * ) malloc( valueUpdateSize );
			memcpy( this->valueUpdate, valueUpdate, valueUpdateSize );
		} else {
			this->valueUpdate = valueUpdate;
		}
	}
};

class Pending {
private:
	bool get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, Key> *&map );
	bool get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, KeyValueUpdate> *&map );
	bool get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, RemappingRecord> *&map );
	bool get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, DegradedLockData> *&map );

public:
	struct {
		std::unordered_multimap<PendingIdentifier, DegradedLockData> degradedLockData;
		LOCK_T degradedLockDataLock;
	} coordinator;
	struct {
		std::unordered_multimap<PendingIdentifier, Key> get;
		std::unordered_multimap<PendingIdentifier, Key> set;
		std::unordered_multimap<PendingIdentifier, KeyValueUpdate> update;
		std::unordered_multimap<PendingIdentifier, Key> del;
		LOCK_T getLock;
		LOCK_T setLock;
		LOCK_T updateLock;
		LOCK_T delLock;
	} applications;
	struct {
		std::unordered_multimap<PendingIdentifier, Key> get;
		std::unordered_multimap<PendingIdentifier, Key> set;
		std::unordered_multimap<PendingIdentifier, RemappingRecord> remappingSet;
		std::unordered_multimap<PendingIdentifier, KeyValueUpdate> update;
		std::unordered_multimap<PendingIdentifier, Key> del;
		LOCK_T getLock;
		LOCK_T setLock;
		LOCK_T remappingSetLock;
		LOCK_T updateLock;
		LOCK_T delLock;
	} slaves;
	struct {
		std::unordered_multimap<PendingIdentifier, RequestStartTime> get;
		std::unordered_multimap<PendingIdentifier, RequestStartTime> set;
		LOCK_T getLock;
		LOCK_T setLock;
	} stats;

	Pending();

	// Insert (Coordinator)
	bool insertDegradedLockData(
		PendingType type, uint32_t id, uint32_t parentId, void *ptr,
		DegradedLockData &degradedLockData,
		bool needsLock = true, bool needsUnlock = true
	);
	// Insert (Applications)
	bool insertKey(
		PendingType type, uint32_t id, void *ptr,
		Key &key, bool needsLock = true, bool needsUnlock = true
	);
	bool insertKeyValueUpdate(
		PendingType type, uint32_t id, void *ptr,
		KeyValueUpdate &keyValueUpdate,
		bool needsLock = true, bool needsUnlock = true
	);
	// Insert (Slaves)
	bool insertKey(
		PendingType type, uint32_t id, uint32_t parentId, void *ptr,
		Key &key, bool needsLock = true, bool needsUnlock = true
	);
	bool insertRemappingRecord(
		PendingType type, uint32_t id, uint32_t parentId, void *ptr,
		RemappingRecord &remappingRecord,
		bool needsLock = true, bool needsUnlock = true
	);
	bool insertKeyValueUpdate(
		PendingType type, uint32_t id, uint32_t parentId, void *ptr,
		KeyValueUpdate &keyValueUpdate,
		bool needsLock = true, bool needsUnlock = true
	);
	bool recordRequestStartTime(
		PendingType type, uint32_t id, uint32_t parentId, void *ptr,
		struct sockaddr_in addr
	);

	// Erase
	bool eraseDegradedLockData(
		PendingType type, uint32_t id, void *ptr = 0,
		PendingIdentifier *pidPtr = 0,
		DegradedLockData *degradedLockDataPtr = 0,
		bool needsLock = true, bool needsUnlock = true
	);
	bool eraseKey(
		PendingType type, uint32_t id, void *ptr = 0,
		PendingIdentifier *pidPtr = 0, Key *keyPtr = 0,
		bool needsLock = true, bool needsUnlock = true
	);
	bool eraseRemappingRecord(
		PendingType type, uint32_t id, void *ptr = 0,
		PendingIdentifier *pidPtr = 0,
		RemappingRecord *remappingRecordPtr = 0,
		bool needsLock = true, bool needsUnlock = true
	);
	bool eraseKeyValueUpdate(
		PendingType type, uint32_t id, void *ptr = 0,
		PendingIdentifier *pidPtr = 0,
		KeyValueUpdate *keyValueUpdatePtr = 0,
		bool needsLock = true, bool needsUnlock = true
	);
	bool eraseRequestStartTime(
		PendingType type, uint32_t id, void *ptr,
		struct timespec &elapsedTime,
		PendingIdentifier *pidPtr = 0,
		RequestStartTime *rstPtr = 0
	);

	// Find
	bool findKey(
		PendingType type, uint32_t id, void *ptr,
		Key *keyPtr
	);
	bool findKeyValueUpdate(
		PendingType type, uint32_t id, void *ptr,
		KeyValueUpdate *keyValuePtr
	);

	// Count
	uint32_t count( PendingType type, uint32_t id, bool needsLock = true, bool needsUnlock = true );
};

#endif
