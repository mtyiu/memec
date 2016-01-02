#ifndef __MASTER_DS_PENDING_HH__
#define __MASTER_DS_PENDING_HH__

#include <cstring>
#include <pthread.h>
#include <netinet/in.h>
#include "stats.hh"
#include "../../common/ds/key_value.hh"
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
	PT_SLAVE_UPDATE,
	PT_SLAVE_DEL,
	PT_KEY_REMAP_LIST
};

class RemapList {
public:
	uint32_t *original, *remapped;
	uint32_t remappedCount;

	RemapList() {
		this->original = 0;
		this->remapped = 0;
		this->remappedCount = 0;
	}

	RemapList( uint32_t *original, uint32_t *remapped, uint32_t remappedCount ) {
		this->original = original;
		this->remapped = remapped;
		this->remappedCount = remappedCount;
	}

	void free() {
		delete[] this->original;
		delete[] this->remapped;
		this->original = 0;
		this->remapped = 0;
	}
};

class DegradedLockData {
public:
	uint8_t opcode;
	uint8_t keySize;
	char *key;
	uint32_t valueUpdateSize;
	uint32_t valueUpdateOffset;
	char *valueUpdate;
	uint32_t *original, *reconstructed;
	uint32_t reconstructedCount;

	DegradedLockData() {
		this->opcode = 0;
		this->keySize = 0;
		this->key = 0;
		this->valueUpdateSize = 0;
		this->valueUpdateOffset = 0;
		this->valueUpdate = 0;
		this->original = 0;
		this->reconstructed = 0;
		this->reconstructedCount = 0;
	}

	void set( uint8_t opcode, uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount, uint8_t keySize = 0, char *key = 0 ) {
		this->opcode = opcode;
		this->keySize = keySize;
		this->key = key;
		this->original = original;
		this->reconstructed = reconstructed;
		this->reconstructedCount = reconstructedCount;
	}

	void set( uint32_t valueUpdateSize, uint32_t valueUpdateOffset, char *valueUpdate ) {
		this->valueUpdateSize = valueUpdateSize;
		this->valueUpdateOffset = valueUpdateOffset;
		this->valueUpdate = ( char * ) malloc( valueUpdateSize );
		memcpy( this->valueUpdate, valueUpdate, valueUpdateSize );
	}

	void free() {
		if ( this->original ) delete[] this->original;
		if ( this->reconstructed ) delete[] this->reconstructed;
		this->original = 0;
		this->reconstructed = 0;
	}
};

class Pending {
private:
	bool get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, Key> *&map );
	bool get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, KeyValue> *&map );
	bool get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, KeyValueUpdate> *&map );
	bool get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, RemapList> *&map );
	bool get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, DegradedLockData> *&map );
	bool get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, std::vector<uint32_t> > *&map );

public:
	struct {
		std::unordered_multimap<PendingIdentifier, DegradedLockData> degradedLockData;
		LOCK_T degradedLockDataLock;
	} coordinator;
	struct {
		std::unordered_multimap<PendingIdentifier, Key> get;
		std::unordered_multimap<PendingIdentifier, KeyValue> set;
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
		std::unordered_multimap<PendingIdentifier, KeyValueUpdate> update;
		std::unordered_multimap<PendingIdentifier, Key> del;
		LOCK_T getLock;
		LOCK_T setLock;
		LOCK_T updateLock;
		LOCK_T delLock;
	} slaves;
	struct {
		std::unordered_multimap<PendingIdentifier, RemapList> remapList;
		LOCK_T remapListLock;
	} requests;
	struct {
		std::unordered_multimap<PendingIdentifier, RequestStartTime> get;
		std::unordered_multimap<PendingIdentifier, RequestStartTime> set;
		LOCK_T getLock;
		LOCK_T setLock;
	} stats;

	Pending();

	// Insert (Coordinator)
	bool insertDegradedLockData(
		PendingType type, uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, void *ptr,
		DegradedLockData &degradedLockData,
		bool needsLock = true, bool needsUnlock = true
	);
	// Insert (Applications)
	bool insertKey(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr,
		Key &key, bool needsLock = true, bool needsUnlock = true,
		uint32_t timestamp = 0
	);
	bool insertKeyValue(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr,
		KeyValue &keyValue, bool needsLock = true, bool needsUnlock = true,
		uint32_t timestamp = 0
	);
	bool insertKeyValueUpdate(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr,
		KeyValueUpdate &keyValueUpdate, 
		bool needsLock = true, bool needsUnlock = true,
		uint32_t timestamp = 0
	);
	// Insert (Slaves)
	bool insertKey(
		PendingType type, uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, void *ptr,
		Key &key, bool needsLock = true, bool needsUnlock = true
	);
	bool insertKeyValueUpdate(
		PendingType type, uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, void *ptr,
		KeyValueUpdate &keyValueUpdate,
		bool needsLock = true, bool needsUnlock = true
	);
	bool recordRequestStartTime(
		PendingType type, uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, void *ptr,
		struct sockaddr_in addr
	);
	// Insert (Request)
	bool insertRemapList(
		PendingType type, uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, void *ptr,
		RemapList &remapList,
		bool needsLock = true, bool needsUnlock = true
	);

	// Erase
	bool eraseDegradedLockData(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr = 0,
		PendingIdentifier *pidPtr = 0,
		DegradedLockData *degradedLockDataPtr = 0,
		bool needsLock = true, bool needsUnlock = true
	);
	bool eraseKey(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr = 0,
		PendingIdentifier *pidPtr = 0, Key *keyPtr = 0,
		bool needsLock = true, bool needsUnlock = true,
		bool checkKey = false, char *checkKeyPtr = 0
	);
	bool eraseKeyValue(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr = 0,
		PendingIdentifier *pidPtr = 0, KeyValue *keyValuePtr = 0,
		bool needsLock = true, bool needsUnlock = true,
		bool checkKey = false, char *checkKeyPtr = 0
	);
	bool eraseKeyValueUpdate(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr = 0,
		PendingIdentifier *pidPtr = 0,
		KeyValueUpdate *keyValueUpdatePtr = 0,
		bool needsLock = true, bool needsUnlock = true,
		bool checkKey = false, char *checkKeyPtr = 0
	);
	bool eraseRequestStartTime(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr,
		struct timespec &elapsedTime,
		PendingIdentifier *pidPtr = 0,
		RequestStartTime *rstPtr = 0
	);
	bool eraseRemapList(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr = 0,
		PendingIdentifier *pidPtr = 0,
		RemapList *remapListPtr = 0,
		bool needsLock = true, bool needsUnlock = true
	);

	// Find
	bool findKeyValue(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr,
		KeyValue *keyValuePtr,
		bool checkKey = false, char *checkKeyPtr = 0
	);
	bool findKeyValueUpdate(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr,
		KeyValueUpdate *keyValuePtr,
		bool checkKey = false, char *checkKeyPtr = 0
	);
	bool findRemapList(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr,
		RemapList *remapListPtr
	);

	// Count
	uint32_t count( PendingType type, uint16_t instanceId, uint32_t requestId, bool needsLock = true, bool needsUnlock = true );
};

#endif
