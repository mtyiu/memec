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
	PT_SLAVE_REMAPPING_SET,
	PT_SLAVE_UPDATE,
	PT_SLAVE_DEL,
	PT_KEY_REMAP_LIST
};

class DegradedLockData : public DegradedLock {
public:
	uint8_t opcode;
	uint8_t keySize;
	char *key;
	uint32_t valueUpdateSize;
	uint32_t valueUpdateOffset;
	char *valueUpdate;

	DegradedLockData() : DegradedLock() {
		this->opcode = 0;
		this->keySize = 0;
		this->key = 0;
		this->valueUpdateSize = 0;
		this->valueUpdateOffset = 0;
		this->valueUpdate = 0;
	}

	void set( uint8_t opcode, uint32_t srcListId, uint32_t srcStripeId, uint32_t srcChunkId, uint32_t dstListId, uint32_t dstChunkId, uint8_t keySize = 0, char *key = 0 ) {
		this->opcode = opcode;
		DegradedLock::set( srcListId, srcStripeId, srcChunkId, dstListId, dstChunkId );
		this->keySize = keySize;
		this->key = key;
	}

	void set( uint32_t valueUpdateSize, uint32_t valueUpdateOffset, char *valueUpdate ) {
		this->valueUpdateSize = valueUpdateSize;
		this->valueUpdateOffset = valueUpdateOffset;
		this->valueUpdate = ( char * ) malloc( valueUpdateSize );
		memcpy( this->valueUpdate, valueUpdate, valueUpdateSize );
	}
};

class Pending {
private:
	bool get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, Key> *&map );
	bool get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, KeyValue> *&map );
	bool get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, KeyValueUpdate> *&map );
	bool get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, RemappingRecord> *&map );
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
		std::unordered_multimap<PendingIdentifier, std::vector<uint32_t> > remapList;
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
	bool insertRemappingRecord(
		PendingType type, uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, void *ptr,
		RemappingRecord &remappingRecord,
		bool needsLock = true, bool needsUnlock = true
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
		std::vector<uint32_t> &remapList,
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
	bool eraseRemappingRecord(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr = 0,
		PendingIdentifier *pidPtr = 0,
		RemappingRecord *remappingRecordPtr = 0,
		bool needsLock = true, bool needsUnlock = true
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
		std::vector<uint32_t> *remapList = 0,
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
		std::vector<uint32_t> *remapList
	);

	// Count
	uint32_t count( PendingType type, uint16_t instanceId, uint32_t requestId, bool needsLock = true, bool needsUnlock = true );
};

#endif
