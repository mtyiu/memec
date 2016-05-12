#ifndef __CLIENT_DS_PENDING_HH__
#define __CLIENT_DS_PENDING_HH__

#include <cstring>
#include <pthread.h>
#include <netinet/in.h>
#include "stats.hh"
#include "../../common/ds/key_value.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/ds/pending.hh"
#include "../../common/lock/lock.hh"
#include "../../common/timestamp/timestamp.hh"

#define GIGA ( 1000 * 1000 * 1000 )

enum PendingType {
	PT_COORDINATOR_DEGRADED_LOCK_DATA,
	PT_APPLICATION_GET,
	PT_APPLICATION_SET,
	PT_APPLICATION_UPDATE,
	PT_APPLICATION_DEL,
	PT_SERVER_GET,
	PT_SERVER_SET,
	PT_SERVER_DEGRADED_SET,
	PT_SERVER_UPDATE,
	PT_SERVER_DEL,
	PT_KEY_REMAP_LIST,
	PT_ACK_REMOVE_PARITY,
	PT_ACK_REVERT_DELTA,
	PT_REQUEST_REPLAY
};

class RequestInfo {
public:
	void *application;
	uint16_t instanceId;
	uint32_t requestId;
	uint8_t opcode;
	union {
		KeyValueUpdate keyValueUpdate;
		KeyValue keyValue;
		Key key;
	};

	RequestInfo() {
	}

	RequestInfo( void *application, uint16_t instanceId, uint32_t requestId, uint8_t opcode, Key key ) {
		this->application = application;
		this->instanceId = instanceId;
		this->requestId = requestId;
		this->opcode = opcode;
		this->key.set( key.size, key.data, key.ptr );
	}

	RequestInfo( void *application, uint16_t instanceId, uint32_t requestId, uint8_t opcode, KeyValue keyValue ) {
		this->application = application;
		this->instanceId = instanceId;
		this->requestId = requestId;
		this->opcode = opcode;
		this->keyValue.set( keyValue.data, keyValue.ptr );
	}

	RequestInfo( void *application, uint16_t instanceId, uint32_t requestId, uint8_t opcode, KeyValueUpdate keyValueUpdate ) {
		this->application = application;
		this->instanceId = instanceId;
		this->requestId = requestId;
		this->opcode = opcode;
		this->keyValueUpdate.set( keyValueUpdate.size, keyValueUpdate.data, keyValueUpdate.ptr );
		this->keyValueUpdate.offset = keyValueUpdate.offset;
		this->keyValueUpdate.length = keyValueUpdate.length;
	}

	void set( void *application, uint16_t instanceId, uint32_t requestId, uint8_t opcode, Key key, bool needsDup ) {
		this->application = application;
		this->instanceId = instanceId;
		this->requestId = requestId;
		this->opcode = opcode;
		if ( needsDup )
			this->key.dup( key.size, key.data, key.ptr );
		else
			this->key.set( key.size, key.data, key.ptr );
	}

	void set( void *application, uint16_t instanceId, uint32_t requestId, uint8_t opcode, KeyValue keyValue, bool needsDup ) {
		this->application = application;
		this->instanceId = instanceId;
		this->requestId = requestId;
		this->opcode = opcode;
		if ( needsDup ) {
			Key key;
			char* valueStr;
			uint32_t valueSize;
			keyValue._deserialize( key.data, key.size, valueStr, valueSize );
			this->keyValue._dup( key.data, key.size, valueStr, valueSize, keyValue.ptr );
		} else {
			this->keyValue.set( keyValue.data, keyValue.ptr );
		}
	}

	void set( void *application, uint16_t instanceId, uint32_t requestId, uint8_t opcode, KeyValueUpdate keyValueUpdate, bool needsDup ) {
		this->application = application;
		this->instanceId = instanceId;
		this->requestId = requestId;
		this->opcode = opcode;
		if ( needsDup )
			this->keyValueUpdate.dup( keyValueUpdate.size, keyValueUpdate.data, keyValueUpdate.ptr );
		else
			this->keyValueUpdate.set( keyValueUpdate.size, keyValueUpdate.data, keyValueUpdate.ptr );
		this->keyValueUpdate.offset = keyValueUpdate.offset;
		this->keyValueUpdate.length = keyValueUpdate.length;
	}
};

class AcknowledgementInfo {
public:
	pthread_cond_t *condition;
	LOCK_T *lock;
	uint32_t *counter;

	AcknowledgementInfo() {
		condition = 0;
		lock = 0;
		counter = 0;
	}

	AcknowledgementInfo( pthread_cond_t *condition, LOCK_T *lock, uint32_t *counter ) {
		this->condition = condition;
		this->lock = lock;
		this->counter = counter;
	}
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
	bool get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, AcknowledgementInfo > *&map );

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
		std::unordered_multimap<PendingIdentifier, Key> remappingSet;
		std::unordered_multimap<PendingIdentifier, KeyValueUpdate> update;
		std::unordered_multimap<PendingIdentifier, Key> del;
		LOCK_T getLock;
		LOCK_T setLock;
		LOCK_T remappingSetLock;
		LOCK_T updateLock;
		LOCK_T delLock;
	} servers;
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
	struct {
		std::unordered_multimap<PendingIdentifier, AcknowledgementInfo> remove;
		std::unordered_multimap<PendingIdentifier, AcknowledgementInfo> revert;
		LOCK_T removeLock;
		LOCK_T revertLock;
	} ack;
	struct {
		std::unordered_map<uint16_t, std::map<uint32_t, RequestInfo> > requests; // server instance id -> (timestamp, request)
		std::unordered_map<uint16_t, uint32_t> requestsStartTime; // server instance id -> first timestamp to start remap
		std::unordered_map<uint16_t, LOCK_T> requestsLock;
	} replay;

	Pending();

	// Insert (Coordinator)
	bool insertDegradedLockData(
		PendingType type, uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, void *ptr,
		DegradedLockData &degradedLockData,
		bool needsLock = true, bool needsUnlock = true,
		uint32_t timestamp = 0
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
	// Insert (Servers)
	bool insertKey(
		PendingType type, uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, void *ptr,
		Key &key, bool needsLock = true, bool needsUnlock = true,
		uint32_t timestamp = 0
	);
	bool insertKeyValueUpdate(
		PendingType type, uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, void *ptr,
		KeyValueUpdate &keyValueUpdate,
		bool needsLock = true, bool needsUnlock = true,
		uint32_t timestamp = 0
	);
	bool recordRequestStartTime(
		PendingType type, uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, void *ptr,
		struct sockaddr_in addr
	);
	// Insert (Request)
	bool insertRemapList(
		PendingType type, uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, void *ptr,
		RemapList &remapList,
		bool needsLock = true, bool needsUnlock = true,
		uint32_t timestamp = 0
	);
	// Insert (Ack)
	bool insertAck(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr,
		AcknowledgementInfo &ackInfo,
		bool needsLock = true, bool needsUnlock = true,
		uint32_t timestamp = 0
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
	bool findKey(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr,
		PendingIdentifier *pidPtr, Key *keyPtr,
		bool needsLock, bool needsUnlock,
		bool checkKey, char *checkKeyPtr,
		void *keyPtrToBeSet = 0
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

	bool eraseAck(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr = 0,
		PendingIdentifier *pidPtr = 0,
		AcknowledgementInfo *ackInfoPtr = 0,
		bool needsLock = true, bool needsUnlock = true
	);

	// remove all the pending ack with a specific instanceId
	bool eraseAck(
		PendingType type, uint16_t instanceId,
		std::vector<AcknowledgementInfo> *ackPtr = 0,
		bool needsLock = true, bool needsUnlock = true
	);

	// Find
	bool findKeyValue( uint32_t requestId );
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
