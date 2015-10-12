#ifndef __MASTER_DS_PENDING_HH__
#define __MASTER_DS_PENDING_HH__

#include <cstring>
#include <pthread.h>
#include <netinet/in.h>
#include "stats.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/ds/pending.hh"

#define GIGA ( 1000 * 1000 * 1000 )

enum PendingType {
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

class Pending {
private:
	bool get( PendingType type, pthread_mutex_t *&lock, std::unordered_multimap<PendingIdentifier, Key> *&map );
	bool get( PendingType type, pthread_mutex_t *&lock, std::unordered_multimap<PendingIdentifier, KeyValueUpdate> *&map );
	bool get( PendingType type, pthread_mutex_t *&lock, std::unordered_multimap<PendingIdentifier, RemappingRecord> *&map );

public:
	struct {
		std::unordered_multimap<PendingIdentifier, Key> get;
		std::unordered_multimap<PendingIdentifier, Key> set;
		std::unordered_multimap<PendingIdentifier, KeyValueUpdate> update;
		std::unordered_multimap<PendingIdentifier, Key> del;
		pthread_mutex_t getLock;
		pthread_mutex_t setLock;
		pthread_mutex_t updateLock;
		pthread_mutex_t delLock;
	} applications;
	struct {
		std::unordered_multimap<PendingIdentifier, Key> get;
		std::unordered_multimap<PendingIdentifier, Key> set;
		std::unordered_multimap<PendingIdentifier, RemappingRecord> remappingSet;
		std::unordered_multimap<PendingIdentifier, KeyValueUpdate> update;
		std::unordered_multimap<PendingIdentifier, Key> del;
		pthread_mutex_t getLock;
		pthread_mutex_t setLock;
		pthread_mutex_t remappingSetLock;
		pthread_mutex_t updateLock;
		pthread_mutex_t delLock;
	} slaves;
	struct {
		std::unordered_multimap<PendingIdentifier, RequestStartTime> get;
		std::unordered_multimap<PendingIdentifier, RequestStartTime> set;
		pthread_mutex_t getLock;
		pthread_mutex_t setLock;
	} stats;

	Pending();

	bool insertKey( PendingType type, uint32_t id, void *ptr, Key &key, bool needsLock = true, bool needsUnlock = true );
	bool insertKey( PendingType type, uint32_t id, uint32_t parentId, void *ptr, Key &key, bool needsLock = true, bool needsUnlock = true );
	bool insertRemappingRecord( PendingType type, uint32_t id, uint32_t parentId, void *ptr, RemappingRecord &remappingRecord, bool needsLock = true, bool needsUnlock = true );
	bool insertKeyValueUpdate( PendingType type, uint32_t id, void *ptr, KeyValueUpdate &keyValueUpdate, bool needsLock = true, bool needsUnlock = true );
	bool insertKeyValueUpdate( PendingType type, uint32_t id, uint32_t parentId, void *ptr, KeyValueUpdate &keyValueUpdate, bool needsLock = true, bool needsUnlock = true );
	bool recordRequestStartTime( PendingType type, uint32_t id, uint32_t parentId, void *ptr, struct sockaddr_in addr );

	bool findKey( PendingType type, uint32_t id, void *ptr, Key *keyPtr );

	bool eraseKey( PendingType type, uint32_t id, void *ptr = 0, PendingIdentifier *pidPtr = 0, Key *keyPtr = 0, bool needsLock = true, bool needsUnlock = true );
	bool eraseRemappingRecord( PendingType type, uint32_t id, void *ptr = 0, PendingIdentifier *pidPtr = 0, RemappingRecord *remappingRecordPtr = 0, bool needsLock = true, bool needsUnlock = true );
	bool eraseKeyValueUpdate( PendingType type, uint32_t id, void *ptr = 0, PendingIdentifier *pidPtr = 0, KeyValueUpdate *keyValueUpdatePtr = 0, bool needsLock = true, bool needsUnlock = true );
	bool eraseRequestStartTime( PendingType type, uint32_t id, void *ptr, struct timespec &elapsedTime, PendingIdentifier *pidPtr = 0, RequestStartTime *rstPtr = 0 );

	uint32_t count( PendingType type, uint32_t id, bool needsLock = true, bool needsUnlock = true );
};

#endif
