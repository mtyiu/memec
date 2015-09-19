#ifndef __MASTER_DS_PENDING_HH__
#define __MASTER_DS_PENDING_HH__

#include <map>
#include <set>
#include <cstring>
#include <pthread.h>
#include <netinet/in.h>
#include "stats.hh"
#include "../../common/ds/pending.hh"

#define GIGA ( 1000 * 1000 * 1000 )

enum PendingType {
	PT_APPLICATION_GET,
	PT_APPLICATION_SET,
	PT_APPLICATION_UPDATE,
	PT_APPLICATION_DEL,
	PT_SLAVE_GET,
	PT_SLAVE_SET,
	PT_SLAVE_UPDATE,
	PT_SLAVE_DEL
};

class Pending {
private:
	bool get( PendingType type, pthread_mutex_t *&lock, std::map<PendingIdentifier, Key> *&map );
	bool get( PendingType type, pthread_mutex_t *&lock, std::map<PendingIdentifier, KeyValueUpdate> *&map );

public:
	struct {
		std::map<PendingIdentifier, Key> get;
		std::map<PendingIdentifier, Key> set;
		std::map<PendingIdentifier, KeyValueUpdate> update;
		std::map<PendingIdentifier, Key> del;
		pthread_mutex_t getLock;
		pthread_mutex_t setLock;
		pthread_mutex_t updateLock;
		pthread_mutex_t delLock;
	} applications;
	struct {
		std::map<PendingIdentifier, Key> get;
		std::map<PendingIdentifier, Key> set;
		std::map<PendingIdentifier, KeyValueUpdate> update;
		std::map<PendingIdentifier, Key> del;
		pthread_mutex_t getLock;
		pthread_mutex_t setLock;
		pthread_mutex_t updateLock;
		pthread_mutex_t delLock;
	} slaves;
	struct {
		std::map<PendingIdentifier, RequestStartTime> get;
		std::map<PendingIdentifier, RequestStartTime> set;
		pthread_mutex_t getLock;
		pthread_mutex_t setLock;
	} stats;

	Pending();

	bool insertKey( PendingType type, uint32_t id, void *ptr, Key &key, bool needsLock = true, bool needsUnlock = true );
	bool insertKey( PendingType type, uint32_t id, uint32_t parentId, void *ptr, Key &key, bool needsLock = true, bool needsUnlock = true );
	bool insertKeyValueUpdate( PendingType type, uint32_t id, void *ptr, KeyValueUpdate &keyValueUpdate, bool needsLock = true, bool needsUnlock = true );
	bool insertKeyValueUpdate( PendingType type, uint32_t id, uint32_t parentId, void *ptr, KeyValueUpdate &keyValueUpdate, bool needsLock = true, bool needsUnlock = true );
	bool recordRequestStartTime( PendingType type, uint32_t id, uint32_t parentId, void *ptr, struct sockaddr_in addr );

	bool eraseKey( PendingType type, uint32_t id, void *ptr = 0, PendingIdentifier *pidPtr = 0, Key *keyPtr = 0, bool needsLock = true, bool needsUnlock = true );
	bool eraseKeyValueUpdate( PendingType type, uint32_t id, void *ptr = 0, PendingIdentifier *pidPtr = 0, KeyValueUpdate *keyValueUpdatePtr = 0, bool needsLock = true, bool needsUnlock = true );
	bool eraseRequestStartTime( PendingType type, uint32_t id, void *ptr, double &elapsedTime, PendingIdentifier *pidPtr = 0, RequestStartTime *rstPtr = 0 );

	uint32_t count( PendingType type, uint32_t id, bool needsLock = true, bool needsUnlock = true );
};

#endif
