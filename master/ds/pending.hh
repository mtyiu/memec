#ifndef __MASTER_DS_PENDING_HH__
#define __MASTER_DS_PENDING_HH__

#include <set>
#include <cstring>
#include <pthread.h>
#include "../../common/ds/pending.hh"

class Pending {
public:
	struct {
		std::set<Key> get;
		std::set<Key> set;
		std::set<KeyValueUpdate> update;
		std::set<Key> del;
		pthread_mutex_t getLock;
		pthread_mutex_t setLock;
		pthread_mutex_t updateLock;
		pthread_mutex_t delLock;
	} applications;
	struct {
		std::set<Key> get;
		std::set<Key> set;
		std::set<KeyValueUpdate> update;
		std::set<Key> del;
		pthread_mutex_t getLock;
		pthread_mutex_t setLock;
		pthread_mutex_t updateLock;
		pthread_mutex_t delLock;
	} slaves;

	Pending() {
		pthread_mutex_init( &this->applications.getLock, 0 );
		pthread_mutex_init( &this->applications.setLock, 0 );
		pthread_mutex_init( &this->applications.updateLock, 0 );
		pthread_mutex_init( &this->applications.delLock, 0 );
		pthread_mutex_init( &this->slaves.getLock, 0 );
		pthread_mutex_init( &this->slaves.setLock, 0 );
		pthread_mutex_init( &this->slaves.updateLock, 0 );
		pthread_mutex_init( &this->slaves.delLock, 0 );
	}
};

#endif
