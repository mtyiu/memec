#ifndef __APPLICATION_DS_PENDING_HH__
#define __APPLICATION_DS_PENDING_HH__

#include <set>
#include <pthread.h>
#include "../../common/ds/key.hh"

class KeyValueUpdate : public Key {
public:
	uint32_t offset, length;
};

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
	} application;
	struct {
		std::set<Key> get;
		std::set<Key> set;
		std::set<KeyValueUpdate> update;
		std::set<Key> del;
		pthread_mutex_t getLock;
		pthread_mutex_t setLock;
		pthread_mutex_t updateLock;
		pthread_mutex_t delLock;
	} masters;

	Pending() {
		pthread_mutex_init( &this->application.getLock, 0 );
		pthread_mutex_init( &this->application.setLock, 0 );
		pthread_mutex_init( &this->application.updateLock, 0 );
		pthread_mutex_init( &this->application.delLock, 0 );
		pthread_mutex_init( &this->masters.getLock, 0 );
		pthread_mutex_init( &this->masters.setLock, 0 );
		pthread_mutex_init( &this->masters.updateLock, 0 );
		pthread_mutex_init( &this->masters.delLock, 0 );
	}
};

#endif
