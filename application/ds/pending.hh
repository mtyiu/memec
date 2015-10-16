#ifndef __APPLICATION_DS_PENDING_HH__
#define __APPLICATION_DS_PENDING_HH__

#include <set>
#include "../../common/ds/key.hh"
#include "../../common/lock/lock.hh"

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
		LOCK_T getLock;
		LOCK_T setLock;
		LOCK_T updateLock;
		LOCK_T delLock;
	} application;
	struct {
		std::set<Key> get;
		std::set<Key> set;
		std::set<KeyValueUpdate> update;
		std::set<Key> del;
		LOCK_T getLock;
		LOCK_T setLock;
		LOCK_T updateLock;
		LOCK_T delLock;
	} masters;

	Pending() {
		LOCK_INIT( &this->application.getLock );
		LOCK_INIT( &this->application.setLock );
		LOCK_INIT( &this->application.updateLock );
		LOCK_INIT( &this->application.delLock );
		LOCK_INIT( &this->masters.getLock );
		LOCK_INIT( &this->masters.setLock );
		LOCK_INIT( &this->masters.updateLock );
		LOCK_INIT( &this->masters.delLock );
	}
};

#endif
