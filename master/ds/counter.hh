#ifndef __MASTER_DS_COUNTER_HH__
#define __MASTER_DS_COUNTER_HH__

#include <stdint.h>
#include "../../common/lock/lock.hh"

class Counter {
private:
	LOCK_T lock;
	uint32_t remapping; /* locking with remapping */
	uint32_t normal;
	uint32_t lockOnly;  /* locking without remappping */
	uint32_t degraded;  /* number of degraded operations */

public:
	Counter() {
		LOCK_INIT( &this->lock );
		this->remapping = 0;
		this->normal = 0;
		this->lockOnly = 0;
		this->degraded = 0;
	}

	inline void increaseLockOnly() {
		LOCK( &this->lock );
		this->lockOnly++;
		UNLOCK( &this->lock );
	}

	inline void decreaseLockOnly() {
		LOCK( &this->lock );
		this->lockOnly--;
		UNLOCK( &this->lock );
	}

	inline void increaseRemapping() {
		LOCK( &this->lock );
		this->remapping++;
		UNLOCK( &this->lock );
	}

	inline void decreaseRemapping() {
		LOCK( &this->lock );
		this->remapping--;
		UNLOCK( &this->lock );
	}

	inline void increaseNormal() {
		LOCK( &this->lock );
		this->normal++;
		UNLOCK( &this->lock );
	}

	inline void decreaseNormal() {
		LOCK( &this->lock );
		this->normal--;
		UNLOCK( &this->lock );
	}

	inline void increaseDegraded() {
		LOCK( &this->lock );
		this->degraded++;
		UNLOCK( &this->lock );
	}

	inline void decreaseDegraded() {
		LOCK( &this->lock );
		this->degraded--;
		UNLOCK( &this->lock );
	}

	inline uint32_t getLockOnly() {
		uint32_t ret;
		LOCK( &this->lock );
		ret = this->lockOnly;
		UNLOCK( &this->lock );
		return ret;
	}

	inline uint32_t getRemapping() {
		uint32_t ret;
		LOCK( &this->lock );
		ret = this->remapping;
		UNLOCK( &this->lock );
		return ret;
	}

	inline uint32_t getNormal() {
		uint32_t ret;
		LOCK( &this->lock );
		ret = this->normal;
		UNLOCK( &this->lock );
		return ret;
	}

	inline uint32_t getDegraded() {
		uint32_t ret;
		LOCK( &this->lock );
		ret = this->degraded;
		UNLOCK( &this->lock );
		return ret;
	}

	inline void getAll( uint32_t &remapping, uint32_t &normal, uint32_t &lockOnly, uint32_t &degraded ) {
		LOCK( &this->lock );
		remapping = this->remapping;
		normal = this->normal;
		lockOnly = this->lockOnly;
		degraded = this->degraded;
		UNLOCK( &this->lock );
	}
};

#endif
