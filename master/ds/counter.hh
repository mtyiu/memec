#ifndef __MASTER_DS_COUNTER_HH__
#define __MASTER_DS_COUNTER_HH__

#include <pthread.h>
#include <stdint.h>

class Counter {
private:
	pthread_mutex_t lock;
	uint32_t remapping; /* locking with remapping */
	uint32_t normal;
	uint32_t lockOnly; /* locking without remappping */

public:
	Counter() {
		pthread_mutex_init( &this->lock, 0 );
		this->remapping = 0;
		this->normal = 0;
		this->lockOnly = 0;
	}

	inline void increaseLockOnly() {
		pthread_mutex_lock( &this->lock );
		this->lockOnly++;
		pthread_mutex_unlock( &this->lock );
	}

	inline void decreaseLockOnly() {
		pthread_mutex_lock( &this->lock );
		this->lockOnly--;
		pthread_mutex_unlock( &this->lock );
	}

	inline void increaseRemapping() {
		pthread_mutex_lock( &this->lock );
		this->remapping++;
		pthread_mutex_unlock( &this->lock );
	}

	inline void decreaseRemapping() {
		pthread_mutex_lock( &this->lock );
		this->remapping--;
		pthread_mutex_unlock( &this->lock );
	}

	inline void increaseNormal() {
		pthread_mutex_lock( &this->lock );
		this->normal++;
		pthread_mutex_unlock( &this->lock );
	}

	inline void decreaseNormal() {
		pthread_mutex_lock( &this->lock );
		this->normal--;
		pthread_mutex_unlock( &this->lock );
	}

	inline uint32_t getLockOnly() {
		uint32_t ret;
		pthread_mutex_lock( &this->lock );
		ret = this->lockOnly;
		pthread_mutex_unlock( &this->lock );
		return ret;
	}

	inline uint32_t getRemapping() {
		uint32_t ret;
		pthread_mutex_lock( &this->lock );
		ret = this->remapping;
		pthread_mutex_unlock( &this->lock );
		return ret;
	}

	inline uint32_t getNormal() {
		uint32_t ret;
		pthread_mutex_lock( &this->lock );
		ret = this->normal;
		pthread_mutex_unlock( &this->lock );
		return ret;
	}
};

#endif
