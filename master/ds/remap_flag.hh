#ifndef __MASTER_DS_REMAP_FLAG_HH__
#define __MASTER_DS_REMAP_FLAG_HH__

#include <pthread.h>

class RemapFlag {
private:
	pthread_mutex_t lock;
	bool isRemapping;

public:
	RemapFlag() {
		pthread_mutex_init( &this->lock, 0 );
		this->isRemapping = false;
	}

	void set( bool isRemapping ) {
		pthread_mutex_lock( &this->lock );
		this->isRemapping = isRemapping;
		pthread_mutex_unlock( &this->lock );
	}

	bool get() {
		bool ret;
		pthread_mutex_lock( &this->lock );
		ret = this->isRemapping;
		pthread_mutex_unlock( &this->lock );
		return ret;
	}
};

#endif
