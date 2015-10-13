#ifndef __MASTER_DS_REMAP_FLAG_HH__
#define __MASTER_DS_REMAP_FLAG_HH__

#include "../../common/lock/lock.hh"

class RemapFlag {
private:
	LOCK_T lock;
	bool isRemapping;

public:
	RemapFlag() {
		LOCK_INIT( &this->lock, 0 );
		this->isRemapping = false;
	}

	void set( bool isRemapping ) {
		LOCK( &this->lock );
		this->isRemapping = isRemapping;
		UNLOCK( &this->lock );
	}

	bool get() {
		bool ret;
		LOCK( &this->lock );
		ret = this->isRemapping;
		UNLOCK( &this->lock );
		return ret;
	}
};

#endif
