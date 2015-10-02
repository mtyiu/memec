#ifndef __COMMON_DS_PENDING_HH__
#define __COMMON_DS_PENDING_HH__

#include <set>
#include <cstring>
#include "../../common/ds/key.hh"

class KeyValueUpdate : public Key {
public:
	uint32_t offset, length;
};

class PendingIdentifier {
public:
	uint32_t id, parentId;
	void *ptr;

	PendingIdentifier() {
		this->set( 0, 0, 0 );
	}

	PendingIdentifier( uint32_t id, uint32_t parentId, void *ptr ) {
		this->set( id, parentId, ptr );
	}

	void set( uint32_t id, uint32_t parentId, void *ptr ) {
		this->id = id;
		this->parentId = parentId;
		this->ptr = ptr;
	}

	bool operator<( const PendingIdentifier &p ) const {
		if ( this->id < p.id )
			return true;
		if ( this->id > p.id )
			return false;

		return this->ptr < p.ptr;
	}
};

#endif
