#ifndef __COMMON_DS_PENDING_HH__
#define __COMMON_DS_PENDING_HH__

#include <map>
#include <cstring>
#include "../../common/ds/key.hh"
#include "../../common/hash/hash_func.hh"

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

	bool operator==( const PendingIdentifier &p ) const {
		return ( this->id == p.id );
	}
};

namespace std {
	template<> struct hash<PendingIdentifier> {
		size_t operator()( const PendingIdentifier &pid ) const {
			return ( size_t ) pid.id;
		}
	};
}

#endif
