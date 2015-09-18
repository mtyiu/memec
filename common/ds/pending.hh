#ifndef __COMMON_DS_PENDING_HH__
#define __COMMON_DS_PENDING_HH__

#include <set>
#include <cstring>
#include "../../common/ds/key.hh"

class KeyValueUpdate : public Key {
public:
	uint32_t offset, length;

	bool operator<( const KeyValueUpdate &k ) const {
		int ret;
		if ( this->size < k.size )
			return true;
		if ( this->size > k.size )
			return false;

		ret = strncmp( this->data, k.data, this->size );
		if ( ret < 0 )
			return true;
		if ( ret > 0 )
			return false;

		if ( this->ptr < k.ptr )
			return true;
		if ( this->ptr > k.ptr )
			return false;

		if ( this->offset < k.offset )
			return true;
		if ( this->offset > k.offset )
			return false;

		return this->length < k.length;
	}

	bool equal( const KeyValueUpdate &k ) const {
		return (
			Key::equal( k ) &&
			this->offset == k.offset &&
			this->length == k.length
		);
	}
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
