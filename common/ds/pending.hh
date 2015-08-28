#ifndef __COMMON_DS_PENDING_HH__
#define __COMMON_DS_PENDING_HH__

#include <set>
#include <cstring>
#include "../../common/ds/key.hh"

class KeyValueUpdate : public Key {
public:
	uint32_t offset, length;

	bool operator<( const KeyValueUpdate &k ) const {
		if ( Key::operator<( k ) )
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

#endif
