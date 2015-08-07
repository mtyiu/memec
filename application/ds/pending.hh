#ifndef __APPLICATION_DS_PENDING_HH__
#define __APPLICATION_DS_PENDING_HH__

#include <set>
#include "../../common/ds/key.hh"

class KeyValueUpdate : public Key {
public:
	uint32_t offset, length;

	bool operator<( const KeyValueUpdate &k ) const {
		if ( ! Key::operator<( k ) )
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

typedef struct {
	struct {
		std::set<Key> get;
		std::set<Key> set;
		std::set<KeyValueUpdate> update;
		std::set<Key> del;
	} application;
	struct {
		std::set<Key> get;
		std::set<Key> set;
		std::set<KeyValueUpdate> update;
		std::set<Key> del;
	} masters;
} Pending;

#endif
