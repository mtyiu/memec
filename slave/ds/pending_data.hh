#ifndef __SLAVE_DS_PENDING_DATA_HH__
#define __SLAVE_DS_PENDING_DATA_HH__

#include "../../common/ds/key.hh"
#include "../../common/ds/value.hh"
#include "../../common/timestamp/timestamp.hh"

class PendingData {
public:
	uint32_t listId;
	uint32_t chunkId;
	Key key;
	Value value;
	Timestamp ts;
	bool isSealed;
	uint32_t offset;

	void set( uint32_t listId, uint32_t chunkId, Key key, Value value, Timestamp timestamp = Timestamp(), bool isSealed = false ) {
		this->set( listId, chunkId, key, value, timestamp, isSealed, 0 );
	}

	void set( uint32_t listId, uint32_t chunkId, Key key, Value value, Timestamp timestamp, bool isSealed, uint32_t offset ) {
		this->listId = listId;
		this->chunkId = chunkId;
		this->key.dup( key.size, key.data );
		this->value.dup( value.size, value.data );
		this->ts = timestamp;
		this->isSealed = isSealed;
		this->offset = offset;
	}

	void setSeal( bool sealed = true ) {
		this->isSealed = sealed;
	}

	void free() {
		this->key.free();
		this->value.free();
	}

	bool operator< ( const PendingData &p ) const {
		return ( this->listId < p.listId || this->chunkId < p.chunkId ||
			this->key < p.key || this->value.size < p.value.size ||
			this->ts < p.ts
		);
	}
};


#endif
