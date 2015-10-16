#ifndef __COMMON_DS_METADATA_HH__
#define __COMMON_DS_METADATA_HH__

#include <stdint.h>

class Metadata {
public:
	uint32_t listId;
	uint32_t stripeId;
	uint32_t chunkId;

	void clear() {
		this->listId = 0;
		this->stripeId = 0;
		this->chunkId = 0;
	}

	void set( uint32_t listId, uint32_t stripeId, uint32_t chunkId ) {
		this->listId = listId;
		this->stripeId = stripeId;
		this->chunkId = chunkId;
	}

	void clone( const Metadata &m ) {
		this->listId = m.listId;
		this->stripeId = m.stripeId;
		this->chunkId = m.chunkId;
	}

	bool operator<( const Metadata &m ) const {
		if ( this->listId < m.listId )
			return true;
		if ( this->listId > m.listId )
			return false;

		if ( this->stripeId < m.stripeId )
			return true;
		if ( this->stripeId > m.stripeId )
			return false;

		return this->chunkId < m.chunkId;
	}
};

class KeyMetadata : public Metadata {
public:
	uint32_t offset, length;
};

class OpMetadata : public Metadata {
public:
	uint8_t opcode;
};

class RemappingRecord {
public:
	bool sent;
	bool valid;
	uint32_t listId, chunkId;
	void *ptr;

	RemappingRecord() {
		this->sent = false;
		this->valid = false;
		this->listId = 0;
		this->chunkId = 0;
		this->ptr = 0;
	}

	RemappingRecord( uint32_t listId, uint32_t chunkId, void *ptr = 0 ) {
		this->sent = false;
		this->valid = true;
		this->set( listId, chunkId, ptr );
	}

	void set( uint32_t listId, uint32_t chunkId, void *ptr = 0 ) {
		this->listId = listId;
		this->chunkId = chunkId;
		this->ptr = ptr;
		this->valid = true;
	}

	void invalid() {
		this->valid = false;
		this->sent = false;
	}

	bool operator== ( const RemappingRecord &r ) {
		return ( this->listId == r.listId && this->chunkId == r.chunkId );
	}

	bool operator!= ( const RemappingRecord &r ) {
		return ( this->listId != r.listId || this->chunkId != r.chunkId );
	}
};

#endif
