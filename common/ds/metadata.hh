#ifndef __COMMON_DS_METADATA_HH__
#define __COMMON_DS_METADATA_HH__

#include <unordered_map>
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

	bool operator==( const Metadata &m ) const {
		return (
			this->listId == m.listId &&
			this->stripeId == m.stripeId &&
			this->chunkId == m.chunkId
		);
	}
};

namespace std {
	template<> struct hash<Metadata> {
		size_t operator()( const Metadata &metadata ) const {
			size_t ret = 0;
			char *ptr = ( char * ) &ret, *tmp;
			tmp = ( char * ) &metadata.stripeId;
			ptr[ 0 ] = tmp[ 0 ];
			ptr[ 1 ] = tmp[ 1 ];
			ptr[ 2 ] = tmp[ 2 ];
			ptr[ 3 ] = tmp[ 3 ];
			tmp = ( char * ) &metadata.listId;
			ptr[ 4 ] = tmp[ 0 ];
			ptr[ 5 ] = tmp[ 1 ];
			tmp = ( char * ) &metadata.chunkId;
			ptr[ 6 ] = tmp[ 0 ];
			ptr[ 7 ] = tmp[ 1 ];
			return ret;
		}
	};
}

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

class DegradedLock {
public:
	uint32_t listId;
	uint32_t chunkId;
	uint8_t keySize;
	char *key;

	DegradedLock() {
		this->listId = 0;
		this->chunkId = 0;
		this->keySize = 0;
		this->key = 0;
	}

	void set( uint32_t listId, uint32_t chunkId, uint8_t keySize, char *key ) {
		this->listId = listId;
		this->chunkId = chunkId;
		this->keySize = keySize;
		this->key = key;
	}
};

#endif
