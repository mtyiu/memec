#ifndef __MASTER_DS_PENDING_HH__
#define __MASTER_DS_PENDING_HH__

#include <set>
#include <cstring>
#include "../../common/ds/key.hh"
#include "../../common/ds/metadata.hh"

class ChunkUpdate : public Metadata {
public:
	uint32_t offset, length, valueUpdateOffset;
	uint8_t keySize;
	char *key;
	void *ptr;

	void set( uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length, void *ptr = 0 ) {
		this->listId = listId;
		this->stripeId = stripeId;
		this->chunkId = chunkId;
		this->offset = offset;
		this->length = length;
		this->ptr = ptr;
	}

	void setKeyValueUpdate( uint8_t keySize, char *key, uint32_t valueUpdateOffset ) {
		this->keySize = keySize;
		this->key = key;
		this->valueUpdateOffset = valueUpdateOffset;
	}

	bool operator<( const ChunkUpdate &m ) const {
		int ret;
		if ( ! Metadata::operator<( m ) )
			return false;

		if ( this->offset < m.offset )
			return true;
		if ( this->offset > m.offset )
			return false;

		if ( this->length < m.length )
			return true;
		if ( this->length > m.length )
			return false;

		if ( this->valueUpdateOffset < m.valueUpdateOffset )
			return true;
		if ( this->valueUpdateOffset > m.valueUpdateOffset )
			return false;

		if ( this->keySize < m.keySize )
			return true;
		if ( this->keySize > m.keySize )
			return false;

		ret = strncmp( this->key, m.key, this->keySize );
		if ( ret < 0 )
			return true;
		if ( ret > 0 )
			return false;

		return this->ptr < m.ptr;
	}

	bool equal( const ChunkUpdate &c ) const {
		return (
			Metadata::equal( c ) &&
			this->valueUpdateOffset == c.valueUpdateOffset &&
			this->offset == c.offset &&
			this->length == c.length
		);
	}
};

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
	} applications;
	struct {
		std::set<Key> get;
		std::set<Key> set;
		std::set<KeyValueUpdate> update;
		std::set<ChunkUpdate> updateChunk;
		std::set<Key> del;
		std::set<ChunkUpdate> deleteChunk;
	} slaves;
} Pending;

#endif
