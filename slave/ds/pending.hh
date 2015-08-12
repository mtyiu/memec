#ifndef __SLAVE_DS_PENDING_HH__
#define __SLAVE_DS_PENDING_HH__

#include "../../common/ds/metadata.hh"
#include "../../common/ds/pending.hh"

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

		if ( this->ptr < m.ptr )
			return true;
		if ( this->ptr > m.ptr )
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

		return ret < 0;
	}

	bool equal( const ChunkUpdate &c ) const {
		return (
			Metadata::equal( c ) &&
			this->offset == c.offset &&
			this->length == c.length
		);
	}
};

typedef struct {
	struct {
		std::set<Key> get;
		std::set<KeyValueUpdate> update;
		std::set<Key> del;
	} masters;
   struct {
		// std::set<ChunkUpdate> getChunk;
		std::set<ChunkUpdate> updateChunk;
		std::set<ChunkUpdate> deleteChunk;
	} slavePeers;
} Pending;

#endif
