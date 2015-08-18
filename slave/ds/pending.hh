#ifndef __SLAVE_DS_PENDING_HH__
#define __SLAVE_DS_PENDING_HH__

#include "../socket/slave_peer_socket.hh"
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

class ChunkRequest : public Metadata {
public:
	SlavePeerSocket *socket;
	mutable Chunk *chunk;

	void set( uint32_t listId, uint32_t stripeId, uint32_t chunkId, SlavePeerSocket *socket, Chunk *chunk = 0 ) {
		this->listId = listId;
		this->stripeId = stripeId;
		this->chunkId = chunkId;
		this->socket = socket;
		this->chunk = chunk;
	}

	bool operator<( const ChunkRequest &m ) const {
		if ( Metadata::operator<( m ) )
			return true;
		if ( this->chunkId > m.chunkId )
			return false;

		// Do not compare the chunk pointer
		return ( this->socket < m.socket );
	}

	bool equal( const ChunkRequest &c ) const {
		return (
			Metadata::equal( c ) &&
			this->socket == c.socket &&
			this->chunk == c.chunk
		);
	}

	bool matchStripe( const ChunkRequest &c ) const {
		return (
			this->listId == c.listId &&
			this->stripeId == c.stripeId
		);
	}
};

class DegradedOp : public Metadata {
public:
	uint8_t opcode;
	union {
		Key key;
		KeyValueUpdate keyValueUpdate;
	} data;

	void set( uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint8_t opcode ) {
		this->listId = listId;
		this->stripeId = stripeId;
		this->chunkId = chunkId;
		this->opcode = opcode;
	}

	bool operator<( const DegradedOp &m ) const {
		if ( Metadata::operator<( m ) )
			return true;

		if ( this->listId == m.listId && this->stripeId == m.stripeId && this->chunkId == m.chunkId ) {
			if ( this->opcode < m.opcode )
				return true;
			if ( this->opcode > m.opcode )
				return false;

			switch( this->opcode ) {
				case PROTO_OPCODE_GET:
				case PROTO_OPCODE_DELETE:
					return ( this->data.key < m.data.key );
				case PROTO_OPCODE_UPDATE:
					return ( this->data.keyValueUpdate < m.data.keyValueUpdate );
			}
		}
		return false;
	}

	bool matchStripe( const DegradedOp &d ) const {
		return (
			this->listId == d.listId &&
			this->stripeId == d.stripeId
		);
	}
};

class Pending {
public:
	struct {
		std::set<Key> get;
		std::set<KeyValueUpdate> update;
		std::set<Key> del;
		pthread_mutex_t getLock;
		pthread_mutex_t updateLock;
		pthread_mutex_t delLock;
	} masters;
   struct {
		std::set<DegradedOp> degradedOps;
		std::set<ChunkRequest> getChunk;
		std::set<ChunkRequest> setChunk;
		std::set<ChunkUpdate> updateChunk;
		std::set<ChunkUpdate> deleteChunk;
		pthread_mutex_t degradedOpsLock;
		pthread_mutex_t getLock;
		pthread_mutex_t setLock;
		pthread_mutex_t updateLock;
		pthread_mutex_t delLock;
	} slavePeers;

	Pending() {
		pthread_mutex_init( &this->masters.getLock, 0 );
		pthread_mutex_init( &this->masters.updateLock, 0 );
		pthread_mutex_init( &this->masters.delLock, 0 );
		pthread_mutex_init( &this->slavePeers.degradedOpsLock, 0 );
		pthread_mutex_init( &this->slavePeers.getLock, 0 );
		pthread_mutex_init( &this->slavePeers.setLock, 0 );
		pthread_mutex_init( &this->slavePeers.updateLock, 0 );
		pthread_mutex_init( &this->slavePeers.delLock, 0 );
	}
};

#endif
