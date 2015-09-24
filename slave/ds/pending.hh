#ifndef __SLAVE_DS_PENDING_HH__
#define __SLAVE_DS_PENDING_HH__

#include "../socket/slave_peer_socket.hh"
#include "../../common/ds/chunk.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/ds/pending.hh"
#include "../../common/protocol/protocol.hh"

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
		if ( this->listId < m.listId )
			return true;
		if ( this->listId > m.listId )
			return false;

		if ( this->stripeId < m.stripeId )
			return true;
		if ( this->stripeId > m.stripeId )
			return false;

		if ( this->chunkId < m.chunkId )
			return true;
		if ( this->chunkId > m.chunkId )
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

	bool matchStripe( const ChunkUpdate &c ) const {
		return (
			this->listId == c.listId &&
			this->stripeId == c.stripeId &&
			this->offset == c.offset &&
			this->length == c.length &&
			this->valueUpdateOffset == c.valueUpdateOffset &&
			this->keySize == c.keySize &&
			strncmp( this->key, c.key, this->keySize ) == 0
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
		if ( this->listId < m.listId )
			return true;
		if ( this->listId > m.listId )
			return false;

		if ( this->stripeId < m.stripeId )
			return true;
		if ( this->stripeId > m.stripeId )
			return false;

		if ( this->chunkId < m.chunkId )
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
		if ( this->listId < m.listId )
			return true;
		if ( this->listId > m.listId )
			return false;

		if ( this->stripeId < m.stripeId )
			return true;
		if ( this->stripeId > m.stripeId )
			return false;

		if ( this->chunkId < m.chunkId )
			return true;
		if ( this->chunkId > m.chunkId )
			return false;

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
		return false;
	}

	bool matchStripe( const DegradedOp &d ) const {
		return (
			this->listId == d.listId &&
			this->stripeId == d.stripeId
		);
	}
};

class RemappingRecordKey {
public:
	RemappingRecord remap;
	Key key;
};

enum PendingType {
	PT_MASTER_REMAPPING_SET,
	PT_MASTER_GET,
	PT_MASTER_UPDATE,
	PT_MASTER_DEL,
	PT_SLAVE_PEER_DEGRADED_OPS,
	PT_SLAVE_PEER_REMAPPING_SET,
	PT_SLAVE_PEER_GET_CHUNK,
	PT_SLAVE_PEER_SET_CHUNK,
	PT_SLAVE_PEER_UPDATE_CHUNK,
	PT_SLAVE_PEER_DEL_CHUNK
};

class Pending {
private:
	bool get( PendingType type, pthread_mutex_t *&lock, std::map<PendingIdentifier, Key> *&map );
	bool get( PendingType type, pthread_mutex_t *&lock, std::map<PendingIdentifier, RemappingRecordKey> *&map );
	bool get( PendingType type, pthread_mutex_t *&lock, std::map<PendingIdentifier, KeyValueUpdate> *&map );
	bool get( PendingType type, pthread_mutex_t *&lock, std::map<PendingIdentifier, DegradedOp> *&map );
	bool get( PendingType type, pthread_mutex_t *&lock, std::map<PendingIdentifier, ChunkRequest> *&map );
	bool get( PendingType type, pthread_mutex_t *&lock, std::map<PendingIdentifier, ChunkUpdate> *&map );

public:
	struct {
		std::map<PendingIdentifier, RemappingRecordKey> remappingSet;
		std::map<PendingIdentifier, Key> get;
		std::map<PendingIdentifier, KeyValueUpdate> update;
		std::map<PendingIdentifier, Key> del;
		pthread_mutex_t remappingSetLock;
		pthread_mutex_t getLock;
		pthread_mutex_t updateLock;
		pthread_mutex_t delLock;
	} masters;
   struct {
		std::map<PendingIdentifier, DegradedOp> degradedOps;
		std::map<PendingIdentifier, RemappingRecordKey> remappingSet;
		std::map<PendingIdentifier, ChunkRequest> getChunk;
		std::map<PendingIdentifier, ChunkRequest> setChunk;
		std::map<PendingIdentifier, ChunkUpdate> updateChunk;
		std::map<PendingIdentifier, ChunkUpdate> deleteChunk;
		pthread_mutex_t degradedOpsLock;
		pthread_mutex_t remappingSetLock;
		pthread_mutex_t getChunkLock;
		pthread_mutex_t setChunkLock;
		pthread_mutex_t updateChunkLock;
		pthread_mutex_t delChunkLock;
	} slavePeers;

	Pending() {
		pthread_mutex_init( &this->masters.remappingSetLock, 0 );
		pthread_mutex_init( &this->masters.getLock, 0 );
		pthread_mutex_init( &this->masters.updateLock, 0 );
		pthread_mutex_init( &this->masters.delLock, 0 );
		pthread_mutex_init( &this->slavePeers.degradedOpsLock, 0 );
		pthread_mutex_init( &this->slavePeers.remappingSetLock, 0 );
		pthread_mutex_init( &this->slavePeers.getChunkLock, 0 );
		pthread_mutex_init( &this->slavePeers.setChunkLock, 0 );
		pthread_mutex_init( &this->slavePeers.updateChunkLock, 0 );
		pthread_mutex_init( &this->slavePeers.delChunkLock, 0 );
	}

	bool insertRemappingRecordKey( PendingType type, uint32_t id, void *ptr, RemappingRecordKey &remappingRecordKey, bool needsLock = true, bool needsUnlock = true );
	bool insertRemappingRecordKey( PendingType type, uint32_t id, uint32_t parentId, void *ptr, RemappingRecordKey &remappingRecordKey, bool needsLock = true, bool needsUnlock = true );
	bool insertKey( PendingType type, uint32_t id, void *ptr, Key &key, bool needsLock = true, bool needsUnlock = true );
	bool insertKeyValueUpdate( PendingType type, uint32_t id, void *ptr, KeyValueUpdate &keyValueUpdate, bool needsLock = true, bool needsUnlock = true );
	bool insertDegradedOp( PendingType type, uint32_t id, uint32_t parentId, void *ptr, DegradedOp &degradedOp, bool needsLock = true, bool needsUnlock = true );
	bool insertChunkRequest( PendingType type, uint32_t id, uint32_t parentId, void *ptr, ChunkRequest &chunkRequest, bool needsLock = true, bool needsUnlock = true );
	bool insertChunkUpdate( PendingType type, uint32_t id, uint32_t parentId, void *ptr, ChunkUpdate &chunkUpdate, bool needsLock = true, bool needsUnlock = true );

	bool eraseRemappingRecordKey( PendingType type, uint32_t id, void *ptr = 0, PendingIdentifier *pidPtr = 0, RemappingRecordKey *remappingRecordKeyPtr = 0, bool needsLock = true, bool needsUnlock = true );
	bool eraseKey( PendingType type, uint32_t id, void *ptr = 0, PendingIdentifier *pidPtr = 0, Key *keyPtr = 0, bool needsLock = true, bool needsUnlock = true );
	bool eraseKeyValueUpdate( PendingType type, uint32_t id, void *ptr = 0, PendingIdentifier *pidPtr = 0, KeyValueUpdate *keyValueUpdatePtr = 0, bool needsLock = true, bool needsUnlock = true );
	bool eraseDegradedOp( PendingType type, uint32_t id, void *ptr = 0, PendingIdentifier *pidPtr = 0, DegradedOp *degradedOpPtr = 0, bool needsLock = true, bool needsUnlock = true );
	bool eraseChunkRequest( PendingType type, uint32_t id, void *ptr = 0, PendingIdentifier *pidPtr = 0, ChunkRequest *chunkRequestPtr = 0, bool needsLock = true, bool needsUnlock = true );
	bool eraseChunkUpdate( PendingType type, uint32_t id, void *ptr = 0, PendingIdentifier *pidPtr = 0, ChunkUpdate *chunkUpdatePtr = 0, bool needsLock = true, bool needsUnlock = true );

	bool findChunkRequest( PendingType type, uint32_t id, void *ptr, std::map<PendingIdentifier, ChunkRequest>::iterator &it, bool needsLock = true, bool needsUnlock = true );

	uint32_t count( PendingType type, uint32_t id, bool needsLock = true, bool needsUnlock = true );
};

#endif
