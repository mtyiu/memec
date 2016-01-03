#ifndef __SLAVE_DS_PENDING_HH__
#define __SLAVE_DS_PENDING_HH__

#include <set>
#include "pending_data.hh"
#include "../socket/coordinator_socket.hh"
#include "../socket/master_socket.hh"
#include "../socket/slave_peer_socket.hh"
#include "../../common/ds/chunk.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/ds/pending.hh"
#include "../../common/ds/sockaddr_in.hh"
#include "../../common/ds/value.hh"
#include "../../common/lock/lock.hh"
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
};

class ChunkRequest : public Metadata {
public:
	SlavePeerSocket *socket;
	mutable Chunk *chunk;
	bool isDegraded;

	void set( uint32_t listId, uint32_t stripeId, uint32_t chunkId, SlavePeerSocket *socket, Chunk *chunk = 0, bool isDegraded = true ) {
		this->listId = listId;
		this->stripeId = stripeId;
		this->chunkId = chunkId;
		this->socket = socket;
		this->chunk = chunk;
		this->isDegraded = isDegraded;
	}
};

class DegradedOp : public Metadata {
public:
	uint8_t opcode;
	bool isSealed;
	MasterSocket *socket;
	uint32_t *original;
	uint32_t *reconstructed;
	uint32_t reconstructedCount;
	union {
		Key key;
		KeyValueUpdate keyValueUpdate;
	} data;

	void set(
		uint8_t opcode, bool isSealed, MasterSocket *socket,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount, bool dup = true
	) {
		this->opcode = opcode;
		this->isSealed = isSealed;
		this->socket = socket;
		this->listId = listId;
		this->stripeId = stripeId;
		this->chunkId = chunkId;
		if ( reconstructedCount ) {
			if ( dup ) {
				this->original = new uint32_t[ reconstructedCount * 2 ];
				this->reconstructed = new uint32_t[ reconstructedCount * 2 ];
				for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
					this->original[ i * 2     ] = original[ i * 2    ];
					this->original[ i * 2 + 1 ] = original[ i * 2 + 1 ];
					this->reconstructed[ i * 2     ] = reconstructed[ i * 2    ];
					this->reconstructed[ i * 2 + 1 ] = reconstructed[ i * 2 + 1 ];
				}
			} else {
				this->original = original;
				this->reconstructed = reconstructed;
				this->reconstructedCount = reconstructedCount;
			}
		} else {
			this->original = 0;
			this->reconstructed = 0;
		}
	}

	void free() {
		if ( this->original ) delete[] this->original;
		if ( this->reconstructed ) delete[] this->reconstructed;
		this->original = 0;
		this->reconstructed = 0;
	}
};

struct PendingDegradedLock {
	uint32_t count;
	uint32_t total;
};

class PendingReconstruction {
public:
	uint32_t listId;
	uint32_t chunkId;
	uint32_t total;
	std::unordered_set<uint32_t> stripeIds;

	void set( uint32_t listId, uint32_t chunkId, std::unordered_set<uint32_t> &stripeIds ) {
		this->listId = listId;
		this->chunkId = chunkId;
		this->stripeIds = stripeIds;
		this->total = ( uint32_t ) this->stripeIds.size();
	}

	void merge( std::unordered_set<uint32_t> &stripeIds ) {
		this->total += stripeIds.size();
		this->stripeIds.insert(
			stripeIds.begin(),
			stripeIds.end()
		);
	}

	bool erase( uint32_t stripeId ) {
		return this->stripeIds.erase( stripeId ) > 0;
	}
};

class PendingRecovery {
public:
	uint32_t addr;
	uint16_t port;
	uint32_t total;
	std::unordered_set<Metadata> chunks;

	PendingRecovery( uint32_t addr, uint16_t port ) {
		this->addr = addr;
		this->port = port;
		this->total = 0;
	}

	void insert( uint32_t count, uint32_t *buf ) {
		Metadata metadata;
		this->total += count;
		for ( uint32_t i = 0; i < count; i++ ) {
			metadata.set(
				buf[ i * 3     ],
				buf[ i * 3 + 1 ],
				buf[ i * 3 + 2 ]
			);
			this->chunks.insert( metadata );
		}
	}
};

enum PendingType {
	PT_COORDINATOR_RECONSTRUCTION,
	PT_MASTER_REMAPPING_SET,
	PT_MASTER_GET,
	PT_MASTER_UPDATE,
	PT_MASTER_DEL,
	PT_SLAVE_PEER_DEGRADED_OPS,
	PT_SLAVE_PEER_REMAPPING_SET,
	PT_SLAVE_PEER_GET,
	PT_SLAVE_PEER_UPDATE,
	PT_SLAVE_PEER_DEL,
	PT_SLAVE_PEER_GET_CHUNK,
	PT_SLAVE_PEER_SET_CHUNK,
	PT_SLAVE_PEER_UPDATE_CHUNK,
	PT_SLAVE_PEER_DEL_CHUNK,
	PT_SLAVE_PEER_PARITY
};

class Pending {
private:
	bool get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, Key> *&map );
	bool get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, KeyValueUpdate> *&map );
	bool get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, DegradedOp> *&map );
	bool get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, ChunkRequest> *&map );
	bool get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, ChunkUpdate> *&map );
	bool get( PendingType type, LOCK_T *&lock, std::unordered_map<struct sockaddr_in, std::set<PendingData>* > *&map );

public:
	struct {
		std::unordered_map<PendingIdentifier, PendingDegradedLock> releaseDegradedLock;
		std::unordered_map<PendingIdentifier, PendingReconstruction> reconstruction;
		std::unordered_map<PendingIdentifier, PendingRecovery> recovery;
		LOCK_T releaseDegradedLockLock;
		LOCK_T reconstructionLock;
		LOCK_T recoveryLock;
	} coordinators;
	struct {
		std::unordered_multimap<PendingIdentifier, Key> get;
		std::unordered_multimap<PendingIdentifier, KeyValueUpdate> update;
		std::unordered_multimap<PendingIdentifier, Key> del;
		LOCK_T getLock;
		LOCK_T updateLock;
		LOCK_T delLock;
	} masters;
   struct {
		std::unordered_multimap<PendingIdentifier, DegradedOp> degradedOps;
		std::unordered_multimap<PendingIdentifier, Key> get; // Degraded GET for unsealed chunks
		std::unordered_multimap<PendingIdentifier, KeyValueUpdate> update;
		std::unordered_multimap<PendingIdentifier, Key> del;
		std::unordered_multimap<PendingIdentifier, ChunkRequest> getChunk;
		std::unordered_multimap<PendingIdentifier, ChunkRequest> setChunk;
		std::unordered_multimap<PendingIdentifier, ChunkUpdate> updateChunk;
		std::unordered_multimap<PendingIdentifier, ChunkUpdate> deleteChunk;
		std::unordered_map<struct sockaddr_in, std::set<PendingData>* > remappedData;
		std::unordered_map<PendingIdentifier, uint32_t> remappedDataRequest;
		LOCK_T degradedOpsLock;
		LOCK_T getLock;
		LOCK_T updateLock;
		LOCK_T delLock;
		LOCK_T getChunkLock;
		LOCK_T setChunkLock;
		LOCK_T updateChunkLock;
		LOCK_T delChunkLock;
		LOCK_T remappedDataLock;
		LOCK_T remappedDataRequestLock;
	} slavePeers;

	Pending() {
		LOCK_INIT( &this->coordinators.releaseDegradedLockLock );
		LOCK_INIT( &this->coordinators.reconstructionLock );
		LOCK_INIT( &this->coordinators.recoveryLock );
		LOCK_INIT( &this->masters.getLock );
		LOCK_INIT( &this->masters.updateLock );
		LOCK_INIT( &this->masters.delLock );
		LOCK_INIT( &this->slavePeers.degradedOpsLock );
		LOCK_INIT( &this->slavePeers.getLock );
		LOCK_INIT( &this->slavePeers.updateLock );
		LOCK_INIT( &this->slavePeers.delLock );
		LOCK_INIT( &this->slavePeers.getChunkLock );
		LOCK_INIT( &this->slavePeers.setChunkLock );
		LOCK_INIT( &this->slavePeers.updateChunkLock );
		LOCK_INIT( &this->slavePeers.delChunkLock );
		LOCK_INIT( &this->slavePeers.remappedDataLock );
		LOCK_INIT( &this->slavePeers.remappedDataRequestLock );
	}

	// Insert (Coordinator)
	void insertReleaseDegradedLock(
		uint16_t instanceId, uint32_t requestId, CoordinatorSocket *socket, uint32_t count
	);
	bool insertReconstruction(
		uint16_t instanceId, uint32_t requestId, CoordinatorSocket *socket, uint32_t listId, uint32_t chunkId,
		std::unordered_set<uint32_t> &stripeIds
	);
	bool insertRecovery(
		uint16_t instanceId, uint32_t requestId, CoordinatorSocket *socket,
		uint32_t addr, uint16_t port,
		uint32_t count,
		uint32_t *buf
	);

	// Insert (Master)
	bool insertKey(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr,
		Key &key,
		bool needsLock = true, bool needsUnlock = true
	);
	bool insertKeyValueUpdate(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr,
		KeyValueUpdate &keyValueUpdate,
		bool needsLock = true, bool needsUnlock = true
	);
	// Insert (Slave Peers)
	bool insertKey(
		PendingType type, uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, void *ptr,
		Key &key,
		bool needsLock = true, bool needsUnlock = true
	);
	bool insertKeyValueUpdate(
		PendingType type, uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, void *ptr,
		KeyValueUpdate &keyValueUpdate,
		bool needsLock = true, bool needsUnlock = true
	);
	bool insertDegradedOp(
		PendingType type, uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, void *ptr,
		DegradedOp &degradedOp,
		bool needsLock = true, bool needsUnlock = true
	);
	bool insertChunkRequest(
		PendingType type, uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, void *ptr,
		ChunkRequest &chunkRequest,
		bool needsLock = true, bool needsUnlock = true
	);
	bool insertChunkUpdate(
		PendingType type, uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, void *ptr,
		ChunkUpdate &chunkUpdate,
		bool needsLock = true, bool needsUnlock = true
	);
	bool insertRemapData(
		struct sockaddr_in target, uint32_t listId, uint32_t chunkId,
		Key key, Value value
	);
	bool insertRemapDataRequest(
		uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, uint32_t requestCount,
		SlavePeerSocket *target
	);
	// Erase
	bool eraseReleaseDegradedLock(
		uint16_t instanceId, uint32_t requestId, uint32_t count,
		uint32_t &remaining,
		uint32_t &total,
		PendingIdentifier *pidPtr = 0
	);
	bool eraseReconstruction(
		uint16_t instanceId, uint32_t requestId, CoordinatorSocket *&socket,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint32_t &remaining, uint32_t &total,
		PendingIdentifier *pidPtr = 0
	);
	bool eraseRecovery(
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint16_t &instanceId, uint32_t &requestId, CoordinatorSocket *&socket,
		uint32_t &addr, uint16_t &port, uint32_t &remaining, uint32_t &total
	);
	bool eraseKey(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr = 0,
		PendingIdentifier *pidPtr = 0,
		Key *keyPtr = 0,
		bool needsLock = true, bool needsUnlock = true
	);
	bool eraseKeyValueUpdate(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr = 0,
		PendingIdentifier *pidPtr = 0,
		KeyValueUpdate *keyValueUpdatePtr = 0,
		bool needsLock = true, bool needsUnlock = true
	);
	bool eraseDegradedOp(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr = 0,
		PendingIdentifier *pidPtr = 0,
		DegradedOp *degradedOpPtr = 0,
		bool needsLock = true, bool needsUnlock = true
	);
	bool eraseChunkRequest(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr = 0,
		PendingIdentifier *pidPtr = 0,
		ChunkRequest *chunkRequestPtr = 0,
		bool needsLock = true, bool needsUnlock = true
	);
	bool eraseChunkUpdate(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr = 0,
		PendingIdentifier *pidPtr = 0,
		ChunkUpdate *chunkUpdatePtr = 0,
		bool needsLock = true, bool needsUnlock = true
	);
	bool eraseRemapData(
		struct sockaddr_in target,
		std::set<PendingData> **pendingData
	);
	bool decrementRemapDataRequest(
		uint16_t instanceId, uint32_t requestId, PendingIdentifier *pidPtr = 0,
		uint32_t *requestCount = 0
	);

	bool findReconstruction(
		uint16_t instanceId, uint32_t requestId,
		uint32_t stripeId,
		uint32_t &listId, uint32_t &chunkId
	);
	bool findChunkRequest(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr,
		std::unordered_multimap<PendingIdentifier, ChunkRequest>::iterator &it,
		bool needsLock = true, bool needsUnlock = true
	);

	uint32_t count(
		PendingType type, uint16_t instanceId, uint32_t requestId,
		bool needsLock = true, bool needsUnlock = true
	);
};

#endif
