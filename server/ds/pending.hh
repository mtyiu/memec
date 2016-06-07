#ifndef __SERVER_DS_PENDING_HH__
#define __SERVER_DS_PENDING_HH__

#include <set>
#include "pending_data.hh"
#include "../socket/coordinator_socket.hh"
#include "../socket/client_socket.hh"
#include "../socket/server_peer_socket.hh"
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
	ServerPeerSocket *socket;
	mutable Chunk *chunk;
	bool isDegraded;
	bool isSealed;
	bool self;
	uint8_t sealIndicatorCount;
	bool *sealIndicator;

	void set( uint32_t listId, uint32_t stripeId, uint32_t chunkId, ServerPeerSocket *socket, Chunk *chunk = 0, bool isDegraded = true, bool self = false ) {
		this->listId = listId;
		this->stripeId = stripeId;
		this->chunkId = chunkId;
		this->socket = socket;
		this->chunk = chunk;
		this->isDegraded = isDegraded;
		this->isSealed = false;
		this->sealIndicatorCount = 0;
		this->sealIndicator = 0;
		this->self = self;
	}

	void setSealStatus( bool isSealed, uint8_t sealIndicatorCount, bool *sealIndicator ) {
		this->isSealed = isSealed;
		this->sealIndicatorCount = sealIndicatorCount;
		this->sealIndicator = 0;
		if ( sealIndicatorCount ) {
			this->sealIndicator = new bool[ sealIndicatorCount ];
			for ( uint8_t i = 0; i < sealIndicatorCount; i++ )
				this->sealIndicator[ i ] = sealIndicator[ i ];
		}
	}
};

class DegradedOp : public Metadata {
public:
	uint8_t opcode;
	bool isSealed;
	ClientSocket *socket;
	uint32_t *original;
	uint32_t *reconstructed;
	uint32_t reconstructedCount;
	uint32_t ongoingAtChunk;
	union {
		Key key;
		KeyValueUpdate keyValueUpdate;
	} data;
	uint32_t timestamp; // from Client

	void set(
		uint8_t opcode, bool isSealed, ClientSocket *socket,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
		uint32_t ongoingAtChunk, uint32_t timestamp,
		bool dup
	) {
		this->opcode = opcode;
		this->isSealed = isSealed;
		this->socket = socket;
		this->listId = listId;
		this->stripeId = stripeId;
		this->chunkId = chunkId;
		this->timestamp = timestamp;
		this->reconstructedCount = reconstructedCount;
		this->ongoingAtChunk = ongoingAtChunk;
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

// For surviving servers that are reconstructing the lost data
class PendingReconstruction {
public:
	uint32_t listId;
	uint32_t chunkId;
	struct {
		uint32_t total;
		std::unordered_set<uint32_t> stripeIds;
	} chunks;
	struct {
		uint32_t total;
		std::unordered_set<Key> keys;
	} unsealed;

	void set( uint32_t listId, uint32_t chunkId, std::unordered_set<uint32_t> &stripeIds, std::unordered_set<Key> &unsealedKeys ) {
		this->listId = listId;
		this->chunkId = chunkId;
		this->chunks.stripeIds = stripeIds;
		this->chunks.total = ( uint32_t ) stripeIds.size();
		this->unsealed.keys = unsealedKeys;
		this->unsealed.total = ( uint32_t ) unsealedKeys.size();
	}

	void merge( std::unordered_set<uint32_t> &stripeIds ) {
		this->chunks.total += stripeIds.size();
		this->chunks.stripeIds.insert(
			stripeIds.begin(),
			stripeIds.end()
		);
	}

	void merge( std::unordered_set<Key> &unsealedKeys ) {
		this->unsealed.total += unsealedKeys.size();
		this->unsealed.keys.insert(
			unsealedKeys.begin(),
			unsealedKeys.end()
		);
	}

	bool erase( uint32_t stripeId ) {
		return this->chunks.stripeIds.erase( stripeId ) > 0;
	}

	bool erase( Key key ) {
		return this->unsealed.keys.erase( key ) > 0;
	}
};

// For reconstructed servers
class PendingRecovery {
public:
	uint32_t addr;
	uint16_t port;
	struct {
		uint32_t total;
		std::unordered_set<Metadata> metadata;
	} chunks;
	struct {
		uint32_t total;
		std::unordered_set<Key> keys;
	} unsealed;

	PendingRecovery( uint32_t addr, uint16_t port ) {
		this->addr = addr;
		this->port = port;
		this->chunks.total = 0;
		this->unsealed.total = 0;
	}

	void insertMetadata( uint32_t count, uint32_t *buf ) {
		Metadata metadata;
		this->chunks.total += count;
		for ( uint32_t i = 0; i < count; i++ ) {
			metadata.set(
				buf[ i * 3     ],
				buf[ i * 3 + 1 ],
				buf[ i * 3 + 2 ]
			);
			this->chunks.metadata.insert( metadata );
		}
	}

	void insertKeys( uint32_t count, char *keysBuf ) {
		Key key;
		this->unsealed.total += count;
		for ( uint32_t i = 0; i < count; i++ ) {
			key.dup( ( uint8_t ) keysBuf[ 0 ], keysBuf + 1 );
			keysBuf += 1 + key.size;
			this->unsealed.keys.insert( key );
		}
	}
};

class PendingRegistration {
public:
	ServerPeerSocket *socket;
	uint32_t requestId;
	bool success;

	void set( ServerPeerSocket *socket, uint32_t requestId, bool success ) {
		this->socket = socket;
		this->requestId = requestId;
		this->success = success;
	}
};

enum PendingType {
	PT_COORDINATOR_RECONSTRUCTION,
	PT_CLIENT_DEGRADED_SET,
	PT_CLIENT_GET,
	PT_CLIENT_UPDATE,
	PT_CLIENT_DEL,
	PT_SERVER_PEER_DEGRADED_OPS,
	PT_SERVER_PEER_DEGRADED_SET,
	PT_SERVER_PEER_SET,
	PT_SERVER_PEER_GET,
	PT_SERVER_PEER_UPDATE,
	PT_SERVER_PEER_DEL,
	PT_SERVER_PEER_GET_CHUNK,
	PT_SERVER_PEER_SET_CHUNK,
	PT_SERVER_PEER_FORWARD_PARITY_CHUNK,
	PT_SERVER_PEER_FORWARD_KEYS,
	PT_SERVER_PEER_UPDATE_CHUNK,
	PT_SERVER_PEER_DEL_CHUNK,
	PT_SERVER_PEER_PARITY,
	PT_SERVER_PEER_REGISTRATION
};

class Pending {
private:
	bool get( PendingType type, LOCK_T *&lock, std::unordered_set<PendingIdentifier> *&map );
	bool get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, Key> *&map );
	bool get( PendingType type, LOCK_T *&lock, std::unordered_multimap<PendingIdentifier, KeyValue> *&map );
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
	} clients;
   struct {
		std::unordered_multimap<PendingIdentifier, DegradedOp> degradedOps;
		std::unordered_multimap<PendingIdentifier, KeyValue> set; // Degraded SET for unsealed chunks
		std::unordered_multimap<PendingIdentifier, Key> get; // Degraded GET for unsealed chunks
		std::unordered_multimap<PendingIdentifier, KeyValueUpdate> update;
		std::unordered_multimap<PendingIdentifier, Key> del;
		std::unordered_multimap<PendingIdentifier, ChunkRequest> getChunk;
		std::unordered_multimap<PendingIdentifier, ChunkRequest> setChunk;
		std::unordered_multimap<PendingIdentifier, ChunkRequest> forwardParityChunk;
		std::unordered_set<PendingIdentifier> forwardKeys;
		std::unordered_multimap<PendingIdentifier, ChunkUpdate> updateChunk;
		std::unordered_multimap<PendingIdentifier, ChunkUpdate> deleteChunk;
		std::unordered_map<struct sockaddr_in, std::set<PendingData>* > remappedData;
		std::unordered_map<PendingIdentifier, uint32_t> remappedDataRequest;
		std::vector<PendingRegistration> registration;

		LOCK_T degradedOpsLock;
		LOCK_T setLock;
		LOCK_T getLock;
		LOCK_T updateLock;
		LOCK_T delLock;
		LOCK_T getChunkLock;
		LOCK_T setChunkLock;
		LOCK_T forwardParityChunkLock;
		LOCK_T forwardKeysLock;
		LOCK_T updateChunkLock;
		LOCK_T deleteChunkLock;
		LOCK_T remappedDataLock;
		LOCK_T remappedDataRequestLock;
		LOCK_T registrationLock;
	} serverPeers;

	Pending() {
		LOCK_INIT( &this->coordinators.releaseDegradedLockLock );
		LOCK_INIT( &this->coordinators.reconstructionLock );
		LOCK_INIT( &this->coordinators.recoveryLock );
		LOCK_INIT( &this->clients.getLock );
		LOCK_INIT( &this->clients.updateLock );
		LOCK_INIT( &this->clients.delLock );
		LOCK_INIT( &this->serverPeers.degradedOpsLock );
		LOCK_INIT( &this->serverPeers.setLock );
		LOCK_INIT( &this->serverPeers.getLock );
		LOCK_INIT( &this->serverPeers.updateLock );
		LOCK_INIT( &this->serverPeers.delLock );
		LOCK_INIT( &this->serverPeers.getChunkLock );
		LOCK_INIT( &this->serverPeers.setChunkLock );
		LOCK_INIT( &this->serverPeers.forwardParityChunkLock );
		LOCK_INIT( &this->serverPeers.forwardKeysLock );
		LOCK_INIT( &this->serverPeers.updateChunkLock );
		LOCK_INIT( &this->serverPeers.deleteChunkLock );
		LOCK_INIT( &this->serverPeers.remappedDataLock );
		LOCK_INIT( &this->serverPeers.remappedDataRequestLock );
		LOCK_INIT( &this->serverPeers.registrationLock );
	}

	// Insert (Coordinator)
	void insertReleaseDegradedLock(
		uint16_t instanceId, uint32_t requestId, CoordinatorSocket *socket, uint32_t count
	);
	bool insertReconstruction(
		uint16_t instanceId, uint32_t requestId, CoordinatorSocket *socket,
		uint32_t listId, uint32_t chunkId,
		std::unordered_set<uint32_t> &stripeIds,
		std::unordered_set<Key> &unsealedKeys
	);
	bool insertRecovery(
		uint16_t instanceId, uint32_t requestId, CoordinatorSocket *socket,
		uint32_t addr, uint16_t port,
		uint32_t chunkCount, uint32_t *metadataBuf,
		uint32_t unsealedCount, char *keysBuf
	);

	// Insert (Client)
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
	// Insert (Server Peers)
	bool insertKey(
		PendingType type, uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, void *ptr,
		Key &key,
		bool needsLock = true, bool needsUnlock = true
	);
	bool insertKeyValue(
		PendingType type, uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, void *ptr,
		KeyValue &keyValue,
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
		ServerPeerSocket *target
	);
	void insertServerPeerRegistration( uint32_t requestId, ServerPeerSocket *socket, bool success );
	bool insert(
		PendingType type, uint16_t instanceId, uint16_t parentInstanceId, uint32_t requestId, uint32_t parentRequestId, void *ptr,
		bool needsLock = true, bool needsUnlock = true
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
		uint32_t &remainingChunks, uint32_t &remainingKeys,
		uint32_t &totalChunks, uint32_t &totalKeys,
		PendingIdentifier *pidPtr = 0
	);
	bool eraseReconstruction(
		uint16_t instanceId, uint32_t requestId, CoordinatorSocket *&socket,
		uint32_t listId, uint32_t chunkId,
		uint8_t keySize, char *keyStr,
		uint32_t &remainingChunks, uint32_t &remainingKeys,
		uint32_t &totalChunks, uint32_t &totalKeys,
		PendingIdentifier *pidPtr = 0
	);
	bool eraseRecovery(
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint16_t &instanceId, uint32_t &requestId, CoordinatorSocket *&socket,
		uint32_t &addr, uint16_t &port,
		uint32_t &remainingChunks, uint32_t &remainingKeys,
		uint32_t &totalChunks, uint32_t &totalKeys
	);
	bool eraseRecovery(
		uint8_t keySize, char *keyStr,
		uint16_t &instanceId, uint32_t &requestId, CoordinatorSocket *&socket,
		uint32_t &addr, uint16_t &port,
		uint32_t &remainingChunks, uint32_t &remainingKeys,
		uint32_t &totalChunks, uint32_t &totalKeys
	);
	bool eraseKey(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr = 0,
		PendingIdentifier *pidPtr = 0,
		Key *keyPtr = 0,
		bool needsLock = true, bool needsUnlock = true
	);
	bool eraseKeyValue(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr = 0,
		PendingIdentifier *pidPtr = 0,
		KeyValue *keyValuePtr = 0,
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
	bool eraseServerPeerRegistration( uint32_t &requestId, ServerPeerSocket *&socket, bool &success );
	bool erase(
		PendingType type, uint16_t instanceId, uint32_t requestId, void *ptr = 0,
		PendingIdentifier *pidPtr = 0,
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
	bool findReconstruction(
		uint16_t instanceId, uint32_t requestId,
		uint8_t keySize, char *keyStr,
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

	void print( PendingType type, FILE *f, bool needsLock = true, bool needsUnlock = true );
};

#endif
