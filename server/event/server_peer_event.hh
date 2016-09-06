#ifndef __SERVER_EVENT_SERVER_PEER_EVENT_HH__
#define __SERVER_EVENT_SERVER_PEER_EVENT_HH__

#include "../protocol/protocol.hh"
#include "../socket/server_peer_socket.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/ds/chunk.hh"
#include "../../common/ds/packet_pool.hh"
#include "../../common/ds/value.hh"
#include "../../common/event/event.hh"

class MixedChunkBuffer;

enum ServerPeerEventType {
	SERVER_PEER_EVENT_TYPE_UNDEFINED,
	// Register
	SERVER_PEER_EVENT_TYPE_REGISTER_REQUEST,
	SERVER_PEER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS,
	SERVER_PEER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE,
	// DEGRADED_SET
	SERVER_PEER_EVENT_TYPE_DEGRADED_SET_RESPONSE_SUCCESS,
	SERVER_PEER_EVENT_TYPE_DEGRADED_SET_RESPONSE_FAILURE,
	// SET
	SERVER_PEER_EVENT_TYPE_SET_REQUEST,
	SERVER_PEER_EVENT_TYPE_SET_RESPONSE_SUCCESS,
	SERVER_PEER_EVENT_TYPE_SET_RESPONSE_FAILURE,
	// FORWARD_KEY
	SERVER_PEER_EVENT_TYPE_FORWARD_KEY_REQUEST,
	SERVER_PEER_EVENT_TYPE_FORWARD_KEY_RESPONSE_SUCCESS,
	SERVER_PEER_EVENT_TYPE_FORWARD_KEY_RESPONSE_FAILURE,
	// GET
	SERVER_PEER_EVENT_TYPE_GET_REQUEST,
	SERVER_PEER_EVENT_TYPE_GET_RESPONSE_SUCCESS,
	SERVER_PEER_EVENT_TYPE_GET_RESPONSE_FAILURE,
	// DELETE
	SERVER_PEER_EVENT_TYPE_DELETE_RESPONSE_SUCCESS,
	SERVER_PEER_EVENT_TYPE_DELETE_RESPONSE_FAILURE,
	// UPDATE
	SERVER_PEER_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS,
	SERVER_PEER_EVENT_TYPE_UPDATE_RESPONSE_FAILURE,
	// REMAPPED_UPDATE
	SERVER_PEER_EVENT_TYPE_REMAPPED_UPDATE_RESPONSE_SUCCESS,
	SERVER_PEER_EVENT_TYPE_REMAPPED_UPDATE_RESPONSE_FAILURE,
	// REMAPPED_DELETE
	SERVER_PEER_EVENT_TYPE_REMAPPED_DELETE_RESPONSE_SUCCESS,
	SERVER_PEER_EVENT_TYPE_REMAPPED_DELETE_RESPONSE_FAILURE,
	// UPDATE_CHUNK
	SERVER_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_SUCCESS,
	SERVER_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_FAILURE,
	// DELETE_CHUNK
	SERVER_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_SUCCESS,
	SERVER_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_FAILURE,
	// GET_CHUNK
	SERVER_PEER_EVENT_TYPE_GET_CHUNK_REQUEST,
	SERVER_PEER_EVENT_TYPE_GET_CHUNK_RESPONSE_SUCCESS,
	SERVER_PEER_EVENT_TYPE_GET_CHUNK_RESPONSE_FAILURE,
	// Batch GET_CHUNK
	SERVER_PEER_EVENT_TYPE_BATCH_GET_CHUNKS,
	// SET_CHUNK
	SERVER_PEER_EVENT_TYPE_SET_CHUNK_REQUEST,
	SERVER_PEER_EVENT_TYPE_SET_CHUNK_RESPONSE_SUCCESS,
	SERVER_PEER_EVENT_TYPE_SET_CHUNK_RESPONSE_FAILURE,
	// FORWARD_CHUNK
	SERVER_PEER_EVENT_TYPE_FORWARD_CHUNK_REQUEST,
	SERVER_PEER_EVENT_TYPE_FORWARD_CHUNK_RESPONSE_SUCCESS,
	SERVER_PEER_EVENT_TYPE_FORWARD_CHUNK_RESPONSE_FAILURE,
	// SEAL_CHUNK
	SERVER_PEER_EVENT_TYPE_SEAL_CHUNK_REQUEST,
	SERVER_PEER_EVENT_TYPE_SEAL_CHUNK_RESPONSE_SUCCESS,
	SERVER_PEER_EVENT_TYPE_SEAL_CHUNK_RESPONSE_FAILURE,
	// Seal chunk buffer
	SERVER_PEER_EVENT_TYPE_SEAL_CHUNKS,
	// Reconstructed unsealed keys
	SERVER_PEER_EVENT_TYPE_UNSEALED_KEYS_RESPONSE_SUCCESS,
	SERVER_PEER_EVENT_TYPE_UNSEALED_KEYS_RESPONSE_FAILURE,
	// Defer
	SERVER_PEER_EVENT_TYPE_DEFERRED,
	// Send
	SERVER_PEER_EVENT_TYPE_SEND,
	// Pending
	SERVER_PEER_EVENT_TYPE_PENDING
};

class ServerPeerEvent : public Event<ServerPeerSocket> {
public:
	ServerPeerEventType type;
	uint32_t timestamp;
	struct {
		struct {
			Metadata metadata;
			uint32_t offset;
			uint32_t length;
			uint32_t updatingChunkId;
		} chunkUpdate;
		struct {
			uint32_t listId, chunkId;
			Key key;
			KeyValue keyValue;
		} get;
		struct {
			uint32_t listId, stripeId, chunkId, valueUpdateOffset, chunkUpdateOffset, length;
			Key key;
		} update;
		struct {
			uint32_t listId, stripeId, chunkId;
			Key key;
		} del;
		struct {
			Metadata metadata;
			Chunk *chunk;
			uint32_t chunkBufferIndex;
			bool needsFree;
			uint8_t sealIndicatorCount;
			bool *sealIndicator;
			bool isMigrating;
		} chunk;
		MixedChunkBuffer *chunkBuffer;
		struct {
			Packet *packet;
		} send;
		struct {
			std::vector<uint32_t> *requestIds;
			std::vector<Metadata> *metadata;
		} batchGetChunks;
		struct {
			uint32_t listId, chunkId;
			Key key;
		} remap;
		struct {
			Key key;
			Value value;
		} set;
		struct {
			uint8_t opcode;
			uint32_t listId, stripeId, chunkId;
			uint8_t keySize;
			uint32_t valueSize;
			char *key, *value;
			struct {
				uint32_t offset, length;
				char *data;
			} update;
		} forwardKey;
		struct {
			Key key;
			uint32_t valueUpdateOffset;
			uint32_t valueUpdateSize;
		} remappingUpdate;
		struct {
			Key key;
		} remappingDel;
		struct {
			struct BatchKeyValueHeader header;
		} unsealedKeys;
		struct {
			uint8_t opcode;
			char *buf;
			size_t size;
		} defer;
	} message;

	// Register
	inline void reqRegister( ServerPeerSocket *socket ) {
		this->type = SERVER_PEER_EVENT_TYPE_REGISTER_REQUEST;
		this->socket = socket;
	}

	inline void resRegister( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, bool success = true ) {
		this->type = success ? SERVER_PEER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
	}

	// DEGRADED_SET
	inline void resDegradedSet(
		ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId,
		Key &key, uint32_t listId, uint32_t chunkId, bool success
	) {
		this->type = success ? SERVER_PEER_EVENT_TYPE_DEGRADED_SET_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_DEGRADED_SET_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
		this->message.remap = {
			.listId = listId,
			.chunkId = chunkId,
			.key = key
		};
	}

	// SET
	inline void reqSet(
		ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId,
		Key key, Value value
	) {
		this->type = SERVER_PEER_EVENT_TYPE_SET_REQUEST;
		this->set( instanceId, requestId, socket );
		this->message.set = {
			.key = key,
			.value = value
		};
	}

	inline void resSet(
		ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId,
		Key key, bool success
	) {
		this->type = success ? SERVER_PEER_EVENT_TYPE_SET_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_SET_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
		this->message.set.key = key;
	}

	// Degraded SET
	inline void reqForwardKey(
		ServerPeerSocket *socket,
		uint8_t opcode,
		uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint8_t keySize, uint32_t valueSize,
		char *key, char *value,
		uint32_t valueUpdateOffset = 0, uint32_t valueUpdateSize = 0, char *valueUpdate = 0
	) {
		bool isUpdate = ( opcode == PROTO_OPCODE_DEGRADED_UPDATE );
		this->type = SERVER_PEER_EVENT_TYPE_FORWARD_KEY_REQUEST;
		this->set( instanceId, requestId, socket );
		this->message.forwardKey = {
			.opcode = opcode,
			.listId = listId,
			.stripeId = stripeId,
			.chunkId = chunkId,
			.keySize = keySize,
			.valueSize = valueSize,
			.key = key,
			.value = value,
			.update = {
				.offset = isUpdate ? valueUpdateOffset : 0,
				.length = isUpdate ? valueUpdateSize   : 0,
				.data   = isUpdate ? valueUpdate       : 0
			}
		};
	}

	inline void resForwardKey(
		ServerPeerSocket *socket, bool success,
		uint8_t opcode,
		uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint8_t keySize, uint32_t valueSize,
		char *key,
		uint32_t valueUpdateOffset = 0, uint32_t valueUpdateSize = 0
	) {
		bool isUpdate = ( opcode == PROTO_OPCODE_DEGRADED_UPDATE );
		this->type = success ? SERVER_PEER_EVENT_TYPE_FORWARD_KEY_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_FORWARD_KEY_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
		this->message.forwardKey = {
			.opcode = opcode,
			.listId = listId,
			.stripeId = stripeId,
			.chunkId = chunkId,
			.keySize = keySize,
			.valueSize = valueSize,
			.key = key,
			.value = 0,
			.update = {
				.offset = isUpdate ? valueUpdateOffset : 0,
				.length = isUpdate ? valueUpdateSize   : 0,
				.data   = 0
			}
		};
	}

	// GET
	inline void reqGet(
		ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t chunkId, Key &key
	) {
		this->type = SERVER_PEER_EVENT_TYPE_GET_REQUEST;
		this->set( instanceId, requestId, socket );
		this->message.get.listId = listId;
		this->message.get.chunkId = chunkId;
		this->message.get.key = key;
	}

	inline void resGet( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, KeyValue &keyValue ) {
		this->type = SERVER_PEER_EVENT_TYPE_GET_RESPONSE_SUCCESS;
		this->set( instanceId, requestId, socket );
		this->message.get.keyValue = keyValue;
	}

	inline void resGet( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key ) {
		this->type = SERVER_PEER_EVENT_TYPE_GET_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
		this->message.get.key = key;
	}

	// UPDATE
	inline void resUpdate(
		ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		Key &key, uint32_t valueUpdateOffset, uint32_t length,
		uint32_t chunkUpdateOffset, bool success
	) {
		this->type = success ? SERVER_PEER_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_UPDATE_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
		this->message.update = {
			.listId = listId,
			.stripeId = stripeId,
			.chunkId = chunkId,
			.valueUpdateOffset = valueUpdateOffset,
			.chunkUpdateOffset = chunkUpdateOffset,
			.length = length,
			.key = key
		};
	}

	// DELETE
	inline void resDelete(
		ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		Key &key, bool success
	) {
		this->type = success ? SERVER_PEER_EVENT_TYPE_DELETE_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_DELETE_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
		this->message.del = {
			.listId = listId,
			.stripeId = stripeId,
			.chunkId = chunkId,
			.key = key
		};
	}

	// REMAPPED_UPDATE
	inline void resRemappedUpdate(
		ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId,
		Key &key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize,
		bool success
	) {
		this->type = success ? SERVER_PEER_EVENT_TYPE_REMAPPED_UPDATE_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_REMAPPED_UPDATE_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
		this->message.remappingUpdate = {
			.key = key,
			.valueUpdateOffset = valueUpdateOffset,
			.valueUpdateSize = valueUpdateSize
		};
	}

	// REMAPPED_DELETE
	inline void resRemappedDelete( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, bool success ) {
		this->type = success ? SERVER_PEER_EVENT_TYPE_REMAPPED_DELETE_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_REMAPPED_DELETE_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
		this->message.remappingDel.key = key;
	}

	// UPDATE_CHUNK
	inline void resUpdateChunk(
		ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId,
		Metadata &metadata, uint32_t offset, uint32_t length,
		uint32_t updatingChunkId, bool success
	) {
		this->type = success ? SERVER_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
		this->message.chunkUpdate = {
			.metadata = metadata,
			.offset = offset,
			.length = length,
			.updatingChunkId = updatingChunkId
		};
	}

	// DELETE_CHUNK
	inline void resDeleteChunk(
		ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId,
		Metadata &metadata, uint32_t offset, uint32_t length,
		uint32_t updatingChunkId, bool success
	) {
		this->type = success ? SERVER_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
		this->message.chunkUpdate = {
			.metadata = metadata,
			.offset = offset,
			.length = length,
			.updatingChunkId = updatingChunkId
		};
	}

	// GET_CHUNK
	inline void reqGetChunk( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata ) {
		this->type = SERVER_PEER_EVENT_TYPE_GET_CHUNK_REQUEST;
		this->set( instanceId, requestId, socket );
		this->message.chunk.metadata = metadata;
		this->message.chunk.chunk = 0;
	}

	inline void resGetChunk(
		ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId,
		Metadata &metadata, bool success, uint32_t chunkBufferIndex,
		Chunk *chunk, uint8_t sealIndicatorCount, bool *sealIndicator
	) {
		this->type = success ? SERVER_PEER_EVENT_TYPE_GET_CHUNK_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_GET_CHUNK_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
		this->message.chunk = {
			.metadata = metadata,
			.chunk = chunk,
			.chunkBufferIndex = chunkBufferIndex,
			.needsFree = false,
			.sealIndicatorCount = sealIndicatorCount,
			.sealIndicator = sealIndicator
		};
	}

	// Batch GET_CHUNK
	inline void batchGetChunks( ServerPeerSocket *socket, std::vector<uint32_t> *requestIds, std::vector<Metadata> *metadata ) {
		this->type = SERVER_PEER_EVENT_TYPE_BATCH_GET_CHUNKS;
		this->socket = socket;
		this->message.batchGetChunks = {
			.requestIds = requestIds,
			.metadata = metadata
		};
	}

	// SET_CHUNK
	inline void reqSetChunk(
		ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId,
		Metadata &metadata, Chunk *chunk, bool needsFree, bool isMigrating = false
	) {
		this->type = SERVER_PEER_EVENT_TYPE_SET_CHUNK_REQUEST;
		this->set( instanceId, requestId, socket );
		this->message.chunk.metadata = metadata;
		this->message.chunk.chunk = chunk;
		this->message.chunk.needsFree = needsFree;
		this->message.chunk.isMigrating = isMigrating;
	}

	inline void resSetChunk(
		ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId,
		Metadata &metadata, bool success
	) {
		this->type = success ? SERVER_PEER_EVENT_TYPE_SET_CHUNK_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_SET_CHUNK_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
		this->message.chunk.metadata = metadata;
		this->message.chunk.chunk = 0;
	}

	// FORWARD_CHUNK
	inline void reqForwardChunk(
		ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId,
		Metadata &metadata, Chunk *chunk, bool needsFree
	) {
		this->type = SERVER_PEER_EVENT_TYPE_FORWARD_CHUNK_REQUEST;
		this->set( instanceId, requestId, socket );
		this->message.chunk.metadata = metadata;
		this->message.chunk.chunk = chunk;
		this->message.chunk.needsFree = needsFree;
	}

	inline void resForwardChunk( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata, bool success ) {
		this->type = success ? SERVER_PEER_EVENT_TYPE_FORWARD_CHUNK_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_FORWARD_CHUNK_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
		this->message.chunk.metadata = metadata;
		this->message.chunk.chunk = 0;
	}

	// SEAL_CHUNK
	inline void reqSealChunk( Chunk *chunk ) {
		this->type = SERVER_PEER_EVENT_TYPE_SEAL_CHUNK_REQUEST;
		this->message.chunk.chunk = chunk;
	}

	inline void resSealChunk( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata, bool success ) {
		this->type = success ? SERVER_PEER_EVENT_TYPE_SEAL_CHUNK_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_SEAL_CHUNK_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
		this->message.chunk.metadata = metadata;
		this->message.chunk.chunk = 0;
	}

	// Seal chunk buffer
	inline void reqSealChunks( MixedChunkBuffer *chunkBuffer ) {
		this->type = SERVER_PEER_EVENT_TYPE_SEAL_CHUNKS;
		this->message.chunkBuffer = chunkBuffer;
	}

	// Reconstructed unsealed keys
	inline void resUnsealedKeys(
		ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId,
		struct BatchKeyValueHeader &header, bool success
	) {
		this->type = success ? SERVER_PEER_EVENT_TYPE_UNSEALED_KEYS_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_UNSEALED_KEYS_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
		this->message.unsealedKeys.header = header;
	}

	// Defer
	inline void defer(
		ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId,
		uint8_t opcode, char *buf, size_t size
	) {
		this->type = SERVER_PEER_EVENT_TYPE_DEFERRED;
		this->set( instanceId, requestId, socket );
		this->message.defer = {
			.opcode = opcode,
			.buf = new char[ size ],
			.size = size
		};
		memcpy( this->message.defer.buf, buf, size );
	}

	// Send
	inline void send( ServerPeerSocket *socket, Packet *packet ) {
		this->type = SERVER_PEER_EVENT_TYPE_SEND;
		this->socket = socket;
		this->message.send.packet = packet;
	}

	// Pending
	inline void pending( ServerPeerSocket *socket ) {
		this->type = SERVER_PEER_EVENT_TYPE_PENDING;
		this->socket = socket;
	}
};

#endif
