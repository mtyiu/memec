#ifndef __SLAVE_EVENT_SLAVE_PEER_EVENT_HH__
#define __SLAVE_EVENT_SLAVE_PEER_EVENT_HH__

#include "../protocol/protocol.hh"
#include "../socket/slave_peer_socket.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/ds/chunk.hh"
#include "../../common/ds/packet_pool.hh"
#include "../../common/ds/value.hh"
#include "../../common/event/event.hh"

class MixedChunkBuffer;

enum SlavePeerEventType {
	SLAVE_PEER_EVENT_TYPE_UNDEFINED,
	// Register
	SLAVE_PEER_EVENT_TYPE_REGISTER_REQUEST,
	SLAVE_PEER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE,
	// REMAPPING_SET
	SLAVE_PEER_EVENT_TYPE_REMAPPING_SET_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_REMAPPING_SET_RESPONSE_FAILURE,
	// SET
	SLAVE_PEER_EVENT_TYPE_SET_REQUEST,
	SLAVE_PEER_EVENT_TYPE_SET_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_SET_RESPONSE_FAILURE,
	// FORWARD_KEY
	SLAVE_PEER_EVENT_TYPE_FORWARD_KEY_REQUEST,
	SLAVE_PEER_EVENT_TYPE_FORWARD_KEY_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_FORWARD_KEY_RESPONSE_FAILURE,
	// GET
	SLAVE_PEER_EVENT_TYPE_GET_REQUEST,
	SLAVE_PEER_EVENT_TYPE_GET_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_GET_RESPONSE_FAILURE,
	// DELETE
	SLAVE_PEER_EVENT_TYPE_DELETE_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_DELETE_RESPONSE_FAILURE,
	// UPDATE
	SLAVE_PEER_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_UPDATE_RESPONSE_FAILURE,
	// REMAPPED_UPDATE
	SLAVE_PEER_EVENT_TYPE_REMAPPED_UPDATE_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_REMAPPED_UPDATE_RESPONSE_FAILURE,
	// REMAPPED_DELETE
	SLAVE_PEER_EVENT_TYPE_REMAPPED_DELETE_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_REMAPPED_DELETE_RESPONSE_FAILURE,
	// UPDATE_CHUNK
	SLAVE_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_FAILURE,
	// DELETE_CHUNK
	SLAVE_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_FAILURE,
	// GET_CHUNK
	SLAVE_PEER_EVENT_TYPE_GET_CHUNK_REQUEST,
	SLAVE_PEER_EVENT_TYPE_GET_CHUNK_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_GET_CHUNK_RESPONSE_FAILURE,
	// Batch GET_CHUNK
	SLAVE_PEER_EVENT_TYPE_BATCH_GET_CHUNKS,
	// SET_CHUNK
	SLAVE_PEER_EVENT_TYPE_SET_CHUNK_REQUEST,
	SLAVE_PEER_EVENT_TYPE_SET_CHUNK_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_SET_CHUNK_RESPONSE_FAILURE,
	// FORWARD_CHUNK
	SLAVE_PEER_EVENT_TYPE_FORWARD_CHUNK_REQUEST,
	SLAVE_PEER_EVENT_TYPE_FORWARD_CHUNK_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_FORWARD_CHUNK_RESPONSE_FAILURE,
	// SEAL_CHUNK
	SLAVE_PEER_EVENT_TYPE_SEAL_CHUNK_REQUEST,
	SLAVE_PEER_EVENT_TYPE_SEAL_CHUNK_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_SEAL_CHUNK_RESPONSE_FAILURE,
	// Seal chunk buffer
	SLAVE_PEER_EVENT_TYPE_SEAL_CHUNKS,
	// Send
	SLAVE_PEER_EVENT_TYPE_SEND,
	// Pending
	SLAVE_PEER_EVENT_TYPE_PENDING
};

class SlavePeerEvent : public Event {
public:
	SlavePeerEventType type;
	uint16_t instanceId;
	uint32_t requestId;
	SlavePeerSocket *socket;
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
			bool needsFree;
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
	} message;

	// Register
	void reqRegister( SlavePeerSocket *socket );
	void resRegister( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, bool success = true );
	// REMAPPING_SET
	void resRemappingSet( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, uint32_t listId, uint32_t chunkId, bool success );
	// SET
	void reqSet( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, Key key, Value value );
	void resSet( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, Key key, bool success );
	// Degraded SET
	void reqForwardKey(
		SlavePeerSocket *socket,
		uint8_t opcode,
		uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint8_t keySize, uint32_t valueSize,
		char *key, char *value,
		uint32_t valueUpdateOffset = 0, uint32_t valueUpdateSize = 0, char *valueUpdate = 0
	);
	void resForwardKey(
		SlavePeerSocket *socket, bool success,
		uint8_t opcode,
		uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint8_t keySize, uint32_t valueSize,
		char *key,
		uint32_t valueUpdateOffset = 0, uint32_t valueUpdateSize = 0
	);
	// GET
	void reqGet( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t chunkId, Key &key );
	void resGet( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, KeyValue &keyValue );
	void resGet( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key );
	// UPDATE
	void resUpdate( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t chunkId, Key &key, uint32_t valueUpdateOffset, uint32_t length, uint32_t chunkUpdateOffset, bool success );
	// DELETE
	void resDelete( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t chunkId, Key &key, bool success );
	// REMAPPED_UPDATE
	void resRemappedUpdate( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, bool success );
	// REMAPPED_DELETE
	void resRemappedDelete( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, bool success );
	// UPDATE_CHUNK
	void resUpdateChunk( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata, uint32_t offset, uint32_t length, uint32_t updatingChunkId, bool success );
	// DELETE_CHUNK
	void resDeleteChunk( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata, uint32_t offset, uint32_t length, uint32_t updatingChunkId, bool success );
	// GET_CHUNK
	void reqGetChunk( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata );
	void resGetChunk( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata, bool success, Chunk *chunk = 0 );
	// Batch GET_CHUNK
	void batchGetChunks( SlavePeerSocket *socket, std::vector<uint32_t> *requestIds, std::vector<Metadata> *metadata );
	// SET_CHUNK
	void reqSetChunk( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata, Chunk *chunk, bool needsFree );
	void resSetChunk( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata, bool success );
	// FORWARD_CHUNK
	void reqForwardChunk( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata, Chunk *chunk, bool needsFree );
	void resForwardChunk( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata, bool success );
	// SEAL_CHUNK
	void reqSealChunk( Chunk *chunk );
	void resSealChunk( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata, bool success );
	// Seal chunk buffer
	void reqSealChunks( MixedChunkBuffer *chunkBuffer );
	// Send
	void send( SlavePeerSocket *socket, Packet *packet );
	// Pending
	void pending( SlavePeerSocket *socket );
};

#endif
