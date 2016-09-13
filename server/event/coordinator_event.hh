#ifndef __SERVER_EVENT_COORDINATOR_EVENT_HH__
#define __SERVER_EVENT_COORDINATOR_EVENT_HH__

#include "../socket/coordinator_socket.hh"
#include "../../common/event/event.hh"

enum CoordinatorEventType {
	COORDINATOR_EVENT_TYPE_REGISTER_REQUEST,
	COORDINATOR_EVENT_TYPE_SYNC,
	COORDINATOR_EVENT_TYPE_RELEASE_DEGRADED_LOCK_RESPONSE_SUCCESS,
	COORDINATOR_EVENT_TYPE_SERVER_RECONSTRUCTED_MESSAGE_RESPONSE,
	COORDINATOR_EVENT_TYPE_RECONSTRUCTION_RESPONSE_SUCCESS,
	COORDINATOR_EVENT_TYPE_RECONSTRUCTION_UNSEALED_RESPONSE_SUCCESS,
	COORDINATOR_EVENT_TYPE_PROMOTE_BACKUP_SERVER_RESPONSE_SUCCESS,
	COORDINATOR_EVENT_TYPE_RESPONSE_PARITY_MIGRATE,
	COORDINATOR_EVENT_TYPE_RESPONSE_SCALING_MIGRATION,
	COORDINATOR_EVENT_TYPE_PENDING
};

class CoordinatorEvent : public Event<CoordinatorSocket> {
public:
	CoordinatorEventType type;
	union {
		struct {
			uint32_t addr;
			uint16_t port;
		} address;
		struct {
			uint32_t count;
		} degraded;
		struct {
			uint32_t listId;
			uint32_t chunkId;
			uint32_t numStripes;
		} reconstruction;
		struct {
			uint32_t listId;
			uint32_t chunkId;
			uint32_t keysCount;
		} reconstructionUnsealed;
		struct {
			uint32_t addr;
			uint16_t port;
			uint32_t numChunks;
			uint32_t numUnsealedKeys;
		} promote;
		struct {
			uint32_t count;
		} migration;
	} message;

	inline void reqRegister( CoordinatorSocket *socket, uint32_t addr, uint16_t port ) {
		this->type = COORDINATOR_EVENT_TYPE_REGISTER_REQUEST;
		this->socket = socket;
		this->message.address = {
			.addr = addr,
			.port = port
		};
	}

	inline void sync( CoordinatorSocket *socket, uint16_t instanceId, uint32_t requestId ) {
		this->type = COORDINATOR_EVENT_TYPE_SYNC;
		this->set( instanceId, requestId, socket );
	}

	inline void resRemappedData() {
		this->type = COORDINATOR_EVENT_TYPE_RESPONSE_PARITY_MIGRATE;
	}

	inline void resRemappedData( CoordinatorSocket *socket, uint16_t instanceId, uint32_t requestId ) {
		this->type = COORDINATOR_EVENT_TYPE_RESPONSE_PARITY_MIGRATE;
		this->set( instanceId, requestId, socket );
	}

	inline void resReleaseDegradedLock( CoordinatorSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t count ) {
		this->type = COORDINATOR_EVENT_TYPE_RELEASE_DEGRADED_LOCK_RESPONSE_SUCCESS;
		this->set( instanceId, requestId, socket );
		this->message.degraded.count = count;
	}

	inline void resServerReconstructedMsg( CoordinatorSocket *socket, uint16_t instanceId, uint32_t requestId ) {
		this->type = COORDINATOR_EVENT_TYPE_SERVER_RECONSTRUCTED_MESSAGE_RESPONSE;
		this->set( instanceId, requestId, socket );
	}

	inline void resReconstruction(
		CoordinatorSocket *socket, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t chunkId, uint32_t numStripes
	) {
		this->type = COORDINATOR_EVENT_TYPE_RECONSTRUCTION_RESPONSE_SUCCESS;
		this->set( instanceId, requestId, socket );
		this->message.reconstruction = {
			.listId = listId,
			.chunkId = chunkId,
			.numStripes = numStripes
		};
	}

	inline void resReconstructionUnsealed(
		CoordinatorSocket *socket, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t chunkId, uint32_t keysCount
	) {
		this->type = COORDINATOR_EVENT_TYPE_RECONSTRUCTION_UNSEALED_RESPONSE_SUCCESS;
		this->set( instanceId, requestId, socket );
		this->message.reconstructionUnsealed = {
			.listId = listId,
			.chunkId = chunkId,
			.keysCount = keysCount
		};
	}

	inline void resPromoteBackupServer(
		CoordinatorSocket *socket, uint16_t instanceId, uint32_t requestId,
		uint32_t addr, uint16_t port,
		uint32_t numChunks, uint32_t numUnsealedKeys
	) {
		this->type = COORDINATOR_EVENT_TYPE_PROMOTE_BACKUP_SERVER_RESPONSE_SUCCESS;
		this->set( instanceId, requestId, socket );
		this->message.promote = {
			.addr = addr,
			.port = port,
			.numChunks = numChunks,
			.numUnsealedKeys = numUnsealedKeys
		};
	}

	inline void resScalingMigration( CoordinatorSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t count ) {
		this->type = COORDINATOR_EVENT_TYPE_RESPONSE_SCALING_MIGRATION;
		this->set( instanceId, requestId, socket );
		this->message.migration.count = count;
	}

	inline void pending( CoordinatorSocket *socket ) {
		this->type = COORDINATOR_EVENT_TYPE_PENDING;
		this->socket = socket;
	}
};

#endif
