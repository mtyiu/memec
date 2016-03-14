#include "worker.hh"
#include "../main/server.hh"

void ServerWorker::dispatch( CoordinatorEvent event ) {
	bool connected, isSend;
	uint32_t requestId;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;

	requestId = ServerWorker::idGenerator->nextVal( this->workerId );

	switch( event.type ) {
		case COORDINATOR_EVENT_TYPE_REGISTER_REQUEST:
			requestId = ServerWorker::idGenerator->nextVal( this->workerId );
			buffer.data = this->protocol.reqRegisterCoordinator(
				buffer.size,
				requestId,
				event.message.address.addr,
				event.message.address.port
			);
			isSend = true;
			break;
		case COORDINATOR_EVENT_TYPE_SYNC:
		{
			uint32_t sealedCount, opsCount;
			bool isCompleted;

			uint32_t timestamp = ServerWorker::timestamp->nextVal();

			buffer.data = this->protocol.sendHeartbeat(
				buffer.size,
				event.instanceId, event.requestId,
				timestamp,
				&ServerWorker::map->sealedLock, ServerWorker::map->sealed, sealedCount,
				&ServerWorker::map->opsLock, ServerWorker::map->ops, opsCount,
				isCompleted
			);

			if ( sealedCount || opsCount )
				ServerWorker::pendingAck->insert( timestamp );

			if ( ! isCompleted )
				ServerWorker::eventQueue->insert( event );

			isSend = true;
		}
			break;
		case COORDINATOR_EVENT_TYPE_RELEASE_DEGRADED_LOCK_RESPONSE_SUCCESS:
			buffer.data = this->protocol.resReleaseDegradedLock(
				buffer.size,
				event.instanceId, event.requestId,
				event.message.degraded.count
			);
			isSend = true;
			break;
		case COORDINATOR_EVENT_TYPE_SERVER_RECONSTRUCTED_MESSAGE_RESPONSE:
			buffer.data = this->protocol.resServerReconstructedMsg(
				buffer.size,
				event.instanceId, event.requestId
			);
			isSend = true;
			break;
		case COORDINATOR_EVENT_TYPE_RECONSTRUCTION_RESPONSE_SUCCESS:
			buffer.data = this->protocol.resReconstruction(
				buffer.size,
				event.instanceId, event.requestId,
				event.message.reconstruction.listId,
				event.message.reconstruction.chunkId,
				event.message.reconstruction.numStripes
			);
			isSend = true;
			break;
		case COORDINATOR_EVENT_TYPE_RECONSTRUCTION_UNSEALED_RESPONSE_SUCCESS:
			buffer.data = this->protocol.resReconstructionUnsealed(
				buffer.size,
				event.instanceId, event.requestId,
				event.message.reconstructionUnsealed.listId,
				event.message.reconstructionUnsealed.chunkId,
				event.message.reconstructionUnsealed.keysCount
			);
			isSend = true;
			break;
		case COORDINATOR_EVENT_TYPE_PROMOTE_BACKUP_SERVER_RESPONSE_SUCCESS:
		{
			Server *server = Server::getInstance();
			LOCK( &server->status.lock );
			server->status.isRecovering = false;
			UNLOCK( &server->status.lock );

			// Check if there are any held server peer registration requests
			struct {
				uint32_t requestId;
				ServerPeerSocket *socket;
				bool success;
			} registration;
			while ( ServerWorker::pending->eraseServerPeerRegistration( registration.requestId, registration.socket, registration.success ) ) {
				ServerPeerEvent serverPeerEvent;
				serverPeerEvent.resRegister(
					registration.socket,
					Server::instanceId,
					registration.requestId,
					registration.success
				);
				ServerWorker::eventQueue->insert( serverPeerEvent );
			}
		}
			buffer.data = this->protocol.resPromoteBackupServer(
				buffer.size,
				event.instanceId, event.requestId,
				event.message.promote.addr,
				event.message.promote.port,
				event.message.promote.numChunks,
				event.message.promote.numUnsealedKeys
			);
			isSend = true;
			break;
		case COORDINATOR_EVENT_TYPE_RESPONSE_PARITY_MIGRATE:
			buffer.data = this->protocol.resRemapParity(
				buffer.size,
				event.instanceId, event.requestId
			);
			isSend = true;
			break;
		case COORDINATOR_EVENT_TYPE_PENDING:
			isSend = false;
			break;
		default:
			return;
	}

	if ( isSend ) {
		ret = event.socket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size )
			__ERROR__( "ServerWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
	} else {
		ProtocolHeader header;
		WORKER_RECEIVE_FROM_EVENT_SOCKET();
		while ( buffer.size > 0 ) {
			WORKER_RECEIVE_WHOLE_MESSAGE_FROM_EVENT_SOCKET( "ServerWorker" );

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_COORDINATOR ) {
				__ERROR__( "ServerWorker", "dispatch", "Invalid message source from coordinator." );
			} else {
				event.instanceId = header.instanceId;
				event.requestId = header.requestId;
				switch( header.opcode ) {
					case PROTO_OPCODE_REGISTER:
						switch( header.magic ) {
							case PROTO_MAGIC_RESPONSE_SUCCESS:
								event.socket->registered = true;
								Server::instanceId = header.instanceId;
								break;
							case PROTO_MAGIC_RESPONSE_FAILURE:
								__ERROR__( "ServerWorker", "dispatch", "Failed to register with coordinator." );
								break;
							default:
								__ERROR__( "ServerWorker", "dispatch", "Invalid magic code from coordinator." );
								break;
						}
						break;
					case PROTO_OPCODE_SERVER_CONNECTED:
						this->handleServerConnectedMsg( event, buffer.data, header.length );
						break;
					case PROTO_OPCODE_SERVER_RECONSTRUCTED:
						this->handleServerReconstructedMsg( event, buffer.data, header.length );
						break;
					case PROTO_OPCODE_BACKUP_SERVER_PROMOTED:
						this->handleBackupServerPromotedMsg( event, buffer.data, header.length );
						break;
					case PROTO_OPCODE_SEAL_CHUNKS:
						Server::getInstance()->seal();
						break;
					case PROTO_OPCODE_FLUSH_CHUNKS:
						Server::getInstance()->flush();
						break;
					case PROTO_OPCODE_SYNC_META:
						Server::getInstance()->sync( header.requestId );
						break;
					case PROTO_OPCODE_RELEASE_DEGRADED_LOCKS:
						this->handleReleaseDegradedLockRequest( event, buffer.data, header.length );
						break;
					case PROTO_OPCODE_RECONSTRUCTION:
						switch( header.magic ) {
							case PROTO_MAGIC_REQUEST:
								this->handleReconstructionRequest( event, buffer.data, header.length );
								break;
							case PROTO_MAGIC_RESPONSE_SUCCESS:
								this->handleCompletedReconstructionAck();
								break;
							case PROTO_MAGIC_RESPONSE_FAILURE:
							default:
								__ERROR__( "ServerWorker", "dispatch", "Invalid magic code from coordinator." );
								break;
						}
						break;
					case PROTO_OPCODE_RECONSTRUCTION_UNSEALED:
						switch( header.magic ) {
							case PROTO_MAGIC_REQUEST:
								this->handleReconstructionUnsealedRequest( event, buffer.data, header.length );
								break;
							case PROTO_MAGIC_RESPONSE_SUCCESS:
							case PROTO_MAGIC_RESPONSE_FAILURE:
							default:
								__ERROR__( "ServerWorker", "dispatch", "Invalid magic code from coordinator." );
								break;
						}
						break;
					case PROTO_OPCODE_PARITY_MIGRATE:
						this->handleRemappedData( event, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_SYNC:
						this->handleHeartbeatAck( event, buffer.data, header.length );
						break;
					default:
						__ERROR__( "ServerWorker", "dispatch", "Invalid opcode from coordinator." );
						break;
				}
			}

			buffer.data += header.length;
			buffer.size -= header.length;
		}
		if ( connected ) event.socket->done();
	}
	if ( ! connected )
		__ERROR__( "ServerWorker", "dispatch", "The coordinator is disconnected." );
}

bool ServerWorker::handleServerConnectedMsg( CoordinatorEvent event, char *buf, size_t size ) {
	struct AddressHeader header;
	if ( ! this->protocol.parseAddressHeader( header, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleServerConnectedMsg", "Invalid address header." );
		return false;
	}

	char tmp[ 22 ];
	Socket::ntoh_ip( header.addr, tmp, 16 );
	Socket::ntoh_port( header.port, tmp + 16, 6 );
	__DEBUG__( YELLOW, "ServerWorker", "handleServerConnectedMsg", "Server: %s:%s is connected.", tmp, tmp + 16 );

	// Find the server peer socket in the array map
	int index = -1;
	for ( int i = 0, len = serverPeers->size(); i < len; i++ ) {
		if ( serverPeers->values[ i ]->equal( header.addr, header.port ) ) {
			index = i;
			break;
		}
	}
	if ( index == -1 ) {
		__ERROR__( "ServerWorker", "handleServerConnectedMsg", "The server is not in the list. Ignoring this server..." );
		return false;
	}

	// Update sockfd in the array Map
	int sockfd = serverPeers->values[ index ]->init();
	serverPeers->keys[ index ] = sockfd;

	// Connect to the server peer
	serverPeers->values[ index ]->start();

	return true;
}

bool ServerWorker::handleHeartbeatAck( CoordinatorEvent event, char *buf, size_t size ) {
	struct HeartbeatHeader header;
	if ( ! this->protocol.parseHeartbeatHeader( header, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleHeartbeatAck", "Invalid acknowledgement header." );
		return false;
	}

	__DEBUG__( YELLOW, "ServerWorker", "handleAcknowledgement", "Timestamp: %u.", header.timestamp );

	uint32_t fromTimestamp;
	if ( ServerWorker::pendingAck->erase( header.timestamp, fromTimestamp ) ) {
		// Send ACK to clients
		ClientEvent clientEvent;
		uint16_t instanceId = Server::instanceId;
		uint32_t requestId = ServerWorker::idGenerator->nextVal( this->workerId );

		Server *server = Server::getInstance();
		LOCK_T *lock = &server->sockets.clients.lock;
		std::vector<ClientSocket *> &sockets = server->sockets.clients.values;

		LOCK( lock );
		for ( size_t i = 0, size = sockets.size(); i < size; i++ ) {
			clientEvent.ackMetadata( sockets[ i ], instanceId, requestId, fromTimestamp, header.timestamp );
			ServerWorker::eventQueue->insert( clientEvent );
		}
		UNLOCK( lock );
	}
	return true;
}
