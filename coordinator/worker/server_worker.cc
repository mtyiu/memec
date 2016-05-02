#include "worker.hh"
#include "../main/coordinator.hh"

void CoordinatorWorker::dispatch( ServerEvent event ) {
	bool connected, isSend;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;
	uint16_t instanceId = Coordinator::instanceId;
	uint32_t requestId;

	switch( event.type ) {
		case SERVER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
			event.socket->instanceId = event.instanceId;
			buffer.data = this->protocol.resRegisterServer( buffer.size, event.instanceId, event.requestId, true );
			isSend = true;
			break;
		case SERVER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
			event.socket->instanceId = event.instanceId;
			buffer.data = this->protocol.resRegisterServer( buffer.size, event.instanceId, event.requestId, false );
			isSend = true;
			break;
		case SERVER_EVENT_TYPE_REQUEST_SEAL_CHUNKS:
			requestId = CoordinatorWorker::idGenerator->nextVal( this->workerId );
			buffer.data = this->protocol.reqSealChunks( buffer.size, instanceId, requestId );
			isSend = true;
			break;
		case SERVER_EVENT_TYPE_REQUEST_FLUSH_CHUNKS:
			requestId = CoordinatorWorker::idGenerator->nextVal( this->workerId );
			buffer.data = this->protocol.reqFlushChunks( buffer.size, instanceId, requestId );
			isSend = true;
			break;
		case SERVER_EVENT_TYPE_REQUEST_SYNC_META:
			requestId = CoordinatorWorker::idGenerator->nextVal( this->workerId );
			buffer.data = this->protocol.reqSyncMeta( buffer.size, instanceId, requestId );
			// add sync meta request to pending set
			Coordinator::getInstance()->pending.addSyncMetaReq( requestId, event.message.sync );
			isSend = true;
			break;
		// reampped parity migration
		case SERVER_EVENT_TYPE_PARITY_MIGRATE:
			buffer.data = event.message.parity.packet->data;
			buffer.size = event.message.parity.packet->size;
			isSend = true;
			break;
		case SERVER_EVENT_TYPE_REQUEST_RELEASE_DEGRADED_LOCK:
			this->handleReleaseDegradedLockRequest(
				event.socket,
				event.message.degraded.lock,
				event.message.degraded.cond,
				event.message.degraded.done
			);
			isSend = false;
			break;
		case SERVER_EVENT_TYPE_PENDING:
			isSend = false;
			break;
		case SERVER_EVENT_TYPE_ANNOUNCE_SERVER_CONNECTED:
		case SERVER_EVENT_TYPE_ANNOUNCE_SERVER_RECONSTRUCTED:
			isSend = false;
			break;
		case SERVER_EVENT_TYPE_RESPONSE_HEARTBEAT:
			buffer.data = this->protocol.resHeartbeat(
				buffer.size, event.instanceId, event.requestId,
				event.message.heartbeat.timestamp,
				event.message.heartbeat.sealed,
				event.message.heartbeat.keys,
				event.message.heartbeat.isLast
			);
			isSend = true;
			break;
		case SERVER_EVENT_TYPE_DISCONNECT:
		case SERVER_EVENT_TYPE_TRIGGER_RECONSTRUCTION:
		case SERVER_EVENT_TYPE_HANDLE_RECONSTRUCTION_REQUEST:
			isSend = false;
			break;
		case SERVER_EVENT_TYPE_ACK_RECONSTRUCTION_SUCCESS:
		case SERVER_EVENT_TYPE_ACK_RECONSTRUCTION_FAILURE:
			buffer.data = this->protocol.ackCompletedReconstruction(
				buffer.size, event.instanceId, event.requestId,
				event.type == SERVER_EVENT_TYPE_ACK_RECONSTRUCTION_SUCCESS
			);
			isSend = true;
			break;
		default:
			return;
	}

	if ( event.type == SERVER_EVENT_TYPE_ANNOUNCE_SERVER_CONNECTED ) {
		ArrayMap<int, ServerSocket> &servers = Coordinator::getInstance()->sockets.servers;
		uint32_t requestId = CoordinatorWorker::idGenerator->nextVal( this->workerId );

		buffer.data = this->protocol.announceServerConnected( buffer.size, instanceId, requestId, event.socket );

		LOCK( &servers.lock );
		for ( uint32_t i = 0; i < servers.size(); i++ ) {
			ServerSocket *server = servers.values[ i ];
			if ( event.socket->equal( server ) || ! server->ready() )
				continue; // No need to tell the new socket

			ret = server->send( buffer.data, buffer.size, connected );
			if ( ret != ( ssize_t ) buffer.size )
				__ERROR__( "CoordinatorWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
		}
		// notify the remap message handler of the new server
		struct sockaddr_in serverAddr = event.socket->getAddr();
		if ( Coordinator::getInstance()->stateTransitHandler )
			Coordinator::getInstance()->stateTransitHandler->addAliveServer( serverAddr );
		UNLOCK( &servers.lock );
	} else if ( event.type == SERVER_EVENT_TYPE_ANNOUNCE_SERVER_RECONSTRUCTED ) {
		ArrayMap<int, ServerSocket> &servers = Coordinator::getInstance()->sockets.servers;
		ArrayMap<int, ServerSocket> &backupServers = Coordinator::getInstance()->sockets.backupServers;

		buffer.data = this->protocol.announceServerReconstructed(
			buffer.size, event.instanceId, event.requestId,
			event.message.reconstructed.src,
			event.message.reconstructed.dst,
			true // toServer
		);

		LOCK( &servers.lock );
		// Count the number of surviving servers
		for ( uint32_t i = 0, serversSize = servers.size(), size = servers.size() + backupServers.size(); i < size; i++ ) {
			ServerSocket *server = i < serversSize ? servers.values[ i ] : backupServers.values[ i - serversSize ];
			if ( server->equal( event.message.reconstructed.dst ) || ! server->ready() )
				continue; // No need to tell the backup server
			event.message.reconstructed.sockets->insert( server );
		}
		// Insert into pending set
		CoordinatorWorker::pending->insertAnnouncement(
			event.instanceId, event.requestId,
			event.message.reconstructed.lock,
			event.message.reconstructed.cond,
			event.message.reconstructed.sockets
		);

		// Send requests
		for ( uint32_t i = 0, serversSize = servers.size(), size = servers.size() + backupServers.size(); i < size; i++ ) {
			ServerSocket *server = i < serversSize ? servers.values[ i ] : backupServers.values[ i - serversSize ];
			if ( server->equal( event.message.reconstructed.dst ) || ! server->ready() )
				continue; // No need to tell the backup server

			ret = server->send( buffer.data, buffer.size, connected );
			if ( ret != ( ssize_t ) buffer.size ) {
				__ERROR__( "CoordinatorWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
				CoordinatorWorker::pending->eraseAnnouncement( event.instanceId, event.requestId, server );
			}
		}
		// notify the remap message handler of the new server
		struct sockaddr_in serverAddr = event.message.reconstructed.dst->getAddr();
		if ( Coordinator::getInstance()->stateTransitHandler )
			Coordinator::getInstance()->stateTransitHandler->addAliveServer( serverAddr );
		UNLOCK( &servers.lock );
	} else if ( event.type == SERVER_EVENT_TYPE_DISCONNECT ) {
		// Remove the server from the pending set of annoucement
		CoordinatorWorker::pending->eraseAnnouncement( event.socket );

		// Mark it as failed
		if ( ! Coordinator::getInstance()->config.global.states.disabled ) {
			Coordinator::getInstance()->switchPhaseForCrashedServer( event.socket );
		} else {
			ServerEvent serverEvent;
			serverEvent.triggerReconstruction( event.socket->getAddr() );
			this->dispatch( serverEvent );
		}
	} else if ( event.type == SERVER_EVENT_TYPE_TRIGGER_RECONSTRUCTION ) {
		ServerSocket *s = 0;
		ArrayMap<int, ServerSocket> &servers = Coordinator::getInstance()->sockets.servers;

		LOCK( &servers.lock );
		for ( uint32_t i = 0, size = servers.size(); i < size; i++ ) {
			if ( servers.values[ i ]->equal( event.message.addr.sin_addr.s_addr, event.message.addr.sin_port ) ) {
				s = servers.values[ i ];
				break;
			}
		}
		UNLOCK( &servers.lock );

		if ( s ) {
			this->handleReconstructionRequest( s );
		} else {
			__ERROR__( "CoordinatorWorker", "dispatch", "Unknown crashed server." );
		}
	} else if ( event.type == SERVER_EVENT_TYPE_HANDLE_RECONSTRUCTION_REQUEST ) {
		this->handleReconstructionRequest( event.socket );
	} else if ( isSend ) {
		ret = event.socket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size )
			__ERROR__( "CoordinatorWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
		if ( ! connected )
			__ERROR__( "CoordinatorWorker", "dispatch", "The server is disconnected." );
		if ( event.type == SERVER_EVENT_TYPE_PARITY_MIGRATE )
			Coordinator::getInstance()->packetPool.free( event.message.parity.packet );
	} else {
		// Parse requests from servers
		ProtocolHeader header;
		WORKER_RECEIVE_FROM_EVENT_SOCKET();

		while( buffer.size > 0 ) {
			WORKER_RECEIVE_WHOLE_MESSAGE_FROM_EVENT_SOCKET( "CoordinatorWorker" );

			// avvoid declaring variables after jump statements
			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_SERVER ) {
				__ERROR__( "CoordinatorWorker", "dispatch", "Invalid message source from server." );
				goto quit_1;
			}

			event.instanceId = header.instanceId;
			event.requestId = header.requestId;
			switch( header.opcode ) {
				case PROTO_OPCODE_RELEASE_DEGRADED_LOCKS:
					switch( header.magic ) {
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleReleaseDegradedLockResponse( event, buffer.data, header.length );
							break;
						default:
							__ERROR__( "CoordinatorWorker", "dispatch", "Invalid magic code from server." );
					}
					break;
				case PROTO_OPCODE_SERVER_RECONSTRUCTED:
					switch( header.magic ) {
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							// printf( "Received response from " );
							// event.socket->print( stdout );
							CoordinatorWorker::pending->eraseAnnouncement( header.instanceId, header.requestId, event.socket );
							break;
						default:
							__ERROR__( "CoordinatorWorker", "dispatch", "Invalid magic code from server." );
					}
					break;
				case PROTO_OPCODE_RECONSTRUCTION:
					switch( header.magic ) {
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleReconstructionResponse( event, buffer.data, header.length );
							break;
						default:
							__ERROR__( "CoordinatorWorker", "dispatch", "Invalid magic code from server." );
					}
					break;
				case PROTO_OPCODE_RECONSTRUCTION_UNSEALED:
					switch( header.magic ) {
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handleReconstructionUnsealedResponse( event, buffer.data, header.length );
							break;
						default:
							__ERROR__( "CoordinatorWorker", "dispatch", "Invalid magic code from server." );
					}
					break;
				case PROTO_OPCODE_BACKUP_SERVER_PROMOTED:
					switch( header.magic ) {
						case PROTO_MAGIC_RESPONSE_SUCCESS:
							this->handlePromoteBackupServerResponse( event, buffer.data, header.length );
							break;
						default:
							__ERROR__( "CoordinatorWorker", "dispatch", "Invalid magic code from server." );
					}
					break;
				case PROTO_OPCODE_SYNC:
					switch( header.magic ) {
						case PROTO_MAGIC_HEARTBEAT:
							this->processHeartbeat( event, buffer.data, header.length );
							break;
						default:
							__ERROR__( "CoordinatorWorker", "dispatch", "Invalid magic code from server." );
							break;
					}
					break;
				case PROTO_OPCODE_PARITY_MIGRATE:
				{
					pthread_mutex_t *lock = 0;
					pthread_cond_t *cond = 0;
					bool *done = 0, isCompleted;

					bool found = this->pending->decrementRemappedDataRequest(
						event.requestId, event.socket->getAddr(),
						lock, cond, done, isCompleted
					);

					if ( found ) {
						if ( isCompleted ) {
							if ( lock ) pthread_mutex_lock( lock );
							if ( done ) *done = true;
							if ( cond ) pthread_cond_signal( cond );
							if ( lock ) pthread_mutex_unlock( lock );
						}
					} else {
						__ERROR__( "CoordinatorWorker", "dispatch", "Invalid id reply to parity migrate request." );
					}
				}
					break;
				default:
					__ERROR__( "CoordinatorWorker", "dispatch", "Invalid opcode from server." );
					goto quit_1;
			}
quit_1:
			buffer.data += header.length;
			buffer.size -= header.length;
		}
		if ( connected )
			event.socket->done();
		else
			__ERROR__( "CoordinatorWorker", "dispatch", "The server is disconnected." );
	}
}

bool CoordinatorWorker::processHeartbeat( ServerEvent event, char *buf, size_t size ) {
	uint32_t count, requestId = event.requestId;
	size_t processed, offset, failed = 0;
	struct HeartbeatHeader heartbeat;
	union {
		struct MetadataHeader metadata;
		struct KeyOpMetadataHeader op;
	} header;

	offset = 0;
	if ( ! this->protocol.parseHeartbeatHeader( heartbeat, buf, size ) ) {
		__ERROR__( "CoordinatorWorker", "dispatch", "Invalid heartbeat protocol header." );
		return false;
	}

	offset += PROTO_HEARTBEAT_SIZE;

	LOCK( &event.socket->map.chunksLock );
	for ( count = 0; count < heartbeat.sealed; count++ ) {
		if ( this->protocol.parseMetadataHeader( header.metadata, processed, buf, size, offset ) ) {
			event.socket->map.insertChunk(
				header.metadata.listId,
				header.metadata.stripeId,
				header.metadata.chunkId,
				false, false
			);
		} else {
			failed++;
		}
		offset += processed;
	}
	UNLOCK( &event.socket->map.chunksLock );

	LOCK( &event.socket->map.keysLock );
	for ( count = 0; count < heartbeat.keys; count++ ) {
		if ( this->protocol.parseKeyOpMetadataHeader( header.op, processed, buf, size, offset ) ) {
			ServerSocket *s = event.socket;
			if ( header.op.opcode == PROTO_OPCODE_DELETE ) { // Handle keys from degraded DELETE
				s = CoordinatorWorker::stripeList->get( header.op.listId, header.op.chunkId );
			}
			s->map.insertKey(
				header.op.key,
				header.op.keySize,
				header.op.listId,
				header.op.stripeId,
				header.op.chunkId,
				header.op.opcode,
				header.op.timestamp,
				false, false
			);
		} else {
			failed++;
		}
		offset += processed;
	}
	UNLOCK( &event.socket->map.keysLock );

	if ( failed ) {
		__ERROR__( "CoordinatorWorker", "processHeartbeat", "Number of failed objects = %lu", failed );
	} else {
		// Send ACK message
		event.resHeartbeat( event.socket, heartbeat.timestamp, heartbeat.sealed, heartbeat.keys, heartbeat.isLast );
		this->dispatch( event );
	}

	// check if this is the last packet for a sync operation
	// remove pending meta sync requests
	if ( requestId && heartbeat.isLast && ! failed ) {
		bool *sync = Coordinator::getInstance()->pending.removeSyncMetaReq( requestId );
		if ( sync )
			*sync = true;
	}

	return failed == 0;
}
