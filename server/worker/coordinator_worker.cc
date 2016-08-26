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

	buffer.data = this->protocol.buffer.send;

	switch( event.type ) {
		case COORDINATOR_EVENT_TYPE_REGISTER_REQUEST:
			requestId = ServerWorker::idGenerator->nextVal( this->workerId );
			buffer.size = this->protocol.generateAddressHeader(
				PROTO_MAGIC_REQUEST, PROTO_MAGIC_TO_COORDINATOR,
				PROTO_OPCODE_REGISTER,
				PROTO_UNINITIALIZED_INSTANCE, requestId,
				event.message.address.addr,
				event.message.address.port
			);
			isSend = true;
			break;
		case COORDINATOR_EVENT_TYPE_SYNC:
		{
			uint32_t sealedCount, opsCount;
			bool isCompleted;

			uint32_t timestamp = Server::getInstance()->timestamp.nextVal();

			buffer.size = this->protocol.generateHeartbeatMessage(
				PROTO_MAGIC_HEARTBEAT, PROTO_MAGIC_TO_COORDINATOR,
				PROTO_OPCODE_SYNC,
				event.instanceId, event.requestId, timestamp,
				&ServerWorker::map->sealedLock, ServerWorker::map->sealed, sealedCount,
				&ServerWorker::map->opsLock, ServerWorker::map->ops, opsCount,
				isCompleted
			);

			if ( sealedCount || opsCount )
				Server::getInstance()->pendingAck.insert( timestamp );

			if ( ! isCompleted )
				ServerWorker::eventQueue->insert( event );

			isSend = true;
		}
			break;
		case COORDINATOR_EVENT_TYPE_RELEASE_DEGRADED_LOCK_RESPONSE_SUCCESS:
			buffer.size = this->protocol.generateDegradedReleaseResHeader(
				PROTO_MAGIC_RESPONSE_SUCCESS, PROTO_MAGIC_TO_COORDINATOR,
				PROTO_OPCODE_RELEASE_DEGRADED_LOCKS,
				event.instanceId, event.requestId,
				event.message.degraded.count
			);
			isSend = true;
			break;
		case COORDINATOR_EVENT_TYPE_SERVER_RECONSTRUCTED_MESSAGE_RESPONSE:
			buffer.size = this->protocol.generateHeader(
				PROTO_MAGIC_RESPONSE_SUCCESS, PROTO_MAGIC_TO_COORDINATOR,
				PROTO_OPCODE_SERVER_RECONSTRUCTED,
				0, // length
				event.instanceId, event.requestId
			);
			isSend = true;
			break;
		case COORDINATOR_EVENT_TYPE_RECONSTRUCTION_RESPONSE_SUCCESS:
			buffer.size = this->protocol.generateReconstructionHeader(
				PROTO_MAGIC_RESPONSE_SUCCESS, PROTO_MAGIC_TO_COORDINATOR,
				PROTO_OPCODE_RECONSTRUCTION,
				event.instanceId, event.requestId,
				event.message.reconstruction.listId,
				event.message.reconstruction.chunkId,
				event.message.reconstruction.numStripes
			);
			isSend = true;
			break;
		case COORDINATOR_EVENT_TYPE_RECONSTRUCTION_UNSEALED_RESPONSE_SUCCESS:
			buffer.size = this->protocol.generateReconstructionHeader(
				PROTO_MAGIC_RESPONSE_SUCCESS,
				PROTO_MAGIC_TO_COORDINATOR,
				PROTO_OPCODE_RECONSTRUCTION_UNSEALED,
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

			buffer.size = this->protocol.generatePromoteBackupServerHeader(
				PROTO_MAGIC_RESPONSE_SUCCESS,
				PROTO_MAGIC_TO_COORDINATOR,
				PROTO_OPCODE_BACKUP_SERVER_PROMOTED,
				event.instanceId, event.requestId,
				event.message.promote.addr,
				event.message.promote.port,
				event.message.promote.numChunks,
				event.message.promote.numUnsealedKeys
			);
			isSend = true;
			break;
		case COORDINATOR_EVENT_TYPE_RESPONSE_PARITY_MIGRATE:
			buffer.size = this->protocol.generateHeader(
				PROTO_MAGIC_RESPONSE_SUCCESS,
				PROTO_MAGIC_TO_COORDINATOR,
				PROTO_OPCODE_PARITY_MIGRATE,
				0,
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
					case PROTO_OPCODE_ADD_NEW_SERVER:
						this->handleAddNewServerRequest( event, buffer.data, header.length );
						break;
					case PROTO_OPCODE_STRIPE_LIST_UPDATE:
						this->handleStripeListUpdateRequest( event, buffer.data, header.length );
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
	if ( Server::getInstance()->pendingAck.erase( header.timestamp, fromTimestamp ) ) {
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

bool ServerWorker::handleAddNewServerRequest( CoordinatorEvent event, char *buf, size_t size ) {
	struct NewServerHeader header;
	char ipStr[ 16 ], portStr[ 6 ];
	if ( ! this->protocol.parseNewServerHeader( header, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleAddNewServerRequest", "Invalid new server header." );
		return false;
	}

	Socket::ntoh_ip( header.addr, ipStr, sizeof( ipStr ) );
	Socket::ntoh_port( header.port, portStr, sizeof( portStr ) );
	__DEBUG__(
		BLUE, "ServerWorker", "handleAddNewServerRequest",
		"Server name: %.*s (length = %u); address: %s; port: %s.",
		header.length, header.name, header.length,
		ipStr, portStr
	);

	char backup = header.name[ header.length ];
	header.name[ header.length ] = 0;
	ServerAddr addr( header.name, header.addr, header.port );
	header.name[ header.length ] = backup;

	// Update global config
	Server *server = Server::getInstance();
	size_t index = server->config.global.servers.size();
	server->config.global.servers.push_back( addr );

	// Add new server peer socket
	int myServerIndex = server->getMyServerIndex();
	ServerPeerSocket *socket = new ServerPeerSocket();
	int tmpfd = - ( server->config.global.servers.size() );
	socket->init(
		tmpfd, addr, &server->sockets.epoll,
		( int ) index == myServerIndex && myServerIndex != -1
	);
	server->sockets.serverPeers.set( tmpfd, socket );

	server->stripeList->addNewServer( socket );

	server->stripeList->print( stderr, false );
	server->stripeList->print( stderr, true );

	return true;
}

bool ServerWorker::handleStripeListUpdateRequest( CoordinatorEvent event, char *buf, size_t size ) {
	struct StripeListScalingHeader header;
	struct StripeListPartitionHeader list;
	size_t next = 0;
	if ( ! this->protocol.parseStripeListScalingHeader( header, next, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleStripeListUpdateRequest", "Invalid new server header." );
		return false;
	}
	__INFO__(
		BLUE, "ServerWorker", "handleStripeListUpdateRequest",
		"Is migrating? %s; number of servers = %u, number of lists = %u, n = %u, k = %u.",
		header.isMigrating ? "yes" : "no",
		header.numServers, header.numLists,
		header.n, header.k
	);

	for ( uint32_t i = 0; i < header.numLists; i++ ) {
		this->protocol.parseStripeListPartitionHeader(
			list, next,
			header.n, header.k,
			buf, size, next
		);

		__INFO__(
			BLUE, "ServerWorker", "handleStripeListUpdateRequest",
			"List #%3u [%10u-%10u]:",
			list.listId, list.partitionFrom, list.partitionTo
		);
		for ( uint32_t j = 0; j < header.n; j++ ) {
			fprintf( stderr, "%u ", j < header.k ? list.data[ j ] : list.parity[ j - header.k ] );
		}
		fprintf( stderr, "\n" );
	}

	return true;
}
