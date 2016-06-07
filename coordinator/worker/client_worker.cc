#include "worker.hh"
#include "../main/coordinator.hh"

void CoordinatorWorker::dispatch( ClientEvent event ) {
	bool connected = false, isSend, success = false;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;

	buffer.data = this->protocol.buffer.send;

	switch( event.type ) {
		case CLIENT_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
		case CLIENT_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
			event.socket->instanceId = event.instanceId;
			buffer.size = this->protocol.generateHeader(
				event.type == CLIENT_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_CLIENT,
				PROTO_OPCODE_REGISTER,
				0, // length
				event.instanceId, event.requestId
			);
			isSend = true;
			break;
		case CLIENT_EVENT_TYPE_PUSH_LOADING_STATS:
			buffer.data = this->protocol.reqPushLoadStats(
				buffer.size,
				Coordinator::instanceId,
				CoordinatorWorker::idGenerator->nextVal( this->workerId ),
				event.message.serverLoading.serverGetLatency,
				event.message.serverLoading.serverSetLatency,
				event.message.serverLoading.overloadedServerSet
			);
			// release the ArrayMaps
			event.message.serverLoading.serverGetLatency->clear();
			event.message.serverLoading.serverSetLatency->clear();
			delete event.message.serverLoading.serverGetLatency;
			delete event.message.serverLoading.serverSetLatency;
			delete event.message.serverLoading.overloadedServerSet;
			isSend = true;
			break;
		case CLIENT_EVENT_TYPE_DEGRADED_SET_LOCK_RESPONSE_SUCCESS:
			success = true;
		case CLIENT_EVENT_TYPE_DEGRADED_SET_LOCK_RESPONSE_FAILURE:
			buffer.size = this->protocol.generateRemappingLockHeader(
				success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_CLIENT,
				PROTO_OPCODE_REMAPPING_LOCK,
				event.instanceId, event.requestId,
				event.message.remap.original,
				event.message.remap.remapped,
				event.message.remap.remappedCount,
				event.message.remap.key.size,
				event.message.remap.key.data
			);
			isSend = true;
			break;
		case CLIENT_EVENT_TYPE_SWITCH_PHASE:
		{
			Coordinator *coordinator = Coordinator::getInstance();
			std::vector<struct sockaddr_in> *servers = event.message.switchPhase.servers;

			isSend = false;
			if ( servers == NULL || ! coordinator->stateTransitHandler )
				break;

			// just trigger the handling of transition, no message need to be handled
			if ( event.message.switchPhase.toRemap ) {
				size_t numClients = coordinator->sockets.clients.size();
				for ( size_t i = 0, numOverloadedServers = servers->size(); i < numOverloadedServers; i++ ) {
					uint16_t instanceId = 0;

					for ( size_t j = 0, numServers = coordinator->sockets.servers.size(); j < numServers; j++ ) {
						struct sockaddr_in addr = servers->at( i );
						if ( coordinator->sockets.servers[ j ]->equal( addr.sin_addr.s_addr, addr.sin_port ) ) {
							instanceId = coordinator->sockets.servers[ j ]->instanceId;
							break;
						}
					}

					if ( instanceId != 0 ) {
						// Update pending set for metadata backup
						if ( ! CoordinatorWorker::pending->addPendingTransition(
							instanceId, // instanceId
							true,       // isDegraded
							numClients  // pending
						) ) {
							__ERROR__( "CoordinatorWorker", "dispatch", "Warning: This server (instance ID = %u) is already under transition to degraded state.", instanceId );
						}
					}

					if ( event.message.switchPhase.isCrashed )
						coordinator->stateTransitHandler->addCrashedServer( servers->at( i ) );
				}
				coordinator->stateTransitHandler->transitToDegraded( servers, event.message.switchPhase.forced );
			} else {
				coordinator->stateTransitHandler->transitToNormal( servers, event.message.switchPhase.forced );
			}
			delete servers;
		}
			break;
		// Degraded operation
		case CLIENT_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_IS_LOCKED:
		case CLIENT_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_WAS_LOCKED:
		{
			bool isLocked = ( event.type == CLIENT_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_IS_LOCKED );

			buffer.size = this->protocol.generateDegradedLockResHeader(
				isLocked ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_CLIENT,
				PROTO_OPCODE_DEGRADED_LOCK,
				event.instanceId, event.requestId,
				isLocked,
				event.message.degradedLock.key.size,
				event.message.degradedLock.key.data,
				event.message.degradedLock.isSealed,
				event.message.degradedLock.stripeId,
				event.message.degradedLock.dataChunkId,
				event.message.degradedLock.dataChunkCount,
				event.message.degradedLock.original,
				event.message.degradedLock.reconstructed,
				event.message.degradedLock.reconstructedCount,
				event.message.degradedLock.ongoingAtChunk,
				event.message.degradedLock.numSurvivingChunkIds,
				event.message.degradedLock.survivingChunkIds
			);
		}
			isSend = true;
			break;
		case CLIENT_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_NOT_LOCKED:
		case CLIENT_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_NOT_FOUND:
			buffer.size = this->protocol.generateDegradedLockResHeader(
				PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_CLIENT,
				PROTO_OPCODE_DEGRADED_LOCK,
				event.instanceId, event.requestId,
				event.type == CLIENT_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_NOT_LOCKED, // exist
				event.message.degradedLock.key.size,
				event.message.degradedLock.key.data
			);
			isSend = true;
			break;
		case CLIENT_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_REMAPPED:
			buffer.size = this->protocol.generateDegradedLockResHeader(
				PROTO_MAGIC_RESPONSE_FAILURE,
				PROTO_MAGIC_TO_CLIENT,
				PROTO_OPCODE_DEGRADED_LOCK,
				event.instanceId, event.requestId,
				event.message.degradedLock.key.size,
				event.message.degradedLock.key.data,
				event.message.degradedLock.original,
				event.message.degradedLock.remapped,
				event.message.degradedLock.remappedCount
			);
			isSend = true;
			break;
		// Recovery
		case CLIENT_EVENT_TYPE_ANNOUNCE_SERVER_RECONSTRUCTED:
			isSend = false;
			break;
		// Pending
		case CLIENT_EVENT_TYPE_PENDING:
			isSend = false;
			break;
		default:
			return;
	}

	if ( isSend ) {
		ret = event.socket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size )
			__ERROR__( "CoordinatorWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
	} else if ( event.type == CLIENT_EVENT_TYPE_SWITCH_PHASE ) {
		connected = true; // just to avoid error message
	} else if ( event.type == CLIENT_EVENT_TYPE_ANNOUNCE_SERVER_RECONSTRUCTED ) {
		ArrayMap<int, ClientSocket> &clients = Coordinator::getInstance()->sockets.clients;
		uint32_t requestId = CoordinatorWorker::idGenerator->nextVal( this->workerId );

		buffer.data = this->protocol.announceServerReconstructed(
			buffer.size, Coordinator::instanceId, requestId,
			event.message.reconstructed.src,
			event.message.reconstructed.dst,
			false // toServer
		);

		LOCK( &clients.lock );
		for ( uint32_t i = 0, size = clients.size(); i < size; i++ ) {
			ClientSocket *client = clients.values[ i ];
			if ( ! client->ready() )
				continue; // Skip failed clients

			ret = client->send( buffer.data, buffer.size, connected );
			if ( ret != ( ssize_t ) buffer.size )
				__ERROR__( "CoordinatorWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
		}
		UNLOCK( &clients.lock );

		connected = true; // just to avoid error message
	} else {
		ProtocolHeader header;
		WORKER_RECEIVE_FROM_EVENT_SOCKET();

		struct LoadStatsHeader loadStatsHeader;
		ArrayMap< struct sockaddr_in, Latency > getLatency, setLatency, *latencyPool = NULL;
		Coordinator *coordinator = Coordinator::getInstance();
		struct sockaddr_in clientAddr;

		while( buffer.size > 0 ) {
			WORKER_RECEIVE_WHOLE_MESSAGE_FROM_EVENT_SOCKET( "CoordinatorWorker" );

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;

			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_CLIENT ) {
				__ERROR__( "CoordinatorWorker", "dispatch", "Invalid message source from client." );
			}

			int index = 0;

			if ( header.magic == PROTO_MAGIC_LOADING_STATS ) {
				this->protocol.parseLoadStatsHeader( loadStatsHeader, buffer.data, buffer.size );
				buffer.data += PROTO_LOAD_STATS_SIZE;
				buffer.size -= PROTO_LOAD_STATS_SIZE;
				if ( ! this->protocol.parseLoadingStats( loadStatsHeader, getLatency, setLatency, buffer.data, buffer.size ) )
					__ERROR__( "CoordinatorWorker", "dispatch", "Invalid amount of data received from client." );

#define SET_SERVER_LATENCY_FOR_CLIENT( _CLIENT_ADDR_, _SRC_, _DST_ ) \
	for ( uint32_t i = 0; i < _SRC_.size(); i++ ) { \
		coordinator->serverLoading._DST_.get( _SRC_.keys[ i ], &index ); \
		if ( index == -1 ) { \
			coordinator->serverLoading._DST_.set( _SRC_.keys[ i ], new ArrayMap<struct sockaddr_in, Latency> () ); \
			index = coordinator->serverLoading._DST_.size() - 1; \
			coordinator->serverLoading._DST_.values[ index ]->set( _CLIENT_ADDR_, _SRC_.values[ i ] ); \
		} else { \
			latencyPool = coordinator->serverLoading._DST_.values[ index ]; \
			latencyPool->get( _CLIENT_ADDR_, &index ); \
			if ( index == -1 ) { \
				latencyPool->set( _CLIENT_ADDR_, _SRC_.values[ i ] ); \
			} else { \
				delete latencyPool->values[ index ]; \
				latencyPool->values[ index ] = _SRC_.values[ i ]; \
			} \
		} \
	} \

				clientAddr = event.socket->getAddr();
				LOCK ( &coordinator->serverLoading.lock );
				SET_SERVER_LATENCY_FOR_CLIENT( clientAddr, getLatency, latestGet );
				SET_SERVER_LATENCY_FOR_CLIENT( clientAddr, setLatency, latestSet );
				UNLOCK ( &coordinator->serverLoading.lock );

				getLatency.needsDelete = false;
				setLatency.needsDelete = false;
				getLatency.clear();
				setLatency.clear();

				buffer.data -= PROTO_LOAD_STATS_SIZE;
				buffer.size += PROTO_LOAD_STATS_SIZE;
			} else if ( header.magic == PROTO_MAGIC_REQUEST ) {
				event.instanceId = header.instanceId;
				event.requestId = header.requestId;
				switch( header.opcode ) {
					case PROTO_OPCODE_REMAPPING_LOCK:
						this->handleDegradedSetLockRequest( event, buffer.data, header.length );
						break;
					case PROTO_OPCODE_DEGRADED_LOCK:
						this->handleDegradedLockRequest( event, buffer.data, header.length );
						break;
					default:
						goto quit_1;
				}
			} else if ( header.magic == PROTO_MAGIC_HEARTBEAT && header.opcode == PROTO_OPCODE_SYNC ) {
				this->handleSyncMetadata( event, buffer.data, header.length );
			} else {
				__ERROR__( "CoordinatorWorker", "dispatch", "Invalid magic code from client." );
				goto quit_1;
			}

#undef SET_SERVER_LATENCY_FOR_CLIENT
quit_1:
			buffer.data += header.length;
			buffer.size -= header.length;
		}

		if ( connected ) event.socket->done();
	}

	if ( ! connected )
		__ERROR__( "CoordinatorWorker", "dispatch", "The client is disconnected." );
}

bool CoordinatorWorker::handleSyncMetadata( ClientEvent event, char *buf, size_t size ) {
	uint32_t count, requestId = event.requestId;
	size_t processed, offset, failed = 0;
	ServerSocket *target = 0;
	struct AddressHeader address;
	struct HeartbeatHeader heartbeat;
	union {
		struct MetadataHeader metadata;
		struct KeyOpMetadataHeader op;
	} header;

	offset = 0;
	if ( ! this->protocol.parseMetadataBackupMessage( address, heartbeat, buf, size ) ) {
		__ERROR__( "CoordinatorWorker", "dispatch", "Invalid heartbeat protocol header." );
		return false;
	}

	offset += PROTO_ADDRESS_SIZE + PROTO_HEARTBEAT_SIZE;

	char ipBuf[ 16 ];
	Socket::ntoh_ip( address.addr, ipBuf, sizeof( ipBuf ) );
	__DEBUG__(
		YELLOW, "CoordinatorWorker", "handleSyncMetadata",
		"Server: %s:%u (sealed: %u; ops: %u).\n",
		ipBuf, Socket::hton_port( address.port ),
		heartbeat.sealed, heartbeat.keys
	);

	ArrayMap<int, ServerSocket> &servers = Coordinator::getInstance()->sockets.servers;
	LOCK( &servers.lock );
	for ( uint32_t i = 0; i < servers.size(); i++ ) {
		if ( servers.values[ i ]->equal( address.addr, address.port ) ) {
			target = servers.values[ i ];
			break;
		}
	}
	UNLOCK( &servers.lock );
	if ( ! target ) {
		__ERROR__( "CoordinatorWorker", "handleSyncMetadata", "Server not found." );
		return false;
	}

	LOCK( &target->map.chunksLock );
	for ( count = 0; count < heartbeat.sealed; count++ ) {
		if ( this->protocol.parseMetadataHeader( header.metadata, processed, buf, size, offset ) ) {
			target->map.insertChunk(
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
	UNLOCK( &target->map.chunksLock );

	LOCK( &target->map.keysLock );
	for ( count = 0; count < heartbeat.keys; count++ ) {
		if ( this->protocol.parseKeyOpMetadataHeader( header.op, processed, buf, size, offset ) ) {
			ServerSocket *s = target;
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
	UNLOCK( &target->map.keysLock );

	if ( failed ) {
		__ERROR__( "CoordinatorWorker", "handleSyncMetadata", "Number of failed objects = %lu", failed );
	}

	// check if this is the last packet for a sync operation
	// remove pending meta sync requests
	if ( requestId && heartbeat.isLast && ! failed ) {
		PendingTransition *pendingTransition = CoordinatorWorker::pending->findPendingTransition( target->instanceId, true );

		if ( pendingTransition ) {
			pthread_mutex_lock( &pendingTransition->lock );
			pendingTransition->pending--;
			if ( pendingTransition->pending == 0 )
				pthread_cond_signal( &pendingTransition->cond );
			pthread_mutex_unlock( &pendingTransition->lock );
		} else {
			__ERROR__( "CoordinatorWorker", "handleSyncMetadata", "Pending transition not found (instance ID: %u).", target->instanceId );
		}
	}

	return failed == 0;
}
