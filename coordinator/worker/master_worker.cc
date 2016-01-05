#include "worker.hh"
#include "../main/coordinator.hh"

void CoordinatorWorker::dispatch( MasterEvent event ) {
	bool connected = false, isSend, success = false;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;

	switch( event.type ) {
		case MASTER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
			event.socket->instanceId = event.instanceId;
			buffer.data = this->protocol.resRegisterMaster( buffer.size, event.instanceId, event.requestId, true );
			isSend = true;
			break;
		case MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
			event.socket->instanceId = event.instanceId;
			buffer.data = this->protocol.resRegisterMaster( buffer.size, event.instanceId, event.requestId, false );
			isSend = true;
			break;
		case MASTER_EVENT_TYPE_PUSH_LOADING_STATS:
			buffer.data = this->protocol.reqPushLoadStats(
				buffer.size,
				Coordinator::instanceId,
				CoordinatorWorker::idGenerator->nextVal( this->workerId ),
				event.message.slaveLoading.slaveGetLatency,
				event.message.slaveLoading.slaveSetLatency,
				event.message.slaveLoading.overloadedSlaveSet
			);
			// release the ArrayMaps
			event.message.slaveLoading.slaveGetLatency->clear();
			event.message.slaveLoading.slaveSetLatency->clear();
			delete event.message.slaveLoading.slaveGetLatency;
			delete event.message.slaveLoading.slaveSetLatency;
			delete event.message.slaveLoading.overloadedSlaveSet;
			isSend = true;
			break;
		case MASTER_EVENT_TYPE_REMAPPING_SET_LOCK_RESPONSE_SUCCESS:
			success = true;
		case MASTER_EVENT_TYPE_REMAPPING_SET_LOCK_RESPONSE_FAILURE:
			buffer.data = this->protocol.resRemappingSetLock(
				buffer.size,
				event.instanceId, event.requestId,
				success,
				event.message.remap.original,
				event.message.remap.remapped,
				event.message.remap.remappedCount,
				event.message.remap.key.size,
				event.message.remap.key.data
			);
			isSend = true;
			break;
		case MASTER_EVENT_TYPE_SWITCH_PHASE:
		{
			Coordinator *coordinator = Coordinator::getInstance();
			std::vector<struct sockaddr_in> *slaves = event.message.switchPhase.slaves;

			isSend = false;
			if ( slaves == NULL || ! coordinator->remapMsgHandler )
				break;
			// just trigger the handling of transition, no message need to be handled
			if ( event.message.switchPhase.toRemap ) {
				size_t numMasters = coordinator->sockets.masters.size();
				for ( size_t i = 0, numOverloadedSlaves = slaves->size(); i < numOverloadedSlaves; i++ ) {
					uint16_t instanceId = 0;

					for ( size_t j = 0, numSlaves = coordinator->sockets.slaves.size(); j < numSlaves; j++ ) {
						struct sockaddr_in addr = slaves->at( i );
						if ( coordinator->sockets.slaves[ j ]->equal( addr.sin_addr.s_addr, addr.sin_port ) ) {
							instanceId = coordinator->sockets.slaves[ j ]->instanceId;
							break;
						}
					}

					if ( instanceId != 0 ) {
						// Update pending set for metadata backup
						CoordinatorWorker::pending->addPendingTransition(
							instanceId, // instanceId
							true,       // isDegraded
							numMasters  // pending
						);
					}
				}

				coordinator->remapMsgHandler->transitToDegraded( slaves ); // Phase 1a --> 2
			} else {
				coordinator->remapMsgHandler->transitToNormal( slaves ); // Phase 1b --> 0
			}
			// free the vector of slaves
			delete slaves;
		}
			break;
		// Degraded operation
		case MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_IS_LOCKED:
		case MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_WAS_LOCKED:
			buffer.data = this->protocol.resDegradedLock(
				buffer.size,
				event.instanceId, event.requestId,
				event.type == MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_IS_LOCKED, // success
				event.message.degradedLock.key.size,
				event.message.degradedLock.key.data,
				event.message.degradedLock.isSealed,
				event.message.degradedLock.stripeId,
				event.message.degradedLock.original,
				event.message.degradedLock.reconstructed,
				event.message.degradedLock.reconstructedCount
			);
			isSend = true;
			break;
		case MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_NOT_LOCKED:
		case MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_NOT_FOUND:
			buffer.data = this->protocol.resDegradedLock(
				buffer.size,
				event.instanceId, event.requestId,
				event.type == MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_NOT_LOCKED, // exist
				event.message.degradedLock.key.size,
				event.message.degradedLock.key.data
			);
			isSend = true;
			break;
		case MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_REMAPPED:
			buffer.data = this->protocol.resDegradedLock(
				buffer.size,
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
		case MASTER_EVENT_TYPE_ANNOUNCE_SLAVE_RECONSTRUCTED:
			isSend = false;
			break;
		// Pending
		case MASTER_EVENT_TYPE_PENDING:
			isSend = false;
			break;
		default:
			return;
	}

	if ( isSend ) {
		ret = event.socket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size )
			__ERROR__( "CoordinatorWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
	} else if ( event.type == MASTER_EVENT_TYPE_SWITCH_PHASE ) {
		connected = true; // just to avoid error message
	} else if ( event.type == MASTER_EVENT_TYPE_ANNOUNCE_SLAVE_RECONSTRUCTED ) {
		ArrayMap<int, MasterSocket> &masters = Coordinator::getInstance()->sockets.masters;
		uint32_t requestId = CoordinatorWorker::idGenerator->nextVal( this->workerId );

		buffer.data = this->protocol.announceSlaveReconstructed(
			buffer.size, Coordinator::instanceId, requestId,
			event.message.reconstructed.src,
			event.message.reconstructed.dst,
			false // toSlave
		);

		LOCK( &masters.lock );
		for ( uint32_t i = 0, size = masters.size(); i < size; i++ ) {
			MasterSocket *master = masters.values[ i ];
			if ( ! master->ready() )
				continue; // Skip failed masters

			ret = master->send( buffer.data, buffer.size, connected );
			if ( ret != ( ssize_t ) buffer.size )
				__ERROR__( "CoordinatorWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
		}
		UNLOCK( &masters.lock );

		connected = true; // just to avoid error message
	} else {
		ProtocolHeader header;
		WORKER_RECEIVE_FROM_EVENT_SOCKET();

		struct LoadStatsHeader loadStatsHeader;
		ArrayMap< struct sockaddr_in, Latency > getLatency, setLatency, *latencyPool = NULL;
		Coordinator *coordinator = Coordinator::getInstance();
		struct sockaddr_in masterAddr;

		while( buffer.size > 0 ) {
			WORKER_RECEIVE_WHOLE_MESSAGE_FROM_EVENT_SOCKET( "CoordinatorWorker" );

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;

			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_MASTER ) {
				__ERROR__( "CoordinatorWorker", "dispatch", "Invalid message source from master." );
			}

			int index = 0;

			if ( header.magic == PROTO_MAGIC_LOADING_STATS ) {
				this->protocol.parseLoadStatsHeader( loadStatsHeader, buffer.data, buffer.size );
				buffer.data += PROTO_LOAD_STATS_SIZE;
				buffer.size -= PROTO_LOAD_STATS_SIZE;
				if ( ! this->protocol.parseLoadingStats( loadStatsHeader, getLatency, setLatency, buffer.data, buffer.size ) )
					__ERROR__( "CoordinatorWorker", "dispatch", "Invalid amount of data received from master." );
				//fprintf( stderr, "get stats GET %d SET %d\n", loadStatsHeader.slaveGetCount, loadStatsHeader.slaveSetCount );
				// set the latest loading stats
				//fprintf( stderr, "fd %d IP %u:%hu\n", event.socket->getSocket(), ntohl( event.socket->getAddr().sin_addr.s_addr ), ntohs( event.socket->getAddr().sin_port ) );

#define SET_SLAVE_LATENCY_FOR_MASTER( _MASTER_ADDR_, _SRC_, _DST_ ) \
	for ( uint32_t i = 0; i < _SRC_.size(); i++ ) { \
		coordinator->slaveLoading._DST_.get( _SRC_.keys[ i ], &index ); \
		if ( index == -1 ) { \
			coordinator->slaveLoading._DST_.set( _SRC_.keys[ i ], new ArrayMap<struct sockaddr_in, Latency> () ); \
			index = coordinator->slaveLoading._DST_.size() - 1; \
			coordinator->slaveLoading._DST_.values[ index ]->set( _MASTER_ADDR_, _SRC_.values[ i ] ); \
		} else { \
			latencyPool = coordinator->slaveLoading._DST_.values[ index ]; \
			latencyPool->get( _MASTER_ADDR_, &index ); \
			if ( index == -1 ) { \
				latencyPool->set( _MASTER_ADDR_, _SRC_.values[ i ] ); \
			} else { \
				delete latencyPool->values[ index ]; \
				latencyPool->values[ index ] = _SRC_.values[ i ]; \
			} \
		} \
	} \

				masterAddr = event.socket->getAddr();
				LOCK ( &coordinator->slaveLoading.lock );
				SET_SLAVE_LATENCY_FOR_MASTER( masterAddr, getLatency, latestGet );
				SET_SLAVE_LATENCY_FOR_MASTER( masterAddr, setLatency, latestSet );
				UNLOCK ( &coordinator->slaveLoading.lock );

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
						this->handleRemappingSetLockRequest( event, buffer.data, header.length );
						break;
					case PROTO_OPCODE_DEGRADED_LOCK:
						this->handleDegradedLockRequest( event, buffer.data, header.length );
						break;
					default:
						goto quit_1;
				}
			} else if ( header.magic == PROTO_MAGIC_REMAPPING ) {
				switch( header.opcode ) {
					case PROTO_OPCODE_SYNC:
					{
						coordinator->pending.decrementRemappingRecords( header.requestId, event.socket->getAddr(), true, false );
						coordinator->pending.checkAndRemoveRemappingRecords( header.requestId, 0, false, true );
					}
						break;
					default:
						__ERROR__( "CoordinatorWorker", "dispatch", "Invalid opcode from master." );
						goto quit_1;
				}
			} else if ( header.magic == PROTO_MAGIC_HEARTBEAT && header.opcode == PROTO_OPCODE_SYNC ) {
				this->handleSyncMetadata( event, buffer.data, header.length );
			} else {
				__ERROR__( "CoordinatorWorker", "dispatch", "Invalid magic code from master." );
				goto quit_1;
			}

#undef SET_SLAVE_LATENCY_FOR_MASTER
quit_1:
			buffer.data += header.length;
			buffer.size -= header.length;
		}

		if ( connected ) event.socket->done();

	}

	if ( ! connected )
		__ERROR__( "CoordinatorWorker", "dispatch", "The master is disconnected." );
}

bool CoordinatorWorker::handleSyncMetadata( MasterEvent event, char *buf, size_t size ) {
	// uint16_t instanceId = event.instanceId;
	uint32_t count, requestId = event.requestId;
	size_t processed, offset, failed = 0;
	SlaveSocket *target = 0;
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
		"Slave: %s:%u (sealed: %u; ops: %u).\n",
		ipBuf, Socket::hton_port( address.port ),
		heartbeat.sealed, heartbeat.keys
	);

	ArrayMap<int, SlaveSocket> &slaves = Coordinator::getInstance()->sockets.slaves;
	LOCK( &slaves.lock );
	for ( uint32_t i = 0; i < slaves.size(); i++ ) {
		if ( slaves.values[ i ]->equal( address.addr, address.port ) ) {
			target = slaves.values[ i ];
			break;
		}
	}
	UNLOCK( &slaves.lock );
	if ( ! target ) {
		__ERROR__( "CoordinatorWorker", "handleSyncMetadata", "Slave not found." );
		return false;
	}

	LOCK( &target->map.chunksLock );
	for ( count = 0; count < heartbeat.sealed; count++ ) {
		if ( this->protocol.parseMetadataHeader( header.metadata, processed, buf, size, offset ) ) {
			// fprintf(
			// 	stderr, "(%u, %u, %u)\n",
			// 	header.metadata.listId,
			// 	header.metadata.stripeId,
			// 	header.metadata.chunkId
			// );
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
			SlaveSocket *s = target;
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
	} else {
		// Send ACK message
		// event.resHeartbeat( target, heartbeat.timestamp, heartbeat.sealed, heartbeat.keys, heartbeat.isLast );
		// this->dispatch( event );
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
	// } else {
	// 	printf( "requestId = %u, heartbeat.isLast = %d, failed = %lu\n", requestId, heartbeat.isLast, failed );
	}

	return failed == 0;
}
