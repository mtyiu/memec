#include "worker.hh"
#include "../main/coordinator.hh"

void CoordinatorWorker::dispatch( MasterEvent event ) {
	bool connected = false, isSend, success = false;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;
	Coordinator *coordinator = Coordinator::getInstance();
	Packet *packet = NULL;

	switch( event.type ) {
		case MASTER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
			buffer.data = this->protocol.resRegisterMaster( buffer.size, event.id, true );
			isSend = true;
			break;
		case MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
			buffer.data = this->protocol.resRegisterMaster( buffer.size, event.id, false );
			isSend = true;
			break;
		case MASTER_EVENT_TYPE_PUSH_LOADING_STATS:
			buffer.data = this->protocol.reqPushLoadStats(
				buffer.size, 0, // id
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
		case MASTER_EVENT_TYPE_FORWARD_REMAPPING_RECORDS:
			buffer.size = event.message.forward.prevSize;
			buffer.data = this->protocol.forwardRemappingRecords ( buffer.size, 0, event.message.forward.data );
			delete [] event.message.forward.data;
			isSend = true;
			break;
		case MASTER_EVENT_TYPE_REMAPPING_SET_LOCK_RESPONSE_SUCCESS:
			success = true;
		case MASTER_EVENT_TYPE_REMAPPING_SET_LOCK_RESPONSE_FAILURE:
			buffer.data = this->protocol.resRemappingSetLock(
				buffer.size,
				event.id,
				success,
				event.message.remap.listId,
				event.message.remap.chunkId,
				event.message.remap.isRemapped,
				event.message.remap.key.size,
				event.message.remap.key.data,
				event.message.remap.sockfd
			);
			isSend = true;
			break;
		case MASTER_EVENT_TYPE_SWITCH_PHASE:
			isSend = false;
			if ( event.message.remap.slaves == NULL || ! Coordinator::getInstance()->remapMsgHandler )
				break;
			// just trigger the handling of transition, no message need to be handled
			if ( event.message.remap.toRemap ) {
				coordinator->remapMsgHandler->transitToDegraded( event.message.remap.slaves ); // Phase 1a --> 2
			} else {
				coordinator->remapMsgHandler->transitToNormal( event.message.remap.slaves ); // Phase 1b --> 0
			}
			// free the vector of slaves
			delete event.message.remap.slaves;
			break;
		case MASTER_EVENT_TYPE_SYNC_REMAPPING_RECORDS:
			// TODO directly send packets out
		{
			std::vector<Packet*> *packets = event.message.remap.syncPackets;

			packet = packets->back();
			buffer.data = packet->data;
			buffer.size = packet->size;

			packets->pop_back();

			// check if this is the last packet to send
			if ( packets->empty() )
				delete packets;
			else
				coordinator->eventQueue.insert( event );
		}
			isSend = true;
			break;
		// Degraded operation
		case MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_IS_LOCKED:
		case MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_WAS_LOCKED:
			buffer.data = this->protocol.resDegradedLock(
				buffer.size,
				event.id,
				event.type == MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_IS_LOCKED /* success */,
				event.message.degradedLock.isSealed,
				event.message.degradedLock.key.size,
				event.message.degradedLock.key.data,
				event.message.degradedLock.listId,
				event.message.degradedLock.stripeId,
				event.message.degradedLock.srcDataChunkId,
				event.message.degradedLock.dstDataChunkId,
				event.message.degradedLock.srcParityChunkId,
				event.message.degradedLock.dstParityChunkId
			);
			isSend = true;
			break;
		case MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_NOT_LOCKED:
		case MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_NOT_FOUND:
			buffer.data = this->protocol.resDegradedLock(
				buffer.size,
				event.id,
				event.type == MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_NOT_LOCKED /* exist */,
				event.message.degradedLock.key.size,
				event.message.degradedLock.key.data,
				event.message.degradedLock.listId,
				event.message.degradedLock.srcDataChunkId,
				event.message.degradedLock.srcParityChunkId
			);
			isSend = true;
			break;
		case MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_REMAPPED:
			buffer.data = this->protocol.resDegradedLock(
				buffer.size,
				event.id,
				event.message.degradedLock.key.size,
				event.message.degradedLock.key.data,
				event.message.degradedLock.listId,
				event.message.degradedLock.srcDataChunkId,
				event.message.degradedLock.dstDataChunkId,
				event.message.degradedLock.srcParityChunkId,
				event.message.degradedLock.dstParityChunkId
			);
			isSend = true;
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
		if ( event.type == MASTER_EVENT_TYPE_SYNC_REMAPPING_RECORDS && packet )
			coordinator->packetPool.free( packet );
	} else if ( event.type == MASTER_EVENT_TYPE_SWITCH_PHASE ) {
		// just to avoid error message
		connected = true;
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
				event.id = header.id;
				switch( header.opcode ) {
					case PROTO_OPCODE_REMAPPING_LOCK:
						this->handleRemappingSetLockRequest( event, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_DEGRADED_LOCK:
						this->handleDegradedLockRequest( event, buffer.data, buffer.size );
						break;
					default:
						goto quit_1;
				}
			} else if ( header.magic == PROTO_MAGIC_REMAPPING ) {
				switch( header.opcode ) {
					case PROTO_OPCODE_SYNC:
					{
						coordinator->pending.decrementRemappingRecords( header.id, event.socket->getAddr(), true, false );
						coordinator->pending.checkAndRemoveRemappingRecords( header.id, 0, false, true );
					}
						break;
					default:
						__ERROR__( "CoordinatorWorker", "dispatch", "Invalid opcode from master." );
						goto quit_1;
				}
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
