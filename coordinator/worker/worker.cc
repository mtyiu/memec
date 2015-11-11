#include "worker.hh"
#include "../main/coordinator.hh"
#include "../../common/util/debug.hh"
#include "../../common/ds/sockaddr_in.hh"

#define WORKER_COLOR	YELLOW

uint32_t CoordinatorWorker::dataChunkCount;
uint32_t CoordinatorWorker::parityChunkCount;
uint32_t CoordinatorWorker::chunkCount;
IDGenerator *CoordinatorWorker::idGenerator;
CoordinatorEventQueue *CoordinatorWorker::eventQueue;
RemappingRecordMap *CoordinatorWorker::remappingRecords;
StripeList<SlaveSocket> *CoordinatorWorker::stripeList;

void CoordinatorWorker::dispatch( MixedEvent event ) {
	switch( event.type ) {
		case EVENT_TYPE_COORDINATOR:
			this->dispatch( event.event.coordinator );
			break;
		case EVENT_TYPE_MASTER:
			this->dispatch( event.event.master );
			break;
		case EVENT_TYPE_SLAVE:
			this->dispatch( event.event.slave );
			break;
		default:
			return;
	}
}

void CoordinatorWorker::dispatch( CoordinatorEvent event ) {
}

void CoordinatorWorker::dispatch( MasterEvent event ) {
	bool connected = false, isSend, success = false;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;
	Coordinator *coordinator = Coordinator::getInstance();

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
			buffer.data = this->protocol.reqPushLoadStats (
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
				event.message.remap.key.data
			);
			isSend = true;
			break;
		case MASTER_EVENT_TYPE_SWITCH_PHASE:
			isSend = false;
			if ( event.message.remap.slaves == NULL || ! Coordinator::getInstance()->remapMsgHandler )
				break;
			// just trigger / stop the remap phase, no message need to be handled
			if ( event.message.remap.toRemap ) {
				coordinator->remapMsgHandler->startRemap( event.message.remap.slaves );
			} else {
				coordinator->remapMsgHandler->stopRemap( event.message.remap.slaves );
			}
			// free the vector of slaves
			delete event.message.remap.slaves;
			break;
		case MASTER_EVENT_TYPE_PENDING:
			isSend = false;
			break;
		// Degraded operation
		case MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_IS_LOCKED:
		case MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_WAS_LOCKED:
			buffer.data = this->protocol.resDegradedLock(
				buffer.size,
				event.id,
				event.message.degradedLock.key.size,
				event.message.degradedLock.key.data,
				event.type == MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_IS_LOCKED /* success */,
				event.message.degradedLock.isSealed,
				event.message.degradedLock.srcListId,
				event.message.degradedLock.srcStripeId,
				event.message.degradedLock.srcChunkId,
				event.message.degradedLock.dstListId,
				event.message.degradedLock.dstChunkId
			);
			isSend = true;
			break;
		case MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_REMAPPED:
			buffer.data = this->protocol.resDegradedLock(
				buffer.size,
				event.id,
				event.message.degradedLock.key.size,
				event.message.degradedLock.key.data,
				event.message.degradedLock.srcListId,
				event.message.degradedLock.srcChunkId
			);
			isSend = true;
			break;
		case MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_NOT_FOUND:
			buffer.data = this->protocol.resDegradedLock(
				buffer.size,
				event.id,
				event.message.degradedLock.key.size,
				event.message.degradedLock.key.data
			);
			isSend = true;
			break;
		default:
			return;
	}

	if ( isSend ) {
		ret = event.socket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size )
			__ERROR__( "CoordinatorWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
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

void CoordinatorWorker::dispatch( SlaveEvent event ) {
	bool connected, isSend, isCompleted;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;
	uint32_t requestId;

	switch( event.type ) {
		case SLAVE_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
			buffer.data = this->protocol.resRegisterSlave( buffer.size, event.id, true );
			isSend = true;
			break;
		case SLAVE_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
			buffer.data = this->protocol.resRegisterSlave( buffer.size, event.id, false );
			isSend = true;
			break;
		case SLAVE_EVENT_TYPE_REQUEST_SEAL_CHUNKS:
			requestId = CoordinatorWorker::idGenerator->nextVal( this->workerId );
			buffer.data = this->protocol.reqSealChunks( buffer.size, requestId );
			isSend = true;
			break;
		case SLAVE_EVENT_TYPE_REQUEST_FLUSH_CHUNKS:
			requestId = CoordinatorWorker::idGenerator->nextVal( this->workerId );
			buffer.data = this->protocol.reqFlushChunks( buffer.size, requestId );
			isSend = true;
			break;
		case SLAVE_EVENT_TYPE_REQUEST_SYNC_META:
			requestId = CoordinatorWorker::idGenerator->nextVal( this->workerId );
			buffer.data = this->protocol.reqSyncMeta( buffer.size, requestId );
			// add sync meta request to pending set
			Coordinator::getInstance()->pending.addSyncMetaReq( requestId, event.sync );
			isSend = true;
			break;
		case SLAVE_EVENT_TYPE_REQUEST_RELEASE_DEGRADED_LOCK:
			requestId = CoordinatorWorker::idGenerator->nextVal( this->workerId );
			buffer.data = this->protocol.reqReleaseDegradedLock(
				buffer.size, requestId,
				&event.socket->map.degradedLocksLock,
				&event.socket->map.degradedLocks,
				&event.socket->map.releasingDegradedLocks,
				isCompleted
			);
			if ( buffer.size == PROTO_HEADER_SIZE ) {
				__ERROR__( "CoordinatorWorker", "dispatch", "No chunks are locked on this slave." );
				return;
			}
			isSend = true;
			break;
		case SLAVE_EVENT_TYPE_PENDING:
			isSend = false;
			break;
		case SLAVE_EVENT_TYPE_ANNOUNCE_SLAVE_CONNECTED:
			isSend = false;
			break;
		case SLAVE_EVENT_TYPE_DISCONNECT:
			isSend = false;
			break;
		default:
			return;
	}

	if ( event.type == SLAVE_EVENT_TYPE_ANNOUNCE_SLAVE_CONNECTED ) {
		ArrayMap<int, SlaveSocket> &slaves = Coordinator::getInstance()->sockets.slaves;
		uint32_t requestId = CoordinatorWorker::idGenerator->nextVal( this->workerId );

		buffer.data = this->protocol.announceSlaveConnected( buffer.size, requestId, event.socket );

		LOCK( &slaves.lock );
		for ( uint32_t i = 0; i < slaves.size(); i++ ) {
			SlaveSocket *slave = slaves.values[ i ];
			if ( event.socket->equal( slave ) || ! slave->ready() )
				continue; // No need to tell the new socket

			ret = slave->send( buffer.data, buffer.size, connected );
			if ( ret != ( ssize_t ) buffer.size )
				__ERROR__( "CoordinatorWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
		}
		// notify the remap message handler of the new slave
		struct sockaddr_in slaveAddr = event.socket->getAddr();
		if ( Coordinator::getInstance()->remapMsgHandler )
			Coordinator::getInstance()->remapMsgHandler->addAliveSlave( slaveAddr );
		UNLOCK( &slaves.lock );
	} else if ( event.type == SLAVE_EVENT_TYPE_DISCONNECT ) {
		this->triggerRecovery( event.socket );
		// notify the remap message handler of a "removed" slave
		if ( Coordinator::getInstance()->remapMsgHandler )
			Coordinator::getInstance()->remapMsgHandler->removeAliveSlave( event.socket->getAddr() );
	} else if ( isSend ) {
		ret = event.socket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size )
			__ERROR__( "CoordinatorWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
		if ( ! connected )
			__ERROR__( "CoordinatorWorker", "dispatch", "The slave is disconnected." );

		if ( event.type == SLAVE_EVENT_TYPE_REQUEST_RELEASE_DEGRADED_LOCK && ! isCompleted ) {
			event.reqReleaseDegradedLock( event.socket );
			this->eventQueue->insert( event );
		}
	} else {
		// Parse requests from slaves
		ProtocolHeader header;
		WORKER_RECEIVE_FROM_EVENT_SOCKET();
		ArrayMap<int, MasterSocket> &masters = Coordinator::getInstance()->sockets.masters;
		while( buffer.size > 0 ) {
			WORKER_RECEIVE_WHOLE_MESSAGE_FROM_EVENT_SOCKET( "CoordinatorWorker" );

			// avvoid declaring variables after jump statements
			size_t bytes, offset, count = 0;
			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_SLAVE ) {
				__ERROR__( "CoordinatorWorker", "dispatch", "Invalid message source from slave." );
				goto quit_1;
			}

			if ( header.opcode != PROTO_OPCODE_SYNC ) {
				__ERROR__( "CoordinatorWorker", "dispatch", "Invalid opcode from slave." );
				goto quit_1;
			}

			if ( header.magic == PROTO_MAGIC_HEARTBEAT ) {
				this->processHeartbeat( event, buffer.data, header.length, header.id );
			} else if ( header.magic == PROTO_MAGIC_REMAPPING ) {
				struct RemappingRecordHeader remappingRecordHeader;
				struct SlaveSyncRemapHeader slaveSyncRemapHeader;
				if ( ! this->protocol.parseRemappingRecordHeader( remappingRecordHeader, buffer.data, buffer.size ) ) {
					__ERROR__( "CoordinatorWorker", "dispatch", "Invalid remapping record protocol header." );
					goto quit_1;
				}
				// start parsing the remapping records
				// TODO buffer.size >> total size of remapping records?
				offset = PROTO_REMAPPING_RECORD_SIZE;
				RemappingRecordMap *map = CoordinatorWorker::remappingRecords;
				for ( count = 0; offset < ( size_t ) buffer.size && count < remappingRecordHeader.remap; offset += bytes ) {
					if ( ! this->protocol.parseSlaveSyncRemapHeader( slaveSyncRemapHeader, bytes, buffer.data, buffer.size - offset, offset ) )
						break;
					count++;

					Key key;
					key.set( slaveSyncRemapHeader.keySize, slaveSyncRemapHeader.key );

					RemappingRecord remappingRecord;
					remappingRecord.set( slaveSyncRemapHeader.listId, slaveSyncRemapHeader.chunkId, 0 );

					if ( slaveSyncRemapHeader.opcode == 0 ) { // remove record
						map->erase( key, remappingRecord );
					} else if ( slaveSyncRemapHeader.opcode == 1 ) { // add record
						map->insert( key, remappingRecord );
					}
				}
				//map->print();
				//fprintf ( stderr, "Remapping Records no.=%lu (%u) upto=%lu size=%lu\n", count, remappingRecordHeader.remap, offset, buffer.size );

				// forward the copies of message to masters
				MasterEvent masterEvent;
				masterEvent.type = MASTER_EVENT_TYPE_FORWARD_REMAPPING_RECORDS;
				masterEvent.message.forward.prevSize = buffer.size;
				for ( uint32_t i = 0; i < masters.size() ; i++ ) {
					masterEvent.socket = masters.values[ i ];
					masterEvent.message.forward.data = new char[ buffer.size ];
					memcpy( masterEvent.message.forward.data, buffer.data, buffer.size );
					CoordinatorWorker::eventQueue->insert( masterEvent );
				}
			} else {
				__ERROR__( "CoordinatorWorker", "dispatch", "Invalid magic code from slave." );
			}
quit_1:
			buffer.data += header.length;
			buffer.size -= header.length;
		}
		if ( connected )
			event.socket->done();
		else
			__ERROR__( "CoordinatorWorker", "dispatch", "The slave is disconnected." );
	}
}

void CoordinatorWorker::free() {
	this->protocol.free();
}

void *CoordinatorWorker::run( void *argv ) {
	CoordinatorWorker *worker = ( CoordinatorWorker * ) argv;
	WorkerRole role = worker->getRole();
	CoordinatorEventQueue *eventQueue = CoordinatorWorker::eventQueue;

#define COORDINATOR_WORKER_EVENT_LOOP(_EVENT_TYPE_, _EVENT_QUEUE_) \
	do { \
		_EVENT_TYPE_ event; \
		bool ret; \
		while( worker->getIsRunning() | ( ret = _EVENT_QUEUE_->extract( event ) ) ) { \
			if ( ret ) \
				worker->dispatch( event ); \
		} \
	} while( 0 )

	switch ( role ) {
		case WORKER_ROLE_MIXED:
			COORDINATOR_WORKER_EVENT_LOOP(
				MixedEvent,
				eventQueue->mixed
			);
			break;
		case WORKER_ROLE_COORDINATOR:
			COORDINATOR_WORKER_EVENT_LOOP(
				CoordinatorEvent,
				eventQueue->separated.coordinator
			);
			break;
		case WORKER_ROLE_MASTER:
			COORDINATOR_WORKER_EVENT_LOOP(
				MasterEvent,
				eventQueue->separated.master
			);
			break;
		case WORKER_ROLE_SLAVE:
			COORDINATOR_WORKER_EVENT_LOOP(
				SlaveEvent,
				eventQueue->separated.slave
			);
			break;
		default:
			break;
	}

	worker->free();
	pthread_exit( 0 );
	return 0;
}

bool CoordinatorWorker::processHeartbeat( SlaveEvent event, char *buf, size_t size, uint32_t requestId ) {
	uint32_t count;
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
			SlaveSocket *s = event.socket;
			if ( header.op.opcode == PROTO_OPCODE_DELETE ) // Handle keys from degraded DELETE
				s = CoordinatorWorker::stripeList->get( header.op.listId, header.op.chunkId );
			s->map.insertKey(
				header.op.key,
				header.op.keySize,
				header.op.listId,
				header.op.stripeId,
				header.op.chunkId,
				header.op.opcode,
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
	// } else {
	// 	__ERROR__( "CoordinatorWorker", "processHeartbeat", "(sealed, keys, remap) = (%u, %u, %u)", heartbeat.sealed, heartbeat.keys, heartbeat.remap );
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

bool CoordinatorWorker::triggerRecovery( SlaveSocket *socket ) {
	int index = CoordinatorWorker::stripeList->search( socket );
	if ( index == -1 ) {
		__ERROR__( "CoordinatorWorker", "triggerRecovery", "The disconnected server does not exist in the consistent hash ring.\n" );
		return false;
	}

	uint32_t numLostChunks = 0;
	std::set<Metadata> unsealedChunks;
	std::unordered_map<uint32_t, SlaveSocket **> sockets;
	std::vector<StripeListIndex> lists = CoordinatorWorker::stripeList->list( ( uint32_t ) index );

	ArrayMap<int, SlaveSocket> &map = Coordinator::getInstance()->sockets.slaves;

	//////////////////////////////////////////////////
	// Get the SlaveSockets of the surviving slaves //
	//////////////////////////////////////////////////
	LOCK( &Map::stripesLock );
	LOCK( &map.lock );
	printf( "Slave disconnected! index = %d. Appeared in:\n", index );
	for ( uint32_t i = 0, size = lists.size(); i < size; i++ ) {
		printf(
			"(%u, %u, %s%u)\n",
			lists[ i ].listId,
			Map::stripes[ lists[ i ].listId ],
			lists[ i ].isParity ? "p" : "",
			lists[ i ].chunkId
		);

		if ( sockets.find( lists[ i ].listId ) == sockets.end() ) {
			SlaveSocket **s = new SlaveSocket*[ CoordinatorWorker::chunkCount ];

			CoordinatorWorker::stripeList->get(
				lists[ i ].listId, s + CoordinatorWorker::dataChunkCount, s
			);

			sockets[ lists[ i ].listId ] = s;
		}
	}
	UNLOCK( &map.lock );
	UNLOCK( &Map::stripesLock );

	LOCK( &socket->map.chunksLock );
	LOCK( &socket->map.keysLock );

	/////////////////////////////////////////////////////////////////
	// Distribute the reconstruction tasks to the surviving slaves //
	/////////////////////////////////////////////////////////////////
	for ( std::unordered_set<Metadata>::iterator chunksIt = socket->map.chunks.begin(); chunksIt != socket->map.chunks.end(); chunksIt++ ) {
		// const Metadata &metadata = *chunksIt;
		// printf( "(%u, %u, %u)\n", metadata.listId, metadata.stripeId, metadata.chunkId );
	}
	numLostChunks += socket->map.chunks.size();

	////////////////////////////
	// Handle unsealed chunks //
	////////////////////////////
	// TODO
	for ( std::unordered_map<Key, Metadata>::iterator keysIt = socket->map.keys.begin(); keysIt != socket->map.keys.end(); keysIt++ ) {
		const Metadata &metadata = keysIt->second;
		if (
			socket->map.chunks.find( metadata ) == socket->map.chunks.end() &&
			unsealedChunks.find( metadata ) == unsealedChunks.end()
		) {
			unsealedChunks.insert( metadata );
			// printf( "(%u, %u, %u)\n", metadata.listId, metadata.stripeId, metadata.chunkId );
		}
	}
	numLostChunks += unsealedChunks.size();

	UNLOCK( &socket->map.keysLock );
	UNLOCK( &socket->map.chunksLock );

	printf( "Number of chunks that need to be recovered: %u\n", numLostChunks );

	return true;
}

bool CoordinatorWorker::handleDegradedLockRequest( MasterEvent event, char *buf, size_t size ) {
	struct DegradedLockReqHeader header;
	if ( ! this->protocol.parseDegradedLockReqHeader( header, buf, size ) ) {
		__ERROR__( "CoordinatorWorker", "handleDegradedLockRequest", "Invalid DEGRADED_LOCK request (size = %lu).", size );
		return false;
	}
	__DEBUG__(
		BLUE, "CoordinatorWorker", "handleDegradedLockRequest",
		"[DEGRADED_LOCK] Key: %.*s (key size = %u); target list ID: %u, target chunk ID: %u",
		( int ) header.keySize, header.key, header.keySize, header.dstListId, header.dstChunkId
	);

	// Metadata metadata;
	RemappingRecord remappingRecord;
	Key key;
	key.set( header.keySize, header.key );

	if ( CoordinatorWorker::remappingRecords->find( key, &remappingRecord ) ) {
		// Remapped
		if ( remappingRecord.listId != header.srcListId || remappingRecord.chunkId != header.srcChunkId ) {
			// Reject the degraded operation
			event.resDegradedLock(
				event.socket, event.id, key,
				remappingRecord.listId, remappingRecord.chunkId
			);
			this->dispatch( event );
			return false;
		}
	}

	// Find the SlaveSocket which stores the stripe with srcListId and srcChunkId
	SlaveSocket *socket = CoordinatorWorker::stripeList->get( header.srcListId, header.srcChunkId );
	Map *map = &socket->map;
	Metadata srcMetadata, dstMetadata;
	bool ret;

	dstMetadata.set( header.dstListId, 0, header.dstChunkId );

	if ( ! map->findMetadataByKey( header.key, header.keySize, srcMetadata ) ) {
		// Key not found
		event.resDegradedLock( event.socket, event.id, key );
		ret = false;
	} else {
		ret = map->insertDegradedLock( srcMetadata, dstMetadata );

		event.resDegradedLock(
			event.socket, event.id, key,
			ret,                          // the degraded lock is attained
			map->isSealed( srcMetadata ), // the chunk is sealed
			srcMetadata.listId, srcMetadata.stripeId, srcMetadata.chunkId,
			dstMetadata.listId, dstMetadata.chunkId
		);
	}
	this->dispatch( event );
	return ret;
}

bool CoordinatorWorker::handleRemappingSetLockRequest( MasterEvent event, char *buf, size_t size ) {
	struct RemappingLockHeader header;
	if ( ! this->protocol.parseRemappingLockHeader( header, buf, size ) ) {
		__ERROR__( "CoordinatorWorker", "handleRemappingSetLockRequest", "Invalid REMAPPING_SET_LOCK request (size = %lu).", size );
		return false;
	}
	__DEBUG__(
		BLUE, "CoordinatorWorker", "handleRemappingSetLockRequest",
		"[REMAPPING_SET_LOCK] Key: %.*s (key size = %u); remapped list ID: %u, remapped chunk ID: %u",
		( int ) header.keySize, header.key, header.keySize, header.listId, header.chunkId
	);

	Key key;
	key.set( header.keySize, header.key );

	// Find the SlaveSocket which stores the stripe with srcListId and srcChunkId
	SlaveSocket *socket = CoordinatorWorker::stripeList->get( header.listId, header.chunkId );
	Map *map = &socket->map;

	// if already exists, does not allow remap; otherwise insert the remapping record
	RemappingRecord remappingRecord( header.listId, header.chunkId );
	if ( map->insertKey(
		header.key, header.keySize, header.listId, 0,
		header.chunkId, PROTO_OPCODE_REMAPPING_LOCK,
		true, true)
	) {
		if ( header.isRemapped ) {
			if ( CoordinatorWorker::remappingRecords->insert( key, remappingRecord ) ) {
				event.resRemappingSetLock( event.socket, event.id, header.isRemapped, key, remappingRecord, true );
			} else {
				event.resRemappingSetLock( event.socket, event.id, header.isRemapped, key, remappingRecord, false );
			}
		} else {
			event.resRemappingSetLock( event.socket, event.id, header.isRemapped, key, remappingRecord, true );
		}
	} else {
		event.resRemappingSetLock( event.socket, event.id, header.isRemapped, key, remappingRecord, false );
	}
	this->dispatch( event );

	return true;
}

bool CoordinatorWorker::init() {
	Coordinator *coordinator = Coordinator::getInstance();

	CoordinatorWorker::dataChunkCount =
	coordinator->config.global.coding.params.getDataChunkCount();
	CoordinatorWorker::parityChunkCount = coordinator->config.global.coding.params.getParityChunkCount();
	CoordinatorWorker::chunkCount = CoordinatorWorker::dataChunkCount + CoordinatorWorker::parityChunkCount;
	CoordinatorWorker::idGenerator = &coordinator->idGenerator;
	CoordinatorWorker::eventQueue = &coordinator->eventQueue;
	CoordinatorWorker::remappingRecords = &coordinator->remappingRecords;
	CoordinatorWorker::stripeList = coordinator->stripeList;

	return true;
}

bool CoordinatorWorker::init( GlobalConfig &config, WorkerRole role, uint32_t workerId ) {
	this->protocol.init(
		Protocol::getSuggestedBufferSize(
			config.size.key,
			config.size.chunk
		)
	);
	this->role = role;
	this->workerId = workerId;
	return role != WORKER_ROLE_UNDEFINED;
}

bool CoordinatorWorker::start() {
	this->isRunning = true;
	if ( pthread_create( &this->tid, NULL, CoordinatorWorker::run, ( void * ) this ) != 0 ) {
		__ERROR__( "CoordinatorWorker", "start", "Cannot start worker thread." );
		return false;
	}
	return true;
}

void CoordinatorWorker::stop() {
	this->isRunning = false;
}

void CoordinatorWorker::print( FILE *f ) {
	char role[ 16 ];
	switch( this->role ) {
		case WORKER_ROLE_MIXED:
			strcpy( role, "Mixed" );
			break;
		case WORKER_ROLE_COORDINATOR:
			strcpy( role, "Coordinator" );
			break;
		case WORKER_ROLE_MASTER:
			strcpy( role, "Master" );
			break;
		case WORKER_ROLE_SLAVE:
			strcpy( role, "Slave" );
			break;
		default:
			return;
	}
	fprintf( f, "%11s worker (Thread ID = %lu): %srunning\n", role, this->tid, this->isRunning ? "" : "not " );
}
