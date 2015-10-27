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
StripeList<ServerAddr> *CoordinatorWorker::stripeList;

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
	bool connected = false, isSend;
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
		case MASTER_EVENT_TYPE_SWITCH_PHASE:
			// just trigger / stop the remap phase, no message need to be handled
			if ( event.message.remap.toRemap ) {
				coordinator->remapMsgHandler.startRemap();
			} else {
				coordinator->remapMsgHandler.stopRemap();
			}
			isSend = false;
			break;
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

			switch ( header.magic ) {
				case PROTO_MAGIC_LOADING_STATS:
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
					break;
				case PROTO_MAGIC_REQUEST:
					goto quit_1;
				default:
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
	bool connected, isSend;
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
			if ( event.socket->equal( slave ) )
				continue; // No need to tell the new socket

			ret = slave->send( buffer.data, buffer.size, connected );
			if ( ret != ( ssize_t ) buffer.size )
				__ERROR__( "CoordinatorWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
		}
		UNLOCK( &slaves.lock );
	} else if ( event.type == SLAVE_EVENT_TYPE_DISCONNECT ) {
		this->triggerRecovery( event.socket );
	} else if ( isSend ) {
		ret = event.socket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size )
			__ERROR__( "CoordinatorWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
		if ( ! connected )
			__ERROR__( "CoordinatorWorker", "dispatch", "The slave is disconnected." );
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
				this->processHeartbeat( event, buffer.data, header.length );
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

bool CoordinatorWorker::processHeartbeat( SlaveEvent event, char *buf, size_t size ) {
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
			event.socket->map.seal(
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
			event.socket->map.setKey(
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

	return failed == 0;
}

bool CoordinatorWorker::triggerRecovery( SlaveSocket *socket ) {
	ServerAddr addr( 0, socket->listenAddr.addr, socket->listenAddr.port, 0 );
	int index = CoordinatorWorker::stripeList->search( &addr, ServerAddr::match );
	if ( index == -1 ) {
		__ERROR__( "CoordinatorWorker", "triggerRecovery", "The disconnected server does not exist in the consistent hash ring.\n" );
		return false;
	}

	uint32_t numLostChunks = 0;
	std::set<Metadata> unsealedChunks;
	std::unordered_map<uint32_t, SlaveSocket **> sockets;
	std::vector<StripeListIndex> lists = CoordinatorWorker::stripeList->list( ( uint32_t ) index );

	std::unordered_map<uint32_t, ServerAddr **> addrs;
	ArrayMap<int, SlaveSocket> &map = Coordinator::getInstance()->sockets.slaves;

	////////////////////////////////////////////////
	// Get the ServerAddr of the surviving slaves //
	////////////////////////////////////////////////
	LOCK( &Map::stripesLock );
	printf( "Slave disconnected! index = %d. Appeared in:\n", index );
	for ( uint32_t i = 0, size = lists.size(); i < size; i++ ) {
		printf(
			"(%u, %u, %s%u)\n",
			lists[ i ].listId,
			Map::stripes[ lists[ i ].listId ],
			lists[ i ].isParity ? "p" : "",
			lists[ i ].chunkId
		);

		if ( addrs.find( lists[ i ].listId ) == addrs.end() ) {
			ServerAddr **addr = new ServerAddr*[ CoordinatorWorker::chunkCount ];
			SlaveSocket **s = new SlaveSocket*[ CoordinatorWorker::chunkCount ];

			CoordinatorWorker::stripeList->get(
				lists[ i ].listId, addr + CoordinatorWorker::dataChunkCount, addr
			);

			addrs[ lists[ i ].listId ] = addr;
			sockets[ lists[ i ].listId ] = s;
		}
	}
	UNLOCK( &Map::stripesLock );

	////////////////////////////////////////////
	// Obtain the SlaveSocket from ServerAddr //
	////////////////////////////////////////////
	LOCK( &map.lock );
	for (
		std::unordered_map<uint32_t, ServerAddr **>::iterator it = addrs.begin();
		it != addrs.end();
		it++
	) {
		ServerAddr **target = it->second;
		for ( uint32_t i = 0; i < CoordinatorWorker::chunkCount; i++ ) {
			for ( uint32_t j = 0, size = map.values.size(); j < size; j++ ) {
				ServerAddr tmp( 0, map.values[ j ]->listenAddr.addr, map.values[ j ]->listenAddr.port, 0 );
				if ( ServerAddr::match( &tmp, target[ i ] ) ) {
					sockets[ it->first ][ i ] = map.values[ j ];
					break;
				}
			}
		}
		delete[] it->second;
	}
	UNLOCK( &map.lock );
	addrs.clear();

	LOCK( &socket->map.chunksLock );
	LOCK( &socket->map.keysLock );

	/////////////////////////////////////////////////////////////////
	// Distribute the reconstruction tasks to the surviving slaves //
	/////////////////////////////////////////////////////////////////
	for ( std::unordered_set<Metadata>::iterator chunksIt = socket->map.chunks.begin(); chunksIt != socket->map.chunks.end(); chunksIt++ ) {
		const Metadata &metadata = *chunksIt;
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
