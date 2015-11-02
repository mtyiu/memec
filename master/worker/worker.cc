#include <ctime>
#include <ctype.h>
#include <utility>
#include "worker.hh"
#include "../main/master.hh"
#include "../remap/basic_remap_scheme.hh"
#include "../../common/util/debug.hh"
#include "../../common/ds/value.hh"

#define WORKER_COLOR	YELLOW

uint32_t MasterWorker::dataChunkCount;
uint32_t MasterWorker::parityChunkCount;
IDGenerator *MasterWorker::idGenerator;
Pending *MasterWorker::pending;
MasterEventQueue *MasterWorker::eventQueue;
StripeList<SlaveSocket> *MasterWorker::stripeList;
PacketPool *MasterWorker::packetPool;
Counter *MasterWorker::counter;
RemapFlag *MasterWorker::remapFlag;
RemappingRecordMap *MasterWorker::remappingRecords = &Master::getInstance()->remappingRecords;

void MasterWorker::dispatch( MixedEvent event ) {
	switch( event.type ) {
		case EVENT_TYPE_APPLICATION:
			this->dispatch( event.event.application );
			break;
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
			break;
	}
}

void MasterWorker::dispatch( ApplicationEvent event ) {
	bool success = true, connected, isSend;
	ssize_t ret;
	uint32_t valueSize;
	Key key;
	char *value;
	struct {
		size_t size;
		char *data;
	} buffer;

	switch( event.type ) {
		case APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
		case APPLICATION_EVENT_TYPE_GET_RESPONSE_SUCCESS:
		case APPLICATION_EVENT_TYPE_SET_RESPONSE_SUCCESS:
		case APPLICATION_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS:
		case APPLICATION_EVENT_TYPE_DELETE_RESPONSE_SUCCESS:
			success = true;
			isSend = true;
			break;
		case APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
		case APPLICATION_EVENT_TYPE_GET_RESPONSE_FAILURE:
		case APPLICATION_EVENT_TYPE_SET_RESPONSE_FAILURE:
		case APPLICATION_EVENT_TYPE_UPDATE_RESPONSE_FAILURE:
		case APPLICATION_EVENT_TYPE_DELETE_RESPONSE_FAILURE:
			success = false;
			isSend = true;
			break;
		case APPLICATION_EVENT_TYPE_PENDING:
		default:
			isSend = false;
			break;
	}

	switch( event.type ) {
		case APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS:
		case APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_FAILURE:
			buffer.data = this->protocol.resRegisterApplication( buffer.size, event.id, success );
			break;
		case APPLICATION_EVENT_TYPE_GET_RESPONSE_SUCCESS:
			event.message.keyValue.deserialize(
				key.data, key.size,
				value, valueSize
			);
			buffer.data = this->protocol.resGet(
				buffer.size,
				event.id,
				success,
				key.size, key.data,
				valueSize, value
			);
			if ( event.needsFree )
				event.message.keyValue.free();
			break;
		case APPLICATION_EVENT_TYPE_GET_RESPONSE_FAILURE:
			buffer.data = this->protocol.resGet(
				buffer.size,
				event.id,
				success,
				event.message.key.size,
				event.message.key.data
			);
			if ( event.needsFree )
				event.message.key.free();
			break;
		case APPLICATION_EVENT_TYPE_SET_RESPONSE_SUCCESS:
		case APPLICATION_EVENT_TYPE_SET_RESPONSE_FAILURE:
			buffer.data = this->protocol.resSet(
				buffer.size,
				event.id,
				success,
				event.message.key.size,
				event.message.key.data
			);
			if ( event.needsFree )
				event.message.key.free();
			break;
		case APPLICATION_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS:
		case APPLICATION_EVENT_TYPE_UPDATE_RESPONSE_FAILURE:
			buffer.data = this->protocol.resUpdate(
				buffer.size,
				event.id,
				success,
				event.message.keyValueUpdate.size,
				event.message.keyValueUpdate.data,
				event.message.keyValueUpdate.offset,
				event.message.keyValueUpdate.length
			);
			if ( event.needsFree )
				event.message.keyValueUpdate.free();
			break;
		case APPLICATION_EVENT_TYPE_DELETE_RESPONSE_SUCCESS:
		case APPLICATION_EVENT_TYPE_DELETE_RESPONSE_FAILURE:
			buffer.data = this->protocol.resDelete(
				buffer.size,
				event.id,
				success,
				event.message.key.size,
				event.message.key.data
			);
			if ( event.needsFree )
				event.message.key.free();
			break;
		case APPLICATION_EVENT_TYPE_PENDING:
			break;
		default:
			return;
	}

	if ( isSend ) {
		ret = event.socket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size )
			__ERROR__( "MasterWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
	} else {
		// Parse requests from applications
		ProtocolHeader header;
		WORKER_RECEIVE_FROM_EVENT_SOCKET();
		while ( buffer.size > 0 ) {
			WORKER_RECEIVE_WHOLE_MESSAGE_FROM_EVENT_SOCKET( "MasterWorker" );

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			if ( header.magic != PROTO_MAGIC_REQUEST || header.from != PROTO_MAGIC_FROM_APPLICATION ) {
				__ERROR__( "MasterWorker", "dispatch", "Invalid protocol header." );
			} else {
				event.id = header.id;
				switch( header.opcode ) {
					case PROTO_OPCODE_GET:
						this->handleGetRequest( event, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_SET:
						if ( Master::getInstance()->remapMsgHandler.useRemappingFlow() ) {
							this->handleRemappingSetRequest( event, buffer.data, buffer.size );
						} else {
							this->handleSetRequest( event, buffer.data, buffer.size );
						}
						break;
					case PROTO_OPCODE_UPDATE:
						this->handleUpdateRequest( event, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_DELETE:
						this->handleDeleteRequest( event, buffer.data, buffer.size );
						break;
					default:
						__ERROR__( "MasterWorker", "dispatch", "Invalid opcode from application." );
						break;
				}
			}
			buffer.data += header.length;
			buffer.size -= header.length;
		}
		if ( connected ) event.socket->done();
	}

	if ( ! connected ) {
		__DEBUG__( RED, "MasterWorker", "dispatch", "The application is disconnected." );
		// delete event.socket;
	}
}

void MasterWorker::dispatch( CoordinatorEvent event ) {
	bool connected, isSend;
	uint32_t requestId;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;

	if ( event.type != COORDINATOR_EVENT_TYPE_PENDING )
		requestId = MasterWorker::idGenerator->nextVal( this->workerId );

	switch( event.type ) {
		case COORDINATOR_EVENT_TYPE_REGISTER_REQUEST:
			buffer.data = this->protocol.reqRegisterCoordinator(
				buffer.size,
				requestId,
				event.message.address.addr,
				event.message.address.port
			);
			isSend = true;
			break;
		case COORDINATOR_EVENT_TYPE_PUSH_LOAD_STATS:
			// TODO lock the latency when constructing msg ??
			buffer.data = this->protocol.reqPushLoadStats(
				buffer.size,
				requestId,
				event.message.loading.slaveGetLatency,
				event.message.loading.slaveSetLatency
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
			__ERROR__( "MasterWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
	} else {
		ProtocolHeader header;
		WORKER_RECEIVE_FROM_EVENT_SOCKET();
		ArrayMap<struct sockaddr_in, Latency> getLatency, setLatency;
		struct LoadStatsHeader loadStatsHeader;
		struct RemappingRecordHeader remappingRecordHeader;
		struct SlaveSyncRemapHeader slaveSyncRemapHeader;
		size_t offset, count, bytes;
		Master *master = Master::getInstance();

		while ( buffer.size > 0 ) {
			WORKER_RECEIVE_WHOLE_MESSAGE_FROM_EVENT_SOCKET( "MasterWorker" );

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_COORDINATOR ) {
				__ERROR__( "MasterWorker", "dispatch", "Invalid message source from coordinator." );
			} else {
				switch( header.opcode ) {
					case PROTO_OPCODE_REGISTER:
						switch( header.magic ) {
							case PROTO_MAGIC_RESPONSE_SUCCESS:
								event.socket->registered = true;
								break;
							case PROTO_MAGIC_RESPONSE_FAILURE:
								__ERROR__( "MasterWorker", "dispatch", "Failed to register with coordinator." );
								break;
							case PROTO_MAGIC_LOADING_STATS:
								this->protocol.parseLoadStatsHeader( loadStatsHeader, buffer.data, buffer.size );
								buffer.data += PROTO_LOAD_STATS_SIZE;
								buffer.size -= PROTO_LOAD_STATS_SIZE;

								// parse the loading stats and merge with existing stats
								LOCK( &master->overloadedSlave.lock );
								master->overloadedSlave.slaveSet.clear();
								this->protocol.parseLoadingStats( loadStatsHeader, getLatency, setLatency, master->overloadedSlave.slaveSet, buffer.data, buffer.size );
								UNLOCK( &master->overloadedSlave.lock );
								master->mergeSlaveCumulativeLoading( &getLatency, &setLatency );

								buffer.data -= PROTO_LOAD_STATS_SIZE;
								buffer.size += PROTO_LOAD_STATS_SIZE;

								getLatency.needsDelete = false;
								setLatency.needsDelete = false;
								getLatency.clear();
								setLatency.clear();

								break;
							default:
								__ERROR__( "MasterWorker", "dispatch", "Invalid magic code from coordinator." );
								break;
						}
						break;
					case PROTO_OPCODE_SYNC:
						if ( header.magic == PROTO_MAGIC_REMAPPING ) {
							if ( ! this->protocol.parseRemappingRecordHeader( remappingRecordHeader, buffer.data, buffer.size ) ) {
								__ERROR__( "CoordinatorWorker", "dispatch", "Invalid remapping record protocol header." );
								goto quit_1;
							}
							// start parsing the remapping records
							offset = PROTO_REMAPPING_RECORD_SIZE;
							RemappingRecordMap *map = MasterWorker::remappingRecords;
							for ( count = 0; offset < ( size_t ) buffer.size && count < remappingRecordHeader.remap; offset += bytes ) {
								if ( ! this->protocol.parseSlaveSyncRemapHeader( slaveSyncRemapHeader, bytes, buffer.data, buffer.size - offset, offset ) )
									break;
								count++;

								Key key;
								key.set ( slaveSyncRemapHeader.keySize, slaveSyncRemapHeader.key );

								RemappingRecord remappingRecord;
								remappingRecord.set( slaveSyncRemapHeader.listId, slaveSyncRemapHeader.chunkId, 0 );

								if ( slaveSyncRemapHeader.opcode == 0 ) { // remove record
									map->erase( key, remappingRecord );
								} else if ( slaveSyncRemapHeader.opcode == 1 ) { // add record
									map->insert( key, remappingRecord );
								}
							}
							//map->print();
						} else {
							__ERROR__( "MasterWorker", "dispatch", "Invalid magic code from coordinator." );
						}
						break;
					default:
						__ERROR__( "MasterWorker", "dispatch", "Invalid opcode from coordinator." );
						break;
				}
			}
quit_1:
			buffer.data += header.length;
			buffer.size -= header.length;
		}

		if ( connected ) event.socket->done();
	}
	if ( ! connected )
		__ERROR__( "MasterWorker", "dispatch", "The coordinator is disconnected." );
}

void MasterWorker::dispatch( MasterEvent event ) {
}

void MasterWorker::dispatch( SlaveEvent event ) {
	bool connected, isSend;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;

	switch( event.type ) {
		case SLAVE_EVENT_TYPE_REGISTER_REQUEST:
			buffer.data = this->protocol.reqRegisterSlave(
				buffer.size,
				MasterWorker::idGenerator->nextVal( this->workerId ),
				event.message.address.addr,
				event.message.address.port
			);
			isSend = true;
			break;
		case SLAVE_EVENT_TYPE_SEND:
			event.message.send.packet->read( buffer.data, buffer.size );
			isSend = true;
			break;
		case SLAVE_EVENT_TYPE_PENDING:
			isSend = false;
			break;
		default:
			return;
	}

	if ( isSend ) {
		ret = event.socket->send( buffer.data, buffer.size, connected );
		if ( ret != ( ssize_t ) buffer.size )
			__ERROR__( "MasterWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );

		if ( event.type == SLAVE_EVENT_TYPE_SEND ) {
			MasterWorker::packetPool->free( event.message.send.packet );
			// fprintf( stderr, "- After free(): " );
			// MasterWorker::packetPool->print( stderr );
		}
	} else {
		// Parse responses from slaves
		ProtocolHeader header;
		WORKER_RECEIVE_FROM_EVENT_SOCKET();
		while ( buffer.size > 0 ) {
			WORKER_RECEIVE_WHOLE_MESSAGE_FROM_EVENT_SOCKET( "MasterWorker" );

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_SLAVE ) {
				__ERROR__( "MasterWorker", "dispatch", "Invalid message source from slave." );
			} else {
				bool success;
				switch( header.magic ) {
					case PROTO_MAGIC_RESPONSE_SUCCESS:
						success = true;
						break;
					case PROTO_MAGIC_RESPONSE_FAILURE:
						success = false;
						break;
					default:
						__ERROR__( "MasterWorker", "dispatch", "Invalid magic code from slave." );
						goto quit_1;
				}

				event.id = header.id;
				switch( header.opcode ) {
					case PROTO_OPCODE_REGISTER:
						if ( success ) {
							event.socket->registered = true;
						} else {
							__ERROR__( "MasterWorker", "dispatch", "Failed to register with slave." );
						}
						break;
					case PROTO_OPCODE_GET:
						this->handleGetResponse( event, success, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_SET:
						this->handleSetResponse( event, success, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_REMAPPING_LOCK:
						this->handleRemappingSetLockResponse( event, success, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_REMAPPING_SET:
						this->handleRemappingSetResponse( event, success, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_UPDATE:
						this->handleUpdateResponse( event, success, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_DELETE:
						this->handleDeleteResponse( event, success, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_REDIRECT_GET:
					case PROTO_OPCODE_REDIRECT_UPDATE:
					case PROTO_OPCODE_REDIRECT_DELETE:
						this->handleRedirectedResponse( event, buffer.data, buffer.size, header.opcode );
						break;
					default:
						__ERROR__( "MasterWorker", "dispatch", "Invalid opcode from slave." );
						goto quit_1;
				}
			}
quit_1:
			buffer.data += header.length;
			buffer.size -= header.length;
		}
		if ( connected ) event.socket->done();
	}
	if ( ! connected )
		__ERROR__( "MasterWorker", "dispatch", "The slave is disconnected." );
}

SlaveSocket *MasterWorker::getSlaves( char *data, uint8_t size, uint32_t &listId, uint32_t &chunkId ) {
	SlaveSocket *ret;

	// Search to see if this key is remapped
	Key key;
	key.set( size, data );
	RemappingRecord record;
	bool found = MasterWorker::remappingRecords->find( key, &record );
	found = ( found && ! Master::getInstance()->config.master.remap.forceNoCacheRecords );
	if ( found ) { // remapped keys
		//fprintf( stderr, "Redirect request from list=%u chunk=%u to list=%u chunk=%u\n", listId, chunkId, record.listId, record.chunkId);
		listId = record.listId;
		chunkId = record.chunkId;
		this->paritySlaveSockets = MasterWorker::stripeList->get(
			listId,
			this->paritySlaveSockets,
			this->dataSlaveSockets
		);
		ret = this->dataSlaveSockets[ chunkId ];
	} else { // non-remapped keys
		listId = MasterWorker::stripeList->get(
			data, ( size_t ) size,
			this->dataSlaveSockets,
			this->paritySlaveSockets,
			&chunkId, false
		);
		ret = *this->dataSlaveSockets;
	}

	return ret->ready() ? ret : 0;
}

SlaveSocket *MasterWorker::getSlaves( char *data, uint8_t size, uint32_t &listId, uint32_t &chunkId, uint32_t &newChunkId ) {
	SlaveSocket *ret = this->getSlaves( data, size, listId, chunkId );
	newChunkId = chunkId;
	if ( Master::getInstance()->remapMsgHandler.useRemappingFlow() ) {
		// Perform degraded operation
		BasicRemappingScheme::getDegradedOpTarget(
			listId, chunkId, newChunkId,
			MasterWorker::dataChunkCount,
			MasterWorker::parityChunkCount,
			this->dataSlaveSockets,
			this->paritySlaveSockets
		);
	}
	// Always return the original data slave socket
	return ret;
}

SlaveSocket *MasterWorker::getSlaves( char *data, uint8_t size, uint32_t &originalListId, uint32_t &originalChunkId, uint32_t &remappedListId, uint32_t &remappedChunkId ) {
	SlaveSocket *ret;

	// Determine original data slave
	originalListId = MasterWorker::stripeList->get(
		data, ( size_t ) size,
		this->dataSlaveSockets,
		this->paritySlaveSockets,
		&originalChunkId, true
	);

	ret = this->dataSlaveSockets[ originalChunkId ];

	// Determine remapped data slave
	BasicRemappingScheme::getRemapTarget(
		originalListId, originalChunkId,
		remappedListId, remappedChunkId,
		MasterWorker::dataChunkCount, MasterWorker::parityChunkCount,
		this->dataSlaveSockets, this->paritySlaveSockets
	);

	this->getSlaves( originalListId, originalChunkId );
	ret = this->dataSlaveSockets[ originalChunkId ];

	return ret->ready() ? ret : 0;
}

SlaveSocket *MasterWorker::getSlaves( uint32_t listId, uint32_t chunkId ) {
	SlaveSocket *ret;
	MasterWorker::stripeList->get( listId, this->paritySlaveSockets, this->dataSlaveSockets );
	ret = this->dataSlaveSockets[ chunkId ];
	return ret->ready() ? ret : 0;
}

bool MasterWorker::handleGetRequest( ApplicationEvent event, char *buf, size_t size ) {
	struct KeyHeader header;
	if ( ! this->protocol.parseKeyHeader( header, buf, size ) ) {
		__ERROR__( "MasterWorker", "handleGetRequest", "Invalid GET request." );
		return false;
	}
	__DEBUG__(
		BLUE, "MasterWorker", "handleGetRequest",
		"[GET] Key: %.*s (key size = %u).",
		( int ) header.keySize, header.key, header.keySize
	);

	uint32_t listId, chunkId, newChunkId;
	bool connected;
	SlaveSocket *socket;

	socket = this->getSlaves(
		header.key, header.keySize,
		listId, chunkId, newChunkId
	);
	if ( ! socket ) {
		Key key;
		key.set( header.keySize, header.key );
		event.resGet( event.socket, event.id, key, false );
		this->dispatch( event );
		return false;
	}

	struct {
		size_t size;
		char *data;
	} buffer;
	Key key;
	ssize_t sentBytes;
	uint32_t requestId = MasterWorker::idGenerator->nextVal( this->workerId );

	key.dup( header.keySize, header.key, ( void * ) event.socket );
	if ( ! MasterWorker::pending->insertKey( PT_APPLICATION_GET, event.id, ( void * ) event.socket, key ) ) {
		__ERROR__( "MasterWorker", "handleGetRequest", "Cannot insert into application GET pending map." );
	}

	if ( chunkId != newChunkId ) {
		////////// Degraded GET //////////
		DegradedLock degradedLock;

		printf( "[GET] Performing degraded operation on %u to %u...\n", chunkId, newChunkId );

		buffer.data = this->protocol.reqDegradedLock( buffer.size, requestId, listId, newChunkId, header.key, header.keySize );

		degradedLock.set( listId, newChunkId, key.size, key.data );

		if ( ! MasterWorker::pending->insertDegradedLock( PT_SLAVE_DEGRADED_LOCK, requestId, event.id, ( void * ) socket, degradedLock ) ) {
			__ERROR__( "MasterWorker", "handleGetRequest", "Cannot insert into slave degraded lock pending map." );
		}

		// Send degraded lock request
		sentBytes = socket->send( buffer.data, buffer.size, connected );
		if ( sentBytes != ( ssize_t ) buffer.size ) {
			__ERROR__( "MasterWorker", "handleGetRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
			return false;
		}
	} else {
		////////// Normal GET //////////
		buffer.data = this->protocol.reqGet( buffer.size, requestId, header.key, header.keySize );

		key.ptr = ( void * ) socket;
		if ( ! MasterWorker::pending->insertKey( PT_SLAVE_GET, requestId, event.id, ( void * ) socket, key ) ) {
			__ERROR__( "MasterWorker", "handleGetRequest", "Cannot insert into slave GET pending map." );
		}

		// Mark the time when request is sent
		MasterWorker::pending->recordRequestStartTime( PT_SLAVE_GET, requestId, event.id, ( void * ) socket, socket->getAddr() );

		// Send GET request
		sentBytes = socket->send( buffer.data, buffer.size, connected );
		if ( sentBytes != ( ssize_t ) buffer.size ) {
			__ERROR__( "MasterWorker", "handleGetRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
			return false;
		}
	}

	return true;
}

bool MasterWorker::handleSetRequest( ApplicationEvent event, char *buf, size_t size ) {
	struct KeyValueHeader header;
	if ( ! this->protocol.parseKeyValueHeader( header, buf, size ) ) {
		__ERROR__( "MasterWorker", "handleSetRequest", "Invalid SET request." );
		return false;
	}
	__DEBUG__(
		BLUE, "MasterWorker", "handleSetRequest",
		"[SET] Key: %.*s (key size = %u); Value: (value size = %u)",
		( int ) header.keySize, header.key, header.keySize, header.valueSize
	);
	MasterWorker::counter->increaseNormal();

	uint32_t listId, chunkId;
	bool connected;
	ssize_t sentBytes;
	SlaveSocket *socket;

	socket = this->getSlaves(
		header.key, header.keySize,
		listId, chunkId
	);

	if ( ! socket ) {
		Key key;
		key.set( header.keySize, header.key );
		event.resSet( event.socket, event.id, key, false, false );
		this->dispatch( event );
		MasterWorker::counter->decreaseNormal();
		Master::getInstance()->remapMsgHandler.ackRemap( MasterWorker::counter->getNormal(), MasterWorker::counter->getRemapping() );
		return false;
	}

	struct {
		size_t size;
		char *data;
	} buffer;
	Key key;
	uint32_t requestId = MasterWorker::idGenerator->nextVal( this->workerId );

#ifdef MASTER_WORKER_SEND_REPLICAS_PARALLEL
	Packet *packet = 0;
	if ( MasterWorker::parityChunkCount ) {
		packet = MasterWorker::packetPool->malloc();
		packet->setReferenceCount( 1 + MasterWorker::parityChunkCount );
		buffer.data = packet->data;
		this->protocol.reqSet( buffer.size, requestId, header.key, header.keySize, header.value, header.valueSize, buffer.data );
		packet->size = buffer.size;
	} else {
		buffer.data = this->protocol.reqSet( buffer.size, requestId, header.key, header.keySize, header.value, header.valueSize );
	}
#else
	buffer.data = this->protocol.reqSet( buffer.size, requestId, header.key, header.keySize, header.value, header.valueSize );
#endif

	key.dup( header.keySize, header.key, ( void * ) event.socket );

	if ( ! MasterWorker::pending->insertKey( PT_APPLICATION_SET, event.id, ( void * ) event.socket, key ) ) {
		__ERROR__( "MasterWorker", "handleSetRequest", "Cannot insert into application SET pending map." );
	}

	for ( uint32_t i = 0; i < MasterWorker::parityChunkCount + 1; i++ ) {
		key.ptr = ( void * )( i == 0 ? socket : this->paritySlaveSockets[ i - 1 ] );
		if ( ! MasterWorker::pending->insertKey(
			PT_SLAVE_SET, requestId, event.id,
			( void * )( i == 0 ? socket : this->paritySlaveSockets[ i - 1 ] ),
			key
		) ) {
			__ERROR__( "MasterWorker", "handleSetRequest", "Cannot insert into slave SET pending map." );
		}
	}

	// Send SET requests
	if ( MasterWorker::parityChunkCount ) {
		for ( uint32_t i = 0; i < MasterWorker::parityChunkCount; i++ ) {
			// Mark the time when request is sent
			MasterWorker::pending->recordRequestStartTime(
				PT_SLAVE_SET, requestId, event.id,
				( void * ) this->paritySlaveSockets[ i ],
				this->paritySlaveSockets[ i ]->getAddr()
			);

#ifdef MASTER_WORKER_SEND_REPLICAS_PARALLEL
			SlaveEvent slaveEvent;
			slaveEvent.send( this->paritySlaveSockets[ i ], packet );
			MasterWorker::eventQueue->prioritizedInsert( slaveEvent );
		}

		MasterWorker::pending->recordRequestStartTime( PT_SLAVE_SET, requestId, event.id, ( void * ) socket, socket->getAddr() );
		SlaveEvent slaveEvent;
		slaveEvent.send( socket, packet );
		this->dispatch( slaveEvent );
#else
			sentBytes = this->paritySlaveSockets[ i ]->send( buffer.data, buffer.size, connected );
			if ( sentBytes != ( ssize_t ) buffer.size ) {
				__ERROR__( "MasterWorker", "handleSetRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
			}
		}

		MasterWorker::pending->recordRequestStartTime( PT_SLAVE_SET, requestId, event.id, ( void * ) socket, socket->getAddr() );
		sentBytes = socket->send( buffer.data, buffer.size, connected );
		if ( sentBytes != ( ssize_t ) buffer.size ) {
			__ERROR__( "MasterWorker", "handleSetRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
			MasterWorker::counter->decreaseNormal();
			Master::getInstance()->remapMsgHandler.ackRemap( MasterWorker::counter->getNormal(), MasterWorker::counter->getRemapping() );
			return false;
		}
#endif
	} else {
		MasterWorker::pending->recordRequestStartTime( PT_SLAVE_SET, requestId, event.id, ( void * ) socket, socket->getAddr() );
		sentBytes = socket->send( buffer.data, buffer.size, connected );
		if ( sentBytes != ( ssize_t ) buffer.size ) {
			__ERROR__( "MasterWorker", "handleSetRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
			MasterWorker::counter->decreaseNormal();
			Master::getInstance()->remapMsgHandler.ackRemap( MasterWorker::counter->getNormal(), MasterWorker::counter->getRemapping() );
			return false;
		}
	}

	MasterWorker::counter->decreaseNormal();
	Master::getInstance()->remapMsgHandler.ackRemap( MasterWorker::counter->getNormal(), MasterWorker::counter->getRemapping() );
	return true;
}

bool MasterWorker::handleUpdateRequest( ApplicationEvent event, char *buf, size_t size ) {
	struct KeyValueUpdateHeader header;
	if ( ! this->protocol.parseKeyValueUpdateHeader( header, true, buf, size ) ) {
		__ERROR__( "MasterWorker", "handleUpdateRequest", "Invalid UPDATE request." );
		return false;
	}
	__DEBUG__(
		BLUE, "MasterWorker", "handleUpdateRequest",
		"[UPDATE] Key: %.*s (key size = %u); Value: (offset = %u, value update size = %u)",
		( int ) header.keySize, header.key, header.keySize,
		header.valueUpdateOffset, header.valueUpdateSize
	);

	uint32_t listId, chunkId, newChunkId;
	bool connected;
	SlaveSocket *socket;

	socket = this->getSlaves(
		header.key, header.keySize,
		listId, chunkId, newChunkId
	);

	if ( ! socket ) {
		KeyValueUpdate keyValueUpdate;
		keyValueUpdate.set( header.keySize, header.key, event.socket );
		keyValueUpdate.offset = header.valueUpdateOffset;
		keyValueUpdate.length = header.valueUpdateSize;
		event.resUpdate( event.socket, event.id, keyValueUpdate, false, false );
		this->dispatch( event );
		return false;
	}

	struct {
		size_t size;
		char *data;
	} buffer;
	KeyValueUpdate keyValueUpdate;
	ssize_t sentBytes;
	uint32_t requestId = MasterWorker::idGenerator->nextVal( this->workerId );

	if ( chunkId != newChunkId ) {
		////////// Degraded UPDATE //////////
		printf( "[UPDATE] Performing degraded operation on %u to %u...\n", chunkId, newChunkId );
	} else {
		////////// Normal UPDATE //////////
		buffer.data = this->protocol.reqUpdate(
			buffer.size, requestId,
			header.key, header.keySize,
			header.valueUpdate, header.valueUpdateOffset, header.valueUpdateSize
		);

		char* valueUpdate = new char [ header.valueUpdateSize ];
		memcpy( valueUpdate, header.valueUpdate, header.valueUpdateSize );
		keyValueUpdate.dup( header.keySize, header.key, valueUpdate );
		keyValueUpdate.offset = header.valueUpdateOffset;
		keyValueUpdate.length = header.valueUpdateSize;

		if ( ! MasterWorker::pending->insertKeyValueUpdate( PT_APPLICATION_UPDATE, event.id, ( void * ) event.socket, keyValueUpdate ) ) {
			__ERROR__( "MasterWorker", "handleUpdateRequest", "Cannot insert into application UPDATE pending map." );
		}

		keyValueUpdate.ptr = ( void * ) socket;
		if ( ! MasterWorker::pending->insertKeyValueUpdate( PT_SLAVE_UPDATE, requestId, event.id, ( void * ) socket, keyValueUpdate ) ) {
			__ERROR__( "MasterWorker", "handleUpdateRequest", "Cannot insert into slave UPDATE pending map." );
		}

		// Send UPDATE request
		sentBytes = socket->send( buffer.data, buffer.size, connected );
		if ( sentBytes != ( ssize_t ) buffer.size ) {
			__ERROR__( "MasterWorker", "handleUpdateRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
			return false;
		}
	}

	return true;
}

bool MasterWorker::handleDeleteRequest( ApplicationEvent event, char *buf, size_t size ) {
	struct KeyHeader header;
	if ( ! this->protocol.parseKeyHeader( header, buf, size ) ) {
		__ERROR__( "MasterWorker", "handleDeleteRequest", "Invalid DELETE request." );
		return false;
	}
	__DEBUG__(
		BLUE, "MasterWorker", "handleDeleteRequest",
		"[DELETE] Key: %.*s (key size = %u).",
		( int ) header.keySize, header.key, header.keySize
	);

	uint32_t listId, chunkId, newChunkId;
	bool connected;
	SlaveSocket *socket;

	socket = this->getSlaves(
		header.key, header.keySize,
		listId, chunkId, newChunkId
	);

	if ( ! socket ) {
		Key key;
		key.set( header.keySize, header.key );
		event.resDelete( event.socket, event.id, key, false, false );
		this->dispatch( event );
		return false;
	}

	struct {
		size_t size;
		char *data;
	} buffer;
	Key key;
	ssize_t sentBytes;
	uint32_t requestId = MasterWorker::idGenerator->nextVal( this->workerId );

	if ( chunkId != newChunkId ) {
		////////// Degraded DELETE //////////
		printf( "[DELETE] Performing degraded operation on %u to %u...\n", chunkId, newChunkId );
	} else {
		////////// Normal DELETE //////////
		buffer.data = this->protocol.reqDelete(
			buffer.size, requestId,
			header.key, header.keySize
		);

		key.dup( header.keySize, header.key, ( void * ) event.socket );
		if ( ! MasterWorker::pending->insertKey( PT_APPLICATION_DEL, event.id, ( void * ) event.socket, key ) ) {
			__ERROR__( "MasterWorker", "handleDeleteRequest", "Cannot insert into application DELETE pending map." );
		}

		key.ptr = ( void * ) socket;
		if ( ! MasterWorker::pending->insertKey( PT_SLAVE_DEL, requestId, event.id, ( void * ) socket, key ) ) {
			__ERROR__( "MasterWorker", "handleDeleteRequest", "Cannot insert into slave DELETE pending map." );
		}

		// Send DELETE requests
		sentBytes = socket->send( buffer.data, buffer.size, connected );
		if ( sentBytes != ( ssize_t ) buffer.size ) {
			__ERROR__( "MasterWorker", "handleDeleteRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
			return false;
		}
	}

	return true;
}

bool MasterWorker::handleRemappingSetRequest( ApplicationEvent event, char *buf, size_t size ) {
	struct KeyValueHeader header;
	if ( ! this->protocol.parseKeyValueHeader( header, buf, size ) ) {
		__ERROR__( "MasterWorker", "handleRemappingSetRequest", "Invalid SET request." );
		return false;
	}
	__DEBUG__(
		BLUE, "MasterWorker", "handleRemappingSetRequest",
		"[REMAPPING_SET] Key: %.*s (key size = %u); Value: (value size = %u)",
		( int ) header.keySize, header.key, header.keySize, header.valueSize
	);

	uint32_t originalListId, originalChunkId, remappedListId, remappedChunkId;
	bool connected;
	ssize_t sentBytes;
	SlaveSocket *socket;

	socket = this->getSlaves(
		header.key, header.keySize,
		originalListId, originalChunkId,
		remappedListId, remappedChunkId
	);

#define NO_REMAPPING ( originalListId == remappedListId && originalChunkId == remappedChunkId )
	if ( NO_REMAPPING )
		MasterWorker::counter->increaseLockOnly();
	else {
		// fprintf(
		// 	stderr, "remap from (%u, %u) to (%u, %u) for key: %.*s\n",
		// 	originalListId, originalChunkId,
		// 	remappedListId, remappedChunkId,
		// 	header.keySize, header.key
		// );
		MasterWorker::counter->increaseRemapping();
	}

	if ( ! socket ) {
		Key key;
		key.set( header.keySize, header.key );
		event.resSet( event.socket, event.id, key, false, false );
		this->dispatch( event );
		if ( NO_REMAPPING )
			MasterWorker::counter->decreaseLockOnly();
		else
			MasterWorker::counter->decreaseRemapping();
		Master::getInstance()->remapMsgHandler.ackRemap( MasterWorker::counter->getNormal(), MasterWorker::counter->getRemapping() );
		return false;
	}

	struct {
		size_t size;
		char *data;
	} buffer;
	Key key;
	uint32_t requestId = MasterWorker::idGenerator->nextVal( this->workerId );

	key.dup( header.keySize, header.key, ( void * ) event.socket );

	// Insert the key into application SET pending map
	if ( ! MasterWorker::pending->insertKey( PT_APPLICATION_SET, event.id, ( void * ) event.socket, key ) ) {
		__ERROR__( "MasterWorker", "handleRemappingSetRequest", "Cannot insert into application SET pending map." );
	}

	// Check whether the lock should be acquired first
	if ( originalListId == remappedListId && originalChunkId == remappedChunkId ) {
		// Send directly to the data slave and let it forward to the parity slaves
		buffer.data = this->protocol.reqRemappingSet(
			buffer.size, requestId,
			originalListId, originalChunkId,
			MasterWorker::parityChunkCount > 0, /* needsForwarding */
			header.key, header.keySize,
			header.value, header.valueSize
		);

		if ( ! MasterWorker::pending->insertKey( PT_SLAVE_SET, requestId, event.id, ( void * ) socket, key ) ) {
			__ERROR__( "MasterWorker", "handleRemappingSetRequest", "Cannot insert into slave SET pending map." );
		}
		MasterWorker::pending->recordRequestStartTime( PT_SLAVE_SET, requestId, event.id, ( void * ) socket, socket->getAddr() );
		// Send SET request
		sentBytes = socket->send( buffer.data, buffer.size, connected );
		if ( sentBytes != ( ssize_t ) buffer.size ) {
			__ERROR__( "MasterWorker", "handleRemappingSetRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
			return false;
		}
	} else {
		// Need to buffer the key-value pair in a packet
		Value *value = new Value(); // Note: Use an Key object to store the value
		value->dup( header.valueSize, header.value );

		// Insert the remapping record into master REMAPPING_SET pending map
		RemappingRecord remappingRecord( remappedListId, remappedChunkId, value );
		if ( ! MasterWorker::pending->insertRemappingRecord( PT_SLAVE_REMAPPING_SET, requestId, event.id, ( void * ) socket, remappingRecord ) ) {
			__ERROR__( "MasterWorker", "handleRemappingSetRequest", "Cannot insert into slave REMAPPING_SET pending map." );
		}

		// Acquire remapping lock
		buffer.data = this->protocol.reqRemappingSetLock(
			buffer.size, requestId,
			remappedListId, remappedChunkId,
			header.key, header.keySize
		);

		sentBytes = socket->send( buffer.data, buffer.size, connected );
		if ( sentBytes != ( ssize_t ) buffer.size ) {
			__ERROR__( "MasterWorker", "handleRemappingSetRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
			if ( NO_REMAPPING )
				MasterWorker::counter->decreaseLockOnly();
			else
				MasterWorker::counter->decreaseRemapping();
			Master::getInstance()->remapMsgHandler.ackRemap( MasterWorker::counter->getNormal(), MasterWorker::counter->getRemapping() );
			return false;
		}
	}

#undef NO_REMAPPING

	Master::getInstance()->remapMsgHandler.ackRemap( MasterWorker::counter->getNormal(), MasterWorker::counter->getRemapping() );

	return true;
}

bool MasterWorker::handleGetResponse( SlaveEvent event, bool success, char *buf, size_t size ) {
	Key key;
	KeyValue keyValue;
	if ( success ) {
		struct KeyValueHeader header;
		if ( this->protocol.parseKeyValueHeader( header, buf, size ) ) {
			key.set( header.keySize, header.key, ( void * ) event.socket );
			keyValue.dup( header.key, header.keySize, header.value, header.valueSize );
		} else {
			__ERROR__( "MasterWorker", "handleGetResponse", "Invalid GET response." );
			return false;
		}
	} else {
		struct KeyHeader header;
		if ( this->protocol.parseKeyHeader( header, buf, size ) ) {
			key.set( header.keySize, header.key, ( void * ) event.socket );
		} else {
			__ERROR__( "MasterWorker", "handleGetResponse", "Invalid GET response." );
			return false;
		}
	}

	ApplicationEvent applicationEvent;
	PendingIdentifier pid;

	if ( ! MasterWorker::pending->eraseKey( PT_SLAVE_GET, event.id, event.socket, &pid, &key ) ) {
		__ERROR__( "MasterWorker", "handleGetResponse", "Cannot find a pending slave GET request that matches the response. This message will be discarded (key = %.*s).", key.size, key.data );
		if ( success ) keyValue.free();
		return false;
	}

	// Mark the elapse time as latency
	Master* master = Master::getInstance();
	if ( master->config.master.loadingStats.updateInterval > 0 ) {
		struct timespec elapsedTime;
		RequestStartTime rst;

		if ( ! MasterWorker::pending->eraseRequestStartTime( PT_SLAVE_GET, pid.id, ( void * ) event.socket, elapsedTime, 0, &rst ) ) {
			__ERROR__( "MasterWorker", "handleGetResponse", "Cannot find a pending stats GET request that matches the response." );
		} else {
			int index = -1;
			LOCK( &master->slaveLoading.lock );
			std::set<Latency> *latencyPool = master->slaveLoading.past.get.get( rst.addr, &index );
			// init. the set if it is not there
			if ( index == -1 ) {
				master->slaveLoading.past.get.set( rst.addr, new std::set<Latency>() );
			}
			// insert the latency to the set
			// TODO use time when Response came, i.e. event created for latency cal.
			Latency latency = Latency ( elapsedTime );
			if ( index == -1 )
				latencyPool = master->slaveLoading.past.get.get( rst.addr );
			latencyPool->insert( latency );
			UNLOCK( &master->slaveLoading.lock );
		}
	}

	key.ptr = 0;
	if ( ! MasterWorker::pending->eraseKey( PT_APPLICATION_GET, pid.parentId, 0, &pid, &key ) ) {
		__ERROR__( "MasterWorker", "handleGetResponse", "Cannot find a pending application GET request that matches the response. This message will be discarded (key = %.*s).", key.size, key.data );
		if ( success ) keyValue.free();
		return false;
	}

	if ( success ) {
		key.free();
		applicationEvent.resGet( ( ApplicationSocket * ) key.ptr, pid.id, keyValue );
	} else {
		applicationEvent.resGet( ( ApplicationSocket * ) key.ptr, pid.id, key );
	}
	MasterWorker::eventQueue->insert( applicationEvent );
	return true;
}

bool MasterWorker::handleSetResponse( SlaveEvent event, bool success, char *buf, size_t size ) {
	struct KeyHeader header;
	if ( ! this->protocol.parseKeyHeader( header, buf, size ) ) {
		__ERROR__( "MasterWorker", "handleSetResponse", "Invalid SET response." );
		return false;
	}
	__DEBUG__(
		BLUE, "MasterWorker", "handleSetResponse",
		"[SET] Key: %.*s (key size = %u)",
		( int ) header.keySize, header.key, header.keySize
	);

	int pending;
	ApplicationEvent applicationEvent;
	PendingIdentifier pid;
	Key key;

	if ( ! MasterWorker::pending->eraseKey( PT_SLAVE_SET, event.id, event.socket, &pid, &key, true, false ) ) {
		UNLOCK( &MasterWorker::pending->slaves.setLock );
		__ERROR__( "MasterWorker", "handleSetResponse", "Cannot find a pending slave SET request that matches the response. This message will be discarded. (ID: %u)", event.id );
		return false;
	}
	// Check pending slave SET requests
	pending = MasterWorker::pending->count( PT_SLAVE_SET, pid.id, false, true );

	// Mark the elapse time as latency
	Master* master = Master::getInstance();
	if ( master->config.master.loadingStats.updateInterval > 0 ) {
		struct timespec elapsedTime;
		RequestStartTime rst;

		if ( ! MasterWorker::pending->eraseRequestStartTime( PT_SLAVE_SET, pid.id, ( void * ) event.socket, elapsedTime, 0, &rst ) ) {
			__ERROR__( "MasterWorker", "handleSetResponse", "Cannot find a pending stats SET request that matches the response." );
		} else {
			int index = -1;
			LOCK( &master->slaveLoading.lock );
			std::set<Latency> *latencyPool = master->slaveLoading.past.set.get( rst.addr, &index );
			// init. the set if it is not there
			if ( index == -1 ) {
				master->slaveLoading.past.set.set( rst.addr, new std::set<Latency>() );
			}
			// insert the latency to the set
			// TODO use time when Response came, i.e. event created for latency cal.
			Latency latency = Latency ( elapsedTime );
			if ( index == -1 )
				latencyPool = master->slaveLoading.past.set.get( rst.addr );
			latencyPool->insert( latency );
			UNLOCK( &master->slaveLoading.lock );
		}
	}

	// __ERROR__( "MasterWorker", "handleSetResponse", "Pending slave SET requests = %d.", pending );

	if ( pending == 0 ) {
		// Only send application SET response when the number of pending slave SET requests equal 0
		if ( ! MasterWorker::pending->eraseKey( PT_APPLICATION_SET, pid.parentId, 0, &pid, &key ) ) {
			__ERROR__( "MasterWorker", "handleSetResponse", "Cannot find a pending application SET request that matches the response. This message will be discarded." );
			return false;
		}

		applicationEvent.resSet( ( ApplicationSocket * ) key.ptr, pid.id, key, success );
		MasterWorker::eventQueue->insert( applicationEvent );
	}
	return true;
}

bool MasterWorker::handleUpdateResponse( SlaveEvent event, bool success, char *buf, size_t size ) {
	struct KeyValueUpdateHeader header;
	if ( ! this->protocol.parseKeyValueUpdateHeader( header, false, buf, size ) ) {
		__ERROR__( "MasterWorker", "handleUpdateResponse", "Invalid UPDATE Response." );
		return false;
	}
	__DEBUG__(
		BLUE, "MasterWorker", "handleUpdateResponse",
		"[UPDATE (%s)] Updated key: %.*s (key size = %u); update value size = %u at offset: %u.",
		success ? "Success" : "Fail",
		( int ) header.keySize, header.key, header.keySize,
		header.valueUpdateSize, header.valueUpdateOffset
	);

	KeyValueUpdate keyValueUpdate;
	ApplicationEvent applicationEvent;
	PendingIdentifier pid;

	// Find the cooresponding request
	if ( ! MasterWorker::pending->eraseKeyValueUpdate( PT_SLAVE_UPDATE, event.id, ( void * ) event.socket, &pid, &keyValueUpdate ) ) {
		__ERROR__( "MasterWorker", "handleUpdateResponse", "Cannot find a pending slave UPDATE request that matches the response. This message will be discarded. (ID: %u)", event.id );
		return false;
	}

	if ( ! MasterWorker::pending->eraseKeyValueUpdate( PT_APPLICATION_UPDATE, pid.parentId, 0, &pid, &keyValueUpdate ) ) {
		__ERROR__( "MasterWorker", "handleUpdateResponse", "Cannot find a pending application UPDATE request that matches the response. This message will be discarded." );
		return false;
	}

	// free the updated value
	delete [] ( ( char* )( keyValueUpdate.ptr) );

	applicationEvent.resUpdate( ( ApplicationSocket * ) pid.ptr, pid.id, keyValueUpdate, success );
	MasterWorker::eventQueue->insert( applicationEvent );

	return true;
}

bool MasterWorker::handleDeleteResponse( SlaveEvent event, bool success, char *buf, size_t size ) {
	struct KeyHeader header;
	if ( ! this->protocol.parseKeyHeader( header, buf, size ) ) {
		__ERROR__( "MasterWorker", "handleDeleteResponse", "Invalid DELETE Response." );
		return false;
	}

	ApplicationEvent applicationEvent;
	PendingIdentifier pid;
	Key key;

	if ( ! MasterWorker::pending->eraseKey( PT_SLAVE_DEL, event.id, ( void * ) event.socket, &pid, &key ) ) {
		__ERROR__( "MasterWorker", "handleDeleteResponse", "Cannot find a pending slave DELETE request that matches the response. This message will be discarded." );
		return false;
	}

	if ( ! MasterWorker::pending->eraseKey( PT_APPLICATION_DEL, pid.parentId, 0, &pid, &key ) ) {
		__ERROR__( "MasterWorker", "handleDeleteResponse", "Cannot find a pending application DELETE request that matches the response. This message will be discarded." );
		return false;
	}

	applicationEvent.resDelete( ( ApplicationSocket * ) key.ptr, pid.id, key, success );
	MasterWorker::eventQueue->insert( applicationEvent );

	// TODO remove remapping records

	return true;
}

bool MasterWorker::handleRedirectedResponse( SlaveEvent event, char *buf, size_t size, uint8_t opcode ) {
	struct RedirectHeader header;
	if ( ! this->protocol.parseRedirectHeader( header, buf, size ) ) {
		__ERROR__( "MasterWorker", "handleRedirectedRequest", "Invalid redirected request header." );
		return false;
	}
	__DEBUG__(
		BLUE, "MasterWorker", "handleRedirectedRequest",
		"[%s] Key: %.*s (key size = %u). Redirect to list=%u chunk=%u. ",
		opcode == PROTO_OPCODE_REDIRECT_UPDATE ? "UPDATE" :
		opcode == PROTO_OPCODE_REDIRECT_DELETE ? "DELETE" :
		opcode == PROTO_OPCODE_REDIRECT_GET ? "GET" : "UNKNOWN",
		( int ) header.keySize, header.key, header.keySize,
		header.listId, header.chunkId
	);

	uint32_t listId = header.listId, chunkId = header.chunkId;
	uint32_t requestId = event.id;
	bool connected;
	SlaveSocket *socket;

	struct {
		size_t size;
		char *data;
	} buffer;
	Key key;
	KeyValueUpdate keyValueUpdate;
	ssize_t sentBytes;

	ApplicationEvent applicationEvent;
	PendingIdentifier pid;

	struct timespec elapsedTime;

	// identify the code for each type of requests
	PendingType applicationPT = PT_APPLICATION_GET;
	PendingType slavePT = PT_SLAVE_GET;
	const char *opName = "GET";
	if ( opcode == PROTO_OPCODE_REDIRECT_UPDATE ) {
		applicationPT = PT_APPLICATION_UPDATE;
		slavePT = PT_SLAVE_UPDATE;
		opName = "UPDATE";
	} else if ( opcode == PROTO_OPCODE_REDIRECT_DELETE ) {
		applicationPT = PT_APPLICATION_DEL;
		slavePT = PT_SLAVE_DEL;
		opName = "DELETE";
	} else if ( opcode != PROTO_OPCODE_REDIRECT_GET ) {
		__ERROR__( "MasterWorker", "handleRedirectedRequest", "Invalid type of redirect request." );
		return false;
	}
	//fprintf( stderr, "handle redirected %s request on key [%s](%u)\n", opName, header.key, header.keySize );

	bool ret = false;

	// purge pending records on previous slave
	if ( slavePT == PT_SLAVE_GET || slavePT == PT_SLAVE_DEL ) {
		ret = MasterWorker::pending->eraseKey( slavePT, event.id, event.socket, &pid, &key );
	} else {
		ret = MasterWorker::pending->eraseKeyValueUpdate( slavePT, event.id, event.socket, &pid, &keyValueUpdate );
	}
	if ( ! ret ) {
		__ERROR__( "MasterWorker", "handleRedirectedRequest", "Cannot find a pending slave %s request that matches the response. This message will be discarded (key = %.*s).", opName, key.size, key.data );
		return false;
	}

	if ( slavePT == PT_SLAVE_GET )
		MasterWorker::pending->eraseRequestStartTime( slavePT, pid.id, ( void * ) event.socket, elapsedTime );

	// abort request if no application is pending for result
	if ( slavePT == PT_SLAVE_GET || slavePT == PT_SLAVE_DEL ) {
		ret = MasterWorker::pending->findKey( applicationPT, pid.parentId, 0, &key );
	} else {
		ret = MasterWorker::pending->findKeyValueUpdate( applicationPT, pid.parentId, 0, &keyValueUpdate );
	}
	if ( ! ret ) {
		__ERROR__( "MasterWorker", "handleRedirectedRequest", "Cannot find a pending application %s request that matches the response. This message will be discarded (key = %.*s).", opName, key.size, key.data );
		return false;
	}

	socket = this->getSlaves( listId, chunkId );

	if ( ! socket ) {
		ApplicationSocket *applicationSocket = ( ApplicationSocket * ) key.ptr;
		key.set( header.keySize, header.key );
		applicationEvent.resGet( applicationSocket, event.id, key, false );
		this->dispatch( applicationEvent );
		return false;
	}

	if ( slavePT == PT_SLAVE_GET ) {
		buffer.data = this->protocol.reqGet( buffer.size, requestId, header.key, header.keySize );
	} else if ( slavePT == PT_SLAVE_UPDATE ) {
		buffer.data = this->protocol.reqUpdate( buffer.size, requestId, header.key, header.keySize, ( char * ) keyValueUpdate.ptr, keyValueUpdate.offset, keyValueUpdate.length );
	} else if ( slavePT == PT_SLAVE_DEL ) {
		buffer.data = this->protocol.reqDelete( buffer.size, requestId, header.key, header.keySize );
	}

	// add pending records on redirected slave
	if ( slavePT == PT_SLAVE_GET || slavePT == PT_SLAVE_DEL ) {
		ret = MasterWorker::pending->insertKey( slavePT, requestId, pid.parentId , ( void * ) socket, key );
	} else {
		ret = MasterWorker::pending->insertKeyValueUpdate( slavePT, requestId, pid.parentId, ( void * ) socket, keyValueUpdate );
	}
	if ( ! ret ) {
		__ERROR__( "MasterWorker", "handleRedirectedequest", "Cannot insert into slave %s pending map.", opName );
		return false;
	}

	// Mark the time when request is sent
	if ( slavePT == PT_SLAVE_GET )
		MasterWorker::pending->recordRequestStartTime( slavePT, requestId, event.id, ( void * ) socket, socket->getAddr() );

	// Send GET request
	sentBytes = socket->send( buffer.data, buffer.size, connected );
	if ( sentBytes != ( ssize_t ) buffer.size ) {
		__ERROR__( "MasterWorker", "handleRedirectedGetRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
		return false;
	}

	// add the remapping record to cache
	key.set( header.keySize, header.key );
	RemappingRecord record ( listId, chunkId );
	MasterWorker::remappingRecords->insert( key, record );

	return true;
}

bool MasterWorker::handleRemappingSetLockResponse( SlaveEvent event, bool success, char *buf, size_t size ) {
	struct RemappingLockHeader header;
	if ( ! this->protocol.parseRemappingLockHeader( header, buf, size ) ) {
		__ERROR__( "MasterWorker", "handleRemappingSetLockResponse", "Invalid REMAPPING_SET_LOCK Response." );
		// TODO is it possible to determine which counter to decrement? ..
		// MasterWorker::counter->decreaseLockOnly();
		MasterWorker::counter->decreaseRemapping();
		Master::getInstance()->remapMsgHandler.ackRemap( MasterWorker::counter->getNormal(), MasterWorker::counter->getRemapping() );
		return false;
	}
	__DEBUG__(
		BLUE, "MasterWorker", "handleRemappingSetLockResponse",
		"[REMAPPING_SET_LOCK (%s)] Key: %.*s (key size = %u); Remapped to (list ID: %u, chunk ID: %u)",
		success ? "Success" : "Fail",
		( int ) header.keySize, header.key, header.keySize, header.listId, header.chunkId
	);

	PendingIdentifier pid;
	RemappingRecord remappingRecord;

	if ( ! MasterWorker::pending->eraseRemappingRecord( PT_SLAVE_REMAPPING_SET, event.id, event.socket, &pid, &remappingRecord ) ) {
		__ERROR__( "MasterWorker", "handleRemappingSetLockResponse", "Cannot find a pending slave REMAPPING_SET_LOCK request that matches the response. This message will be discarded. (ID: %u)", event.id );
		MasterWorker::counter->decreaseRemapping();
		Master::getInstance()->remapMsgHandler.ackRemap( MasterWorker::counter->getNormal(), MasterWorker::counter->getRemapping() );
		return false;
	}

	if ( ! success ) {
		// TODO
		__ERROR__( "MasterWorker", "handleRemappingSetLockResponse", "TODO: Handle the case when the lock cannot be acquired." );
		MasterWorker::counter->decreaseRemapping();
		Master::getInstance()->remapMsgHandler.ackRemap( MasterWorker::counter->getNormal(), MasterWorker::counter->getRemapping() );
		return false;
	}

	SlaveSocket *socket;
	Key key;
	Value *value = ( Value * ) remappingRecord.ptr;

	socket = this->getSlaves( remappingRecord.listId, remappingRecord.chunkId );

	if ( ! socket ) {
		// TODO
		__ERROR__( "MasterWorker", "handleRemappingSetLockResponse", "Not yet implemented!" );
		MasterWorker::counter->decreaseRemapping();
		Master::getInstance()->remapMsgHandler.ackRemap( MasterWorker::counter->getNormal(), MasterWorker::counter->getRemapping() );
		return false;
	}

	if ( ! MasterWorker::pending->findKey( PT_APPLICATION_SET, pid.parentId, 0, &key ) ) {
		__ERROR__( "MasterWorker", "handleRemappingSetLockResponse", "Cannot find a pending application SET request that matches the response. This message will be discarded. (ID: %u)", event.id );
		MasterWorker::counter->decreaseRemapping();
		Master::getInstance()->remapMsgHandler.ackRemap( MasterWorker::counter->getNormal(), MasterWorker::counter->getRemapping() );
		return false;
	}

#undef NO_REMAPPING

	for ( uint32_t i = 0; i < MasterWorker::parityChunkCount + 1; i++ ) {
		key.ptr = ( void * )( i == 0 ? socket : this->paritySlaveSockets[ i - 1 ] );
		if ( ! MasterWorker::pending->insertKey(
			PT_SLAVE_SET, pid.id, pid.parentId,
			( void * )( i == 0 ? socket : this->paritySlaveSockets[ i - 1 ] ),
			key
		) ) {
			__ERROR__( "MasterWorker", "handleRemappingSetLockResponse", "Cannot insert into slave SET pending map." );
		}
	}

	// Prepare a packet buffer
	Packet *packet = MasterWorker::packetPool->malloc();
	size_t s;
	packet->setReferenceCount( 1 + MasterWorker::parityChunkCount );
	this->protocol.reqRemappingSet(
		s, pid.id,
		remappingRecord.listId, remappingRecord.chunkId, false,
		key.data, key.size,
		value->data, value->size,
		packet->data
	);
	packet->size = s;
	delete value;
	value = 0;

	// Send SET requests
	for ( uint32_t i = 0; i < MasterWorker::parityChunkCount; i++ ) {
		// Mark the time when request is sent
		MasterWorker::pending->recordRequestStartTime(
			PT_SLAVE_SET, pid.id, pid.parentId,
			( void * ) this->paritySlaveSockets[ i ],
			this->paritySlaveSockets[ i ]->getAddr()
		);

		SlaveEvent slaveEvent;
		slaveEvent.send( this->paritySlaveSockets[ i ], packet );
#ifdef MASTER_WORKER_SEND_REPLICAS_PARALLEL
		MasterWorker::eventQueue->prioritizedInsert( slaveEvent );
#else
		this->dispatch( slaveEvent );
#endif
	}

	MasterWorker::pending->recordRequestStartTime( PT_SLAVE_SET, pid.id, pid.parentId, ( void * ) socket, socket->getAddr() );

	SlaveEvent slaveEvent;
	slaveEvent.send( socket, packet );
	this->dispatch( slaveEvent );
	return true;
}

bool MasterWorker::handleRemappingSetResponse( SlaveEvent event, bool success, char *buf, size_t size ) {
	struct RemappingLockHeader header;
	if ( ! this->protocol.parseRemappingLockHeader( header, buf, size ) ) {
		__ERROR__( "MasterWorker", "handleRemappingSetResponse", "Invalid REMAPPING_SET Response." );
		return false;
	}
	__DEBUG__(
		BLUE, "MasterWorker", "handleRemappingSetResponse",
		"[REMAPPING_SET (%s)] Key: %.*s (key size = %u); list ID: %u, chunk ID: %u.",
		success ? "Success" : "Fail",
		( int ) header.keySize, header.key, header.keySize,
		header.listId, header.chunkId
	);

	// find the type of counter by comparing the remapped Ids to original Ids
	uint32_t originalListId, originalChunkId;
	originalListId = MasterWorker::stripeList->get(
		header.key, ( size_t ) header.keySize,
		0, 0, &originalChunkId, false
	);

#define NO_REMAPPING ( originalListId == header.listId && originalChunkId == header.chunkId )

	int pending;
	ApplicationEvent applicationEvent;
	PendingIdentifier pid;
	Key key;

	// Find the cooresponding request
	if ( ! MasterWorker::pending->eraseKey( PT_SLAVE_SET, event.id, ( void * ) event.socket, &pid, &key, true, false ) ) {
		UNLOCK( &MasterWorker::pending->slaves.setLock );
		__ERROR__( "MasterWorker", "handleRemappingSetResponse", "Cannot find a pending slave SET request that matches the response. This message will be discarded. (ID: %u)", event.id );
		__ERROR__(
			"MasterWorker", "handleRemappingSetResponse",
			"[REMAPPING_SET (%s)] Key: %.*s (key size = %u); list ID: %u, chunk ID: %u.",
			success ? "Success" : "Fail",
			( int ) header.keySize, header.key, header.keySize,
			header.listId, header.chunkId
		);
		// remapping SET ended
		if ( NO_REMAPPING )
			MasterWorker::counter->decreaseLockOnly();
		else
			MasterWorker::counter->decreaseRemapping();
		Master::getInstance()->remapMsgHandler.ackRemap( MasterWorker::counter->getNormal(), MasterWorker::counter->getRemapping() );

		return false;
	}
	// Check pending slave SET requests
	pending = MasterWorker::pending->count( PT_SLAVE_SET, pid.id, false, true );

	// Mark the elapse time as latency
	Master* master = Master::getInstance();
	if ( master->config.master.loadingStats.updateInterval > 0 ) {
		timespec elapsedTime;
		RequestStartTime rst;

		if ( ! MasterWorker::pending->eraseRequestStartTime( PT_SLAVE_SET, pid.id, ( void * ) event.socket, elapsedTime, 0, &rst ) ) {
			__ERROR__( "MasterWorker", "handleRemappingSetResponse", "Cannot find a pending stats SET request that matches the response." );
		} else {
			int index = -1;
			LOCK( &master->slaveLoading.lock );
			std::set<Latency> *latencyPool = master->slaveLoading.past.set.get( rst.addr, &index );
			// init. the set if it is not there
			if ( index == -1 ) {
				master->slaveLoading.past.set.set( rst.addr, new std::set<Latency>() );
			}
			// insert the latency to the set
			// TODO use time when Response came, i.e. event created for latency cal.
			Latency latency = Latency ( elapsedTime );
			if ( index == -1 )
				latencyPool = master->slaveLoading.past.set.get( rst.addr );
			latencyPool->insert( latency );
			UNLOCK( &master->slaveLoading.lock );
		}
	}

	// if ( pending ) {
	// 	__ERROR__( "MasterWorker", "handleRemappingSetResponse", "Pending slave REMAPPING_SET requests = %d (%sremapping).", pending, NO_REMAPPING ? "no " : "" );
	// }

	if ( pending == 0 ) {
		// Only send application SET response when the number of pending slave SET requests equal 0
		if ( ! MasterWorker::pending->eraseKey( PT_APPLICATION_SET, pid.parentId, 0, &pid, &key ) ) {
			__ERROR__( "MasterWorker", "handleRemappingSetResponse", "Cannot find a pending application SET request that matches the response. This message will be discarded." );
			return false;
		}

		applicationEvent.resSet( ( ApplicationSocket * ) key.ptr, pid.id, key, success );
		MasterWorker::eventQueue->insert( applicationEvent );

		// remapping SET ended
		if ( NO_REMAPPING )
			MasterWorker::counter->decreaseLockOnly();
		else {
			MasterWorker::counter->decreaseRemapping();

			// add a remaping record
			key.set( header.keySize, header.key );
			RemappingRecord record ( header.listId, header.chunkId );
			MasterWorker::remappingRecords->insert( key, record );
		}

		Master::getInstance()->remapMsgHandler.ackRemap( MasterWorker::counter->getNormal(), MasterWorker::counter->getRemapping() );
	}

#undef NO_REMAPPING
	return true;
}

void MasterWorker::free() {
	this->protocol.free();
	delete[] this->dataSlaveSockets;
	delete[] this->paritySlaveSockets;
}

void *MasterWorker::run( void *argv ) {
	MasterWorker *worker = ( MasterWorker * ) argv;
	WorkerRole role = worker->getRole();
	MasterEventQueue *eventQueue = MasterWorker::eventQueue;

#define MASTER_WORKER_EVENT_LOOP(_EVENT_TYPE_, _EVENT_QUEUE_) \
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
			// MASTER_WORKER_EVENT_LOOP(
			// 	MixedEvent,
			// 	eventQueue->mixed
			// );
		{
			MixedEvent event;
			bool ret;
			while( worker->getIsRunning() | ( ret = eventQueue->extractMixed( event ) ) ) {
				if ( ret )
					worker->dispatch( event );
			}
		}
			break;
		case WORKER_ROLE_APPLICATION:
			MASTER_WORKER_EVENT_LOOP(
				ApplicationEvent,
				eventQueue->separated.application
			);
			break;
		case WORKER_ROLE_COORDINATOR:
			MASTER_WORKER_EVENT_LOOP(
				CoordinatorEvent,
				eventQueue->separated.coordinator
			);
			break;
		case WORKER_ROLE_MASTER:
			MASTER_WORKER_EVENT_LOOP(
				MasterEvent,
				eventQueue->separated.master
			);
			break;
		case WORKER_ROLE_SLAVE:
			MASTER_WORKER_EVENT_LOOP(
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

bool MasterWorker::init() {
	Master *master = Master::getInstance();

	MasterWorker::idGenerator = &master->idGenerator;
	MasterWorker::dataChunkCount = master->config.global.coding.params.getDataChunkCount();
	MasterWorker::parityChunkCount = master->config.global.coding.params.getParityChunkCount();
	MasterWorker::pending = &master->pending;
	MasterWorker::eventQueue = &master->eventQueue;
	MasterWorker::stripeList = master->stripeList;
	MasterWorker::counter = &master->counter;
	MasterWorker::remapFlag = &master->remapFlag;
	MasterWorker::packetPool = &master->packetPool;
	return true;
}

bool MasterWorker::init( GlobalConfig &config, WorkerRole role, uint32_t workerId ) {
	this->protocol.init(
		Protocol::getSuggestedBufferSize(
			config.size.key,
			config.size.chunk
		),
		MasterWorker::parityChunkCount
	);
	this->dataSlaveSockets = new SlaveSocket*[ MasterWorker::dataChunkCount ];
	this->paritySlaveSockets = new SlaveSocket*[ MasterWorker::parityChunkCount ];
	this->role = role;
	this->workerId = workerId;
	return role != WORKER_ROLE_UNDEFINED;
}

bool MasterWorker::start() {
	this->isRunning = true;
	if ( pthread_create( &this->tid, NULL, MasterWorker::run, ( void * ) this ) != 0 ) {
		__ERROR__( "MasterWorker", "start", "Cannot start worker thread." );
		return false;
	}
	return true;
}

void MasterWorker::stop() {
	this->isRunning = false;
}

void MasterWorker::print( FILE *f ) {
	char role[ 16 ];
	switch( this->role ) {
		case WORKER_ROLE_MIXED:
			strcpy( role, "Mixed" );
			break;
		case WORKER_ROLE_APPLICATION:
			strcpy( role, "Application" );
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
