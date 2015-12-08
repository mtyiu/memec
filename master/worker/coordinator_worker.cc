#include "worker.hh"
#include "../main/master.hh"

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
		case COORDINATOR_EVENT_TYPE_RESPONSE_SYNC_REMAPPING_RECORDS:
			buffer.data = this->protocol.resSyncRemappingRecords(
				buffer.size,
				event.id
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
				bool success;
				switch( header.magic ) {
					case PROTO_MAGIC_RESPONSE_SUCCESS:
						success = true;
						break;
					case PROTO_MAGIC_RESPONSE_FAILURE:
					default:
						success = false;
						break;
				}

				event.id = header.id;
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
								if ( ! this->protocol.parseSlaveSyncRemapHeader( slaveSyncRemapHeader, bytes, buffer.data, buffer.size, offset ) ) {
									__ERROR__( "CoordinatorWorker", "dispatch", "Failed to parse remapping record message. reading buffer of size %lu at %lu", buffer.size, offset );
									break;
								}
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
							event.resSyncRemappingRecords();
							this->dispatch( event );
							//map->print();
						} else {
							__ERROR__( "MasterWorker", "dispatch", "Invalid magic code from coordinator." );
						}
						break;
					case PROTO_OPCODE_DEGRADED_LOCK:
						this->handleDegradedLockResponse( event, success, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_REMAPPING_LOCK:
						switch( header.magic ) {
							case PROTO_MAGIC_RESPONSE_SUCCESS:
								success = true;
								break;
							case PROTO_MAGIC_RESPONSE_FAILURE:
								success = false;
								break;
							default:
								__ERROR__( "MasterWorker", "dispatch", "Invalid magic code from coordinator." );
								goto quit_1;
						}
						this->handleRemappingSetLockResponse( event, success, buffer.data, buffer.size );
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