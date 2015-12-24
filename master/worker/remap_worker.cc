#include "worker.hh"
#include "../main/master.hh"
#include "../../common/ds/value.hh"

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

	uint32_t originalListId, originalChunkId, remappedListId;
	std::vector<uint32_t> remappedChunkId; // data + parities ( 1 + m )
	bool connected;
	ssize_t sentBytes;
	SlaveSocket *socket;

	socket = this->getSlaves(
		header.key, header.keySize,
		originalListId, originalChunkId,
		remappedListId, remappedChunkId
	);

	if ( ! socket ) {
		Key key;
		key.set( header.keySize, header.key );
		event.resSet( event.socket, event.instanceId, event.requestId, key, false, false );
		this->dispatch( event );
		return false;
	}

	std::unordered_set<uint32_t>  remappedIndex;
	RequestRemapState reqRemapState = NO_REMAP;
	// determine whether data / parity is remapped
	for ( uint32_t i = 0; i < 1 + MasterWorker::parityChunkCount; i++ ) {
		if ( i == 0 ) {
			// data is remapped
			if ( ( MasterWorker::dataChunkCount < 2 && remappedListId != originalListId ) ||
				( MasterWorker::dataChunkCount > 1 && remappedChunkId[ i ] != originalChunkId )
			) {
				reqRemapState = DATA_REMAP;
			}
		} else if ( remappedChunkId[ i ] != i - 1 + MasterWorker::dataChunkCount ) {
			// parity is remapped
			if ( reqRemapState == DATA_REMAP ) {
				reqRemapState = MIXED_REMAP;
				break;
			} else if ( reqRemapState == NO_REMAP ) {
				reqRemapState = PARITY_REMAP;
				break;
			}
		}
	}

#define NO_DATA_REMAPPING ( reqRemapState == NO_REMAP || reqRemapState == PARITY_REMAP )
#define NO_PARITY_REMAPPING ( reqRemapState == NO_REMAP || reqRemapState == DATA_REMAP )

	struct {
		size_t size;
		char *data;
	} buffer;
	Key key;
	uint16_t instanceId = Master::instanceId;
	uint32_t requestId = MasterWorker::idGenerator->nextVal( this->workerId );

	key.dup( header.keySize, header.key, ( void * ) event.socket );

	// Insert the key into application SET pending map
	if ( ! MasterWorker::pending->insertKey( PT_APPLICATION_SET, event.instanceId, event.requestId, ( void * ) event.socket, key ) ) {
		__ERROR__( "MasterWorker", "handleRemappingSetRequest", "Cannot insert into application SET pending map." );
	}

	// Not needed???
	//if ( MasterWorker::updateInterval && NO_DATA_REMAPPING )
	//	MasterWorker::pending->recordRequestStartTime( PT_SLAVE_SET, requestId, event.instanceId, event.requestId, ( void * ) socket, socket->getAddr() );

	// always add a remapping record ( to buffer the value )
	Value *value = new Value(); // Note: Use an Key object to store the value
	value->dup( header.valueSize, header.value );

	// always acquire lock from coordinator first
	buffer.data = this->protocol.reqRemappingSetLock(
		buffer.size, instanceId, requestId,
		remappedListId, remappedChunkId,
		( uint32_t ) reqRemapState,
		header.key, header.keySize,
		-1 // not used
	);

	// insert the list of remapped slaves into pending map
	MasterWorker::pending->insertRemapList( PT_KEY_REMAP_LIST, instanceId, event.instanceId, requestId, event.requestId, ( void * ) socket, remappedChunkId );

	for( uint32_t i = 0; i < Master::getInstance()->sockets.coordinators.size(); i++ ) {
		CoordinatorSocket *coordinator = Master::getInstance()->sockets.coordinators.values[ i ];

		// Insert the remapping record into master REMAPPING_SET pending map
		// Note: the records point to the same copy of value
		RemappingRecord remappingRecord( remappedListId, remappedChunkId[ 0 ], value );
		if ( ! MasterWorker::pending->insertRemappingRecord( PT_SLAVE_REMAPPING_SET, instanceId, event.instanceId, requestId, event.requestId, coordinator, remappingRecord, true, true ) ) {
			__ERROR__( "MasterWorker", "handleRemappingSetRequest", "Cannot insert into slave REMAPPING_SET pending map." );
		}

		// send the message
		sentBytes = coordinator->send( buffer.data, buffer.size, connected );

		if ( sentBytes != ( ssize_t ) buffer.size ) {
			__ERROR__( "MasterWorker", "handleRemappingSetRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );

			if ( i == 0 ) {
				// revert all changes made
				MasterWorker::pending->eraseRemappingRecord( PT_SLAVE_REMAPPING_SET, instanceId, requestId, coordinator, 0, 0, true, true );
				delete value;

				return false;
			} else {
				// TODO handle message failure for some coordinators
			}

		}
	}

#undef NO_DATA_REMAPPING
#undef NO_PARITY_REMAPPING

	return true;
}

bool MasterWorker::handleRemappingSetLockResponse( CoordinatorEvent event, bool success, char *buf, size_t size ) {
	struct RemappingLockHeader header;
	if ( ! this->protocol.parseRemappingLockHeader( header, buf, size ) ) {
		__ERROR__( "MasterWorker", "handleRemappingSetLockResponse", "Invalid REMAPPING_SET_LOCK Response." );
		return false;
	}
	__DEBUG__(
		BLUE, "MasterWorker", "handleRemappingSetLockResponse",
		"[REMAPPING_SET_LOCK (%s)] Key: %.*s (key size = %u); Remapped to (list ID: %u, chunk ID: %u)",
		success ? "Success" : "Fail",
		( int ) header.keySize, header.key, header.keySize, header.listId, header.chunkId
	);

	uint32_t originalListId, originalChunkId;

	this->getSlaves(
		header.key, header.keySize,
		originalListId, originalChunkId
	);


	PendingIdentifier pid;
	RemappingRecord remappingRecord;

	if ( ! MasterWorker::pending->eraseRemappingRecord( PT_SLAVE_REMAPPING_SET, event.instanceId, event.requestId, 0, &pid, &remappingRecord, true, false ) ) {
		UNLOCK( &MasterWorker::pending->slaves.remappingSetLock );
		__ERROR__( "MasterWorker", "handleRemappingSetLockResponse", "Cannot find a pending slave REMAPPING_SET_LOCK request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
		return false;
	}

	SlaveSocket *socket;
	socket = this->getSlaves( originalListId, originalChunkId );

	// get the list of remapped slaves back
	std::vector<uint32_t> remapList;
	MasterWorker::pending->findRemapList( PT_KEY_REMAP_LIST, event.instanceId, event.requestId, 0, &remapList );
	if ( remapList.empty() ) {
		__ERROR__( "MasterWorker", "handleRemappingSetLockResponse", "List of remap slave not found, going to discard response." );
		success = false;
	}
	RequestRemapState reqRemapState = NO_REMAP;
	if ( originalListId != header.listId || originalChunkId != header.chunkId )
		reqRemapState = DATA_REMAP;
	for ( uint32_t i = 1; i < 1 + MasterWorker::parityChunkCount; i++ ) {
		if ( originalListId == header.listId && remapList[ i ] == i - 1 + MasterWorker::dataChunkCount ) {
			continue;
		}
		reqRemapState = ( reqRemapState == DATA_REMAP )? MIXED_REMAP : PARITY_REMAP;
		break;
	}

#define NO_DATA_REMAPPING ( reqRemapState == NO_REMAP || reqRemapState == PARITY_REMAP )
#define NO_PARITY_REMAPPING ( reqRemapState == NO_REMAP || reqRemapState == DATA_REMAP )

	if ( ! success ) {
		// TODO
		__ERROR__( "MasterWorker", "handleRemappingSetLockResponse", "TODO: Handle the case when the lock cannot be acquired." );
		// if lock fails report to application directly ..
		MasterWorker::pending->eraseRemapList( PT_KEY_REMAP_LIST, event.instanceId, event.requestId, 0, &pid, &remapList );
		return false;
	}

	// wait until all coordinator response to the locking
	// TODO handle failure response from some coordinators
	int pending = MasterWorker::pending->count( PT_SLAVE_REMAPPING_SET, event.instanceId, event.requestId, false, true );
	if ( pending > 0 ) {
		return true;
	}

	Key key;
	Value *value = ( Value * ) remappingRecord.ptr;

	if ( ! socket ) {
		// TODO
		__ERROR__( "MasterWorker", "handleRemappingSetLockResponse", "Not yet implemented!" );
		return false;
	}

	if ( ! MasterWorker::pending->findKey( PT_APPLICATION_SET, pid.parentInstanceId, pid.parentRequestId, 0, &key, true, header.key ) ) {
		__ERROR__( "MasterWorker", "handleRemappingSetLockResponse", "Cannot find a pending application SET request that matches the response. This message will be discarded. (ID: (%u, %u))", pid.parentInstanceId, pid.parentRequestId );
		// set socket fd for counter
		return false;
	}


	// get the socket of remapped data slave
	socket = this->getSlaves( remappingRecord.listId, remappingRecord.chunkId );

	// Add data and parity slaves into the pending set
	for ( uint32_t i = 0; i < MasterWorker::parityChunkCount + 1; i++ ) {
		SlaveSocket *target;
		if ( i == 0 ) {
			target = socket;
		} else if ( remapList[ i ] > MasterWorker::dataChunkCount - 1 ) {
			target = this->paritySlaveSockets[ remapList[ i ] - MasterWorker::dataChunkCount ];
		} else {
			target = this->dataSlaveSockets[ remapList[ i ] ];
		}
		if ( ! MasterWorker::pending->insertKey(
			PT_SLAVE_SET, pid.instanceId, pid.parentInstanceId, pid.requestId, pid.parentRequestId,
			target,
			key
		) ) {
			__ERROR__( "MasterWorker", "handleRemappingSetLockResponse", "Cannot insert into slave SET pending map." );
		}
	}

	// Prepare a packet buffer
	Packet *packet = 0;
	size_t s;

	// Send SET requests (parity)
	for ( uint32_t i = 0; i < MasterWorker::parityChunkCount; i++ ) {
		SlaveSocket *paritySocket = 0;
		struct sockaddr_in originalParityAddr = this->paritySlaveSockets[ i ]->getAddr();
		if ( remapList[ i + 1 ] > MasterWorker::dataChunkCount - 1 )
			paritySocket = this->paritySlaveSockets[ remapList[ i + 1 ] - MasterWorker::dataChunkCount ];
		else
			paritySocket = this->dataSlaveSockets[ remapList[ i + 1 ] ];
		packet = MasterWorker::packetPool->malloc();
		// for parity slaves
		packet->setReferenceCount( 1 );
		this->protocol.reqRemappingSet(
			s, pid.instanceId, pid.requestId,
			remappingRecord.listId, remappingRecord.chunkId,
			key.data, key.size,
			value->data, value->size,
			packet->data,
			this->paritySlaveSockets[ i ]->getSocket(), true /* isParity */,
			&originalParityAddr
		);
		packet->size = s;
		if ( MasterWorker::updateInterval ) {
			// Mark the time when request is sent
			MasterWorker::pending->recordRequestStartTime(
				PT_SLAVE_SET, pid.instanceId, pid.parentInstanceId, pid.requestId, pid.parentRequestId,
				( void * ) paritySocket, paritySocket->getAddr()
			);
		}

		SlaveEvent slaveEvent;
		slaveEvent.send( paritySocket, packet );
#ifdef MASTER_WORKER_SEND_REPLICAS_PARALLEL
		MasterWorker::eventQueue->prioritizedInsert( slaveEvent );
#else
		this->dispatch( slaveEvent );
#endif
	}

	if ( MasterWorker::updateInterval )
		MasterWorker::pending->recordRequestStartTime( PT_SLAVE_SET, pid.instanceId, pid.parentInstanceId, pid.requestId, pid.parentRequestId, ( void * ) socket, socket->getAddr() );

	packet = MasterWorker::packetPool->malloc();
	packet->setReferenceCount( 1 );
	struct sockaddr_in originalDataAddr = this->dataSlaveSockets[ originalChunkId ]->getAddr();
	this->protocol.reqRemappingSet(
		s, pid.instanceId, pid.requestId,
		remappingRecord.listId, remappingRecord.chunkId,
		key.data, key.size,
		value->data, value->size,
		packet->data,
		header.sockfd, false, /* ! isParity */
		&originalDataAddr
	);
	packet->size = s;

	// release value buffer
	delete value;
	value = 0;

	// Send SET request (data)
	SlaveEvent slaveEvent;
	slaveEvent.send( socket, packet );
	this->dispatch( slaveEvent );
	return true;

#undef NO_DATA_REMAPPING
#undef NO_PARITY_REMAPPING
#undef DECREMENT_COUNTER_ON_FAILURE
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

	this->getSlaves(
		header.key, header.keySize,
		originalListId, originalChunkId
	);

#define NO_REMAPPING ( originalListId == header.listId && originalChunkId == header.chunkId )

	int pending;
	ApplicationEvent applicationEvent;
	PendingIdentifier pid;
	Key key;

	// Find the cooresponding request
	if ( ! MasterWorker::pending->eraseKey( PT_SLAVE_SET, event.instanceId, event.requestId, ( void * ) event.socket, &pid, &key, true, false ) ) {
		UNLOCK( &MasterWorker::pending->slaves.setLock );
		__ERROR__( "MasterWorker", "handleRemappingSetResponse", "Cannot find a pending slave SET request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
		__ERROR__(
			"MasterWorker", "handleRemappingSetResponse",
			"[REMAPPING_SET (%s)] Key: %.*s (key size = %u); list ID: %u, chunk ID: %u.",
			success ? "Success" : "Fail",
			( int ) header.keySize, header.key, header.keySize,
			header.listId, header.chunkId
		);

		return false;
	}
	// Check pending slave SET requests
	pending = MasterWorker::pending->count( PT_SLAVE_SET, pid.instanceId, pid.requestId, false, true );

	// Mark the elapse time as latency
	Master* master = Master::getInstance();
	if ( MasterWorker::updateInterval ) {
		timespec elapsedTime;
		RequestStartTime rst;

		if ( ! MasterWorker::pending->eraseRequestStartTime( PT_SLAVE_SET, pid.instanceId, pid.requestId, ( void * ) event.socket, elapsedTime, 0, &rst ) ) {
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
		if ( ! MasterWorker::pending->eraseKey( PT_APPLICATION_SET, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &key, true, true, true, header.key ) ) {
			__ERROR__( "MasterWorker", "handleRemappingSetResponse", "Cannot find a pending application SET request that matches the response. This message will be discarded." );
			return false;
		}

		applicationEvent.resSet( ( ApplicationSocket * ) pid.ptr, pid.instanceId, pid.requestId, key, success );
		MasterWorker::eventQueue->insert( applicationEvent );

		// add a remaping record
		if ( ! NO_REMAPPING ) {
			key.set( header.keySize, header.key );
			RemappingRecord record ( header.listId, header.chunkId );
			MasterWorker::remappingRecords->insert( key, record, this->dataSlaveSockets[ originalChunkId ]->getAddr() );
		}
	}

#undef NO_REMAPPING
	return true;
}
