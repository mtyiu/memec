#include "worker.hh"
#include "../main/client.hh"
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

	uint32_t *original, *remapped;
	uint32_t remappedCount;
	bool connected, useCoordinatedFlow;
	ssize_t sentBytes;
	SlaveSocket *originalDataSlaveSocket;

	if ( ! this->getSlaves( PROTO_OPCODE_SET, header.key, header.keySize, original, remapped, remappedCount, originalDataSlaveSocket, useCoordinatedFlow ) ) {
		Key key;
		key.set( header.keySize, header.key );
		event.resSet( event.socket, event.instanceId, event.requestId, key, false, false );
		this->dispatch( event );
		return false;
	} else {
		// printf( "Remapping " );
		// for ( uint32_t i = 0; i < remappedCount; i++ ) {
		// 	if ( i ) printf( "; " );
		// 	printf(
		// 		"(%u, %u) |-> (%u, %u)",
		// 		original[ i * 2 ], original[ i * 2 + 1 ],
		// 		remapped[ i * 2 ], remapped[ i * 2 + 1 ]
		// 	);
		// }
		// printf( " (count = %u).\n", remappedCount );
	}

	struct {
		size_t size;
		char *data;
	} buffer;
	Key key;
	KeyValue keyValue;
	uint16_t instanceId = Master::instanceId;
	uint32_t requestId = MasterWorker::idGenerator->nextVal( this->workerId );

	keyValue.dup( header.key, header.keySize, header.value, header.valueSize );
	key = keyValue.key();

	// Insert the key into application SET pending map
	if ( ! MasterWorker::pending->insertKeyValue( PT_APPLICATION_SET, event.instanceId, event.requestId, ( void * ) event.socket, keyValue ) ) {
		__ERROR__( "MasterWorker", "handleRemappingSetRequest", "Cannot insert into application SET pending map. (%u, %u)", event.instanceId, event.requestId );
	}

	// always acquire lock from coordinator first
	buffer.data = this->protocol.reqRemappingSetLock(
		buffer.size, instanceId, requestId,
		original, remapped, remappedCount,
		header.key, header.keySize
	);

	// insert the list of remapped slaves into pending map
	// Note: The original and remapped pointers are updated in getSlaves()
	RemapList remapList( original, remapped, remappedCount );
	for( uint32_t i = 0; i < Master::getInstance()->sockets.coordinators.size(); i++ ) {
		CoordinatorSocket *coordinatorSocket = Master::getInstance()->sockets.coordinators.values[ i ];

		MasterWorker::pending->insertRemapList( PT_KEY_REMAP_LIST, instanceId, event.instanceId, requestId, event.requestId, ( void * ) coordinatorSocket, remapList );

		sentBytes = coordinatorSocket->send( buffer.data, buffer.size, connected );
		if ( sentBytes != ( ssize_t ) buffer.size )
			__ERROR__( "MasterWorker", "handleRemappingSetRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
		break; // Only send to one coordinator
	}

	// printf( "[%u] Requesting REMAPPING_SET_LOCK...\n", requestId );

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
		"[REMAPPING_SET_LOCK (%s)] [%u, %u] Key: %.*s (key size = %u)",
		success ? "Success" : "Fail",
		event.instanceId, event.requestId,
		( int ) header.keySize, header.key, header.keySize
	);

	// printf( "[%u, %u] Handling REMAPPING_SET_LOCK response...\n", event.instanceId, event.requestId );

	// Find the corresponding REMAPPING_SET_LOCK request //
	PendingIdentifier pid;
	RemapList remapList;
	if ( ! MasterWorker::pending->eraseRemapList( PT_KEY_REMAP_LIST, event.instanceId, event.requestId, 0, &pid, &remapList ) ) {
		__ERROR__( "MasterWorker", "handleRemappingSetLockResponse", "Cannot find a pending REMAPPING_SET_LOCK request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
		return false;
	} else {
		remapList.free();
	}

	// printf( "[%u, %u] Handling REMAPPING_SET_LOCK response...\n", pid.instanceId, pid.requestId );

	// Handle the case when the lock cannot be acquired //
	if ( ! success ) {
		__ERROR__( "MasterWorker", "handleRemappingSetLockResponse", "TODO: Handle the case when the lock cannot be acquired (ID: (%u, %u), key: %.*s).", event.instanceId, event.requestId, header.keySize, header.key );
		// if lock fails report to application directly ..
		KeyValue keyValue;
		if ( ! MasterWorker::pending->eraseKeyValue( PT_APPLICATION_SET, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &keyValue, true, true, true, header.key ) ) {
			__ERROR__( "MasterWorker", "handleRemappingSetLockResponse", "Cannot find a pending application SET request that matches the response. This message will be discarded. (Key = %.*s, ID = (%u, %u))", header.keySize, header.key, pid.parentInstanceId, pid.parentRequestId );
			return false;
		// } else {
		// 	Master::getInstance()->printPending();
		// 	printf( "Request found.\n" );
		}

		ApplicationEvent applicationEvent;
		applicationEvent.resSet(
			( ApplicationSocket * ) pid.ptr, pid.instanceId, pid.requestId,
			keyValue, false // success
		);
		this->dispatch( applicationEvent );

		return false;
	}

	// Prepare the list of SlaveSockets for the SET request //
	uint32_t originalListId, originalChunkId;
	SlaveSocket *dataSlaveSocket = this->getSlaves(
		header.key, header.keySize,
		originalListId, originalChunkId
	);
	for ( uint32_t i = 0; i < header.remappedCount; i++ ) {
		SlaveSocket *s = this->getSlaves(
			header.remapped[ i * 2     ],
			header.remapped[ i * 2 + 1 ]
		);
		if ( header.original[ i * 2 + 1 ] < MasterWorker::dataChunkCount ) {
			dataSlaveSocket = s;
		} else {
			this->paritySlaveSockets[ header.original[ i * 2 + 1 ] - MasterWorker::dataChunkCount ] = s;
		}
	}

	// Find the corresponding SET request //
	Key key;
	KeyValue keyValue;
	uint8_t keySize;
	uint32_t valueSize;
	char *keyStr, *valueStr;
	if ( ! MasterWorker::pending->findKeyValue( PT_APPLICATION_SET, pid.parentInstanceId, pid.parentRequestId, 0, &keyValue, true, header.key ) ) {
		__ERROR__( "MasterWorker", "handleRemappingSetLockResponse", "Cannot find a pending application SET request that matches the response. This message will be discarded. (ID: (%u, %u))", pid.parentInstanceId, pid.parentRequestId );
		return false;
	}
	key = keyValue.key();
	keyValue.deserialize( keyStr, keySize, valueStr, valueSize );

	// Insert pending SET requests for each involved slaves //
	for ( uint32_t i = 0; i < MasterWorker::parityChunkCount + 1; i++ ) {
		if ( ! MasterWorker::pending->insertKey(
			PT_SLAVE_REMAPPING_SET, pid.instanceId, pid.parentInstanceId, pid.requestId, pid.parentRequestId,
			( i == 0 ) ? dataSlaveSocket : this->paritySlaveSockets[ i - 1 ],
			key
		) ) {
			__ERROR__( "MasterWorker", "handleRemappingSetLockResponse", "Cannot insert into slave SET pending map." );
		}
	}

	// Send the SET requests to all parity servers //
	Packet *packet = 0;
	struct {
		char *data;
		size_t size;
	} buffer;
	for ( uint32_t i = 0; i < MasterWorker::parityChunkCount; i++ ) {
		packet = MasterWorker::packetPool->malloc();
		packet->setReferenceCount( 1 );
		this->protocol.reqRemappingSet(
			buffer.size, pid.instanceId, pid.requestId,
			originalListId, i + MasterWorker::dataChunkCount, // Original list & chunk IDs
			header.original, header.remapped, header.remappedCount,
			keyStr, keySize,
			valueStr, valueSize,
			packet->data
		);
		packet->size = buffer.size;

		// printf( "[%u, %u] Sending %.*s to ", pid.parentInstanceId, pid.parentRequestId, keySize, keyStr );
		// this->paritySlaveSockets[ i ]->printAddress();
		// printf( "\n" );

		if ( MasterWorker::updateInterval ) {
			// Mark the time when request is sent
			MasterWorker::pending->recordRequestStartTime(
				PT_SLAVE_REMAPPING_SET, pid.instanceId, pid.parentInstanceId, pid.requestId, pid.parentRequestId,
				( void * ) this->paritySlaveSockets[ i ],
				this->paritySlaveSockets[ i ]->getAddr()
			);
		}

		SlaveEvent slaveEvent;
		slaveEvent.send( this->paritySlaveSockets[ i ], packet );
#ifdef CLIENT_WORKER_SEND_REPLICAS_PARALLEL
		MasterWorker::eventQueue->prioritizedInsert( slaveEvent );
#else
		this->dispatch( slaveEvent );
#endif
	}

	// Send the SET requests to the data server //
	ssize_t sentBytes;
	bool connected;
	if ( MasterWorker::updateInterval ) {
		MasterWorker::pending->recordRequestStartTime(
			PT_SLAVE_REMAPPING_SET, pid.instanceId, pid.parentInstanceId, pid.requestId, pid.parentRequestId,
			( void * ) dataSlaveSocket,
			dataSlaveSocket->getAddr()
		);
	}
	buffer.data = this->protocol.reqRemappingSet(
		buffer.size, pid.instanceId, pid.requestId,
		originalListId, originalChunkId,
		header.original, header.remapped, header.remappedCount,
		keyStr, keySize,
		valueStr, valueSize
	);
	sentBytes = dataSlaveSocket->send( buffer.data, buffer.size, connected );
	if ( sentBytes != ( ssize_t ) buffer.size ) {
		__ERROR__( "MasterWorker", "handleRemappingSetLockResponse", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
	}

	// printf( "[%u] Sending REMAPPING_SET request...\n", pid.requestId );

	return true;
}

bool MasterWorker::handleRemappingSetResponse( SlaveEvent event, bool success, char *buf, size_t size ) {
	struct RemappingSetHeader header;
	if ( ! this->protocol.parseRemappingSetHeader( header, buf, size ) ) {
		__ERROR__( "MasterWorker", "handleRemappingSetResponse", "Invalid REMAPPING_SET Response." );
		return false;
	}
	__DEBUG__(
		BLUE, "MasterWorker", "handleRemappingSetResponse",
		"[REMAPPING_SET (%s)] Key: %.*s (key size = %u).",
		success ? "Success" : "Fail",
		( int ) header.keySize, header.key, header.keySize
	);

	int pending;
	ApplicationEvent applicationEvent;
	PendingIdentifier pid;
	Key key;
	KeyValue keyValue;

	// printf( "[%u] Handling REMAPPING_SET response...\n", event.requestId );

	// Find the cooresponding request //
	if ( ! MasterWorker::pending->eraseKey( PT_SLAVE_REMAPPING_SET, event.instanceId, event.requestId, ( void * ) event.socket, &pid, &key, true, false ) ) {
		UNLOCK( &MasterWorker::pending->slaves.setLock );
		__ERROR__( "MasterWorker", "handleRemappingSetResponse", "Cannot find a pending slave SET request that matches the response. This message will be discarded. (Key: %.*s; ID: (%u, %u); list ID: %u, chunk ID: %u)", header.keySize, header.key, event.instanceId, event.requestId, header.listId, header.chunkId );
		return false;
	}
	// Check pending slave SET requests //
	pending = MasterWorker::pending->count( PT_SLAVE_REMAPPING_SET, pid.instanceId, pid.requestId, false, true );

	// Mark the elapse time as latency //
	Master *master = Master::getInstance();
	if ( MasterWorker::updateInterval ) {
		struct timespec elapsedTime;
		RequestStartTime rst;

		if ( ! MasterWorker::pending->eraseRequestStartTime( PT_SLAVE_REMAPPING_SET, pid.instanceId, pid.requestId, ( void * ) event.socket, elapsedTime, 0, &rst ) ) {
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
	// 	__ERROR__( "MasterWorker", "handleRemappingSetResponse", "Pending slave REMAPPING_SET requests = %d (remapped count: %u).", pending, header.remappedCount );
	// }

	if ( pending == 0 ) {
		// Only send application SET response when the number of pending slave SET requests equal 0
		if ( ! MasterWorker::pending->eraseKeyValue( PT_APPLICATION_SET, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &keyValue, true, true, true, header.key ) ) {
			__ERROR__( "MasterWorker", "handleRemappingSetResponse", "Cannot find a pending application SET request that matches the response. This message will be discarded." );
			return false;
		}
		key = keyValue.key();

		applicationEvent.resSet( ( ApplicationSocket * ) pid.ptr, pid.instanceId, pid.requestId, keyValue, success );
		assert( pid.ptr );
		this->dispatch( applicationEvent );
	}

	return true;
}
