#include "worker.hh"
#include "../main/client.hh"
#include "../../common/ds/value.hh"

bool ClientWorker::handleDegradedSetRequest( ApplicationEvent event, char *buf, size_t size ) {
	struct KeyValueHeader header;
	if ( ! this->protocol.parseKeyValueHeader( header, buf, size ) ) {
		__ERROR__( "ClientWorker", "handleDegradedSetRequest", "Invalid SET request." );
		return false;
	}
	__DEBUG__(
		BLUE, "ClientWorker", "handleDegradedSetRequest",
		"[DEGRADED_SET] Key: %.*s (key size = %u); Value: (value size = %u)",
		( int ) header.keySize, header.key, header.keySize, header.valueSize
	);

	uint32_t *original, *remapped;
	uint32_t remappedCount;
	bool connected, useCoordinatedFlow;
	ssize_t sentBytes;
	ServerSocket *originalDataServerSocket;

	if ( ! this->getServers( PROTO_OPCODE_SET, header.key, header.keySize, original, remapped, remappedCount, originalDataServerSocket, useCoordinatedFlow ) ) {
		Key key;
		key.set( header.keySize, header.key );
		event.resSet( event.socket, event.instanceId, event.requestId, key, false, false );
		this->dispatch( event );
		return false;
	}

	struct {
		size_t size;
		char *data;
	} buffer;
	Key key;
	KeyValue keyValue;
	uint16_t instanceId = Client::instanceId;
	uint32_t requestId = ClientWorker::idGenerator->nextVal( this->workerId );

	keyValue._dup( header.key, header.keySize, header.value, header.valueSize );
	key = keyValue.key();

	// Insert the key into application SET pending map
	if ( ! ClientWorker::pending->insertKeyValue( PT_APPLICATION_SET, event.instanceId, event.requestId, ( void * ) event.socket, keyValue ) ) {
		__ERROR__( "ClientWorker", "handleDegradedSetRequest", "Cannot insert into application SET pending map. (%u, %u)", event.instanceId, event.requestId );
	}

	// always acquire lock from coordinator first
	buffer.data = this->protocol.reqDegradedSetLock(
		buffer.size, instanceId, requestId,
		original, remapped, remappedCount,
		header.key, header.keySize
	);

	// insert the list of remapped servers into pending map
	// Note: The original and remapped pointers are updated in getServers()
	RemapList remapList( original, remapped, remappedCount );
	for( uint32_t i = 0; i < Client::getInstance()->sockets.coordinators.size(); i++ ) {
		CoordinatorSocket *coordinatorSocket = Client::getInstance()->sockets.coordinators.values[ i ];

		ClientWorker::pending->insertRemapList( PT_KEY_REMAP_LIST, instanceId, event.instanceId, requestId, event.requestId, ( void * ) coordinatorSocket, remapList );

		sentBytes = coordinatorSocket->send( buffer.data, buffer.size, connected );
		if ( sentBytes != ( ssize_t ) buffer.size )
			__ERROR__( "ClientWorker", "handleDegradedSetRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
		break; // Only send to one coordinator
	}

	return true;
}

bool ClientWorker::handleDegradedSetLockResponse( CoordinatorEvent event, bool success, char *buf, size_t size ) {
	struct RemappingLockHeader header;
	if ( ! this->protocol.parseRemappingLockHeader( header, buf, size ) ) {
		__ERROR__( "ClientWorker", "handleDegradedSetLockResponse", "Invalid DEGRADED_SET_LOCK Response." );
		return false;
	}
	__DEBUG__(
		BLUE, "ClientWorker", "handleDegradedSetLockResponse",
		"[DEGRADED_SET_LOCK (%s)] [%u, %u] Key: %.*s (key size = %u)",
		success ? "Success" : "Fail",
		event.instanceId, event.requestId,
		( int ) header.keySize, header.key, header.keySize
	);

	// Find the corresponding DEGRADED_SET_LOCK request //
	PendingIdentifier pid;
	RemapList remapList;
	if ( ! ClientWorker::pending->eraseRemapList( PT_KEY_REMAP_LIST, event.instanceId, event.requestId, 0, &pid, &remapList ) ) {
		__ERROR__( "ClientWorker", "handleDegradedSetLockResponse", "Cannot find a pending DEGRADED_SET_LOCK request that matches the response. This message will be discarded. (ID: (%u, %u))", event.instanceId, event.requestId );
		return false;
	} else {
		remapList.free();
	}

	// Handle the case when the lock cannot be acquired //
	if ( ! success ) {
		__ERROR__( "ClientWorker", "handleDegradedSetLockResponse", "TODO: Handle the case when the lock cannot be acquired (ID: (%u, %u), key: %.*s).", event.instanceId, event.requestId, header.keySize, header.key );
		// if lock fails report to application directly ..
		KeyValue keyValue;
		if ( ! ClientWorker::pending->eraseKeyValue( PT_APPLICATION_SET, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &keyValue, true, true, true, header.key ) ) {
			__ERROR__( "ClientWorker", "handleDegradedSetLockResponse", "Cannot find a pending application SET request that matches the response. This message will be discarded. (Key = %.*s, ID = (%u, %u))", header.keySize, header.key, pid.parentInstanceId, pid.parentRequestId );
			return false;
		}

		ApplicationEvent applicationEvent;
		applicationEvent.resSet(
			( ApplicationSocket * ) pid.ptr, pid.instanceId, pid.requestId,
			keyValue, false // success
		);
		this->dispatch( applicationEvent );

		return false;
	}

	// Prepare the list of ServerSockets for the SET request //
	uint32_t originalListId, originalChunkId;
	ServerSocket *dataServerSocket = this->getServers(
		header.key, header.keySize,
		originalListId, originalChunkId
	);
	for ( uint32_t i = 0; i < header.remappedCount; i++ ) {
		ServerSocket *s = this->getServers(
			header.remapped[ i * 2     ],
			header.remapped[ i * 2 + 1 ]
		);
		if ( header.original[ i * 2 + 1 ] < ClientWorker::dataChunkCount ) {
			dataServerSocket = s;
		} else {
			this->parityServerSockets[ header.original[ i * 2 + 1 ] - ClientWorker::dataChunkCount ] = s;
		}
	}

	// Find the corresponding SET request //
	Key key;
	KeyValue keyValue;
	uint8_t keySize;
	uint32_t valueSize;
	char *keyStr, *valueStr;
	if ( ! ClientWorker::pending->findKeyValue( PT_APPLICATION_SET, pid.parentInstanceId, pid.parentRequestId, 0, &keyValue, true, header.key ) ) {
		__ERROR__( "ClientWorker", "handleDegradedSetLockResponse", "Cannot find a pending application SET request that matches the response. This message will be discarded. (ID: (%u, %u))", pid.parentInstanceId, pid.parentRequestId );
		return false;
	}
	key = keyValue.key();
	keyValue._deserialize( keyStr, keySize, valueStr, valueSize );

	// Insert pending SET requests for each involved servers //
	for ( uint32_t i = 0; i < ClientWorker::parityChunkCount + 1; i++ ) {
		if ( ! ClientWorker::pending->insertKey(
			PT_SERVER_DEGRADED_SET, pid.instanceId, pid.parentInstanceId, pid.requestId, pid.parentRequestId,
			( i == 0 ) ? dataServerSocket : this->parityServerSockets[ i - 1 ],
			key
		) ) {
			__ERROR__( "ClientWorker", "handleDegradedSetLockResponse", "Cannot insert into server SET pending map." );
		}
	}

	// Send the SET requests to all parity servers //
	Packet *packet = 0;
	struct {
		char *data;
		size_t size;
	} buffer;
	for ( uint32_t i = 0; i < ClientWorker::parityChunkCount; i++ ) {
		packet = ClientWorker::packetPool->malloc();
		packet->setReferenceCount( 1 );
		this->protocol.reqDegradedSet(
			buffer.size, pid.instanceId, pid.requestId,
			originalListId, i + ClientWorker::dataChunkCount, // Original list & chunk IDs
			header.original, header.remapped, header.remappedCount,
			keyStr, keySize,
			valueStr, valueSize,
			packet->data
		);
		packet->size = buffer.size;

		if ( ClientWorker::updateInterval ) {
			// Mark the time when request is sent
			ClientWorker::pending->recordRequestStartTime(
				PT_SERVER_DEGRADED_SET, pid.instanceId, pid.parentInstanceId, pid.requestId, pid.parentRequestId,
				( void * ) this->parityServerSockets[ i ],
				this->parityServerSockets[ i ]->getAddr()
			);
		}

		ServerEvent serverEvent;
		serverEvent.send( this->parityServerSockets[ i ], packet );
#ifdef CLIENT_WORKER_SEND_REPLICAS_PARALLEL
		ClientWorker::eventQueue->prioritizedInsert( serverEvent );
#else
		this->dispatch( serverEvent );
#endif
	}

	// Send the SET requests to the data server //
	ssize_t sentBytes;
	bool connected;
	if ( ClientWorker::updateInterval ) {
		ClientWorker::pending->recordRequestStartTime(
			PT_SERVER_DEGRADED_SET, pid.instanceId, pid.parentInstanceId, pid.requestId, pid.parentRequestId,
			( void * ) dataServerSocket,
			dataServerSocket->getAddr()
		);
	}
	buffer.data = this->protocol.reqDegradedSet(
		buffer.size, pid.instanceId, pid.requestId,
		originalListId, originalChunkId,
		header.original, header.remapped, header.remappedCount,
		keyStr, keySize,
		valueStr, valueSize
	);
	sentBytes = dataServerSocket->send( buffer.data, buffer.size, connected );
	if ( sentBytes != ( ssize_t ) buffer.size ) {
		__ERROR__( "ClientWorker", "handleDegradedSetLockResponse", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
	}

	return true;
}

bool ClientWorker::handleDegradedSetResponse( ServerEvent event, bool success, char *buf, size_t size ) {
	struct DegradedSetHeader header;
	if ( ! this->protocol.parseDegradedSetHeader( header, buf, size ) ) {
		__ERROR__( "ClientWorker", "handleDegradedSetResponse", "Invalid DEGRADED_SET Response." );
		return false;
	}
	__DEBUG__(
		BLUE, "ClientWorker", "handleDegradedSetResponse",
		"[DEGRADED_SET (%s)] Key: %.*s (key size = %u).",
		success ? "Success" : "Fail",
		( int ) header.keySize, header.key, header.keySize
	);

	int pending;
	ApplicationEvent applicationEvent;
	PendingIdentifier pid;
	Key key;
	KeyValue keyValue;

	// Find the cooresponding request //
	if ( ! ClientWorker::pending->eraseKey( PT_SERVER_DEGRADED_SET, event.instanceId, event.requestId, ( void * ) event.socket, &pid, &key, true, false ) ) {
		UNLOCK( &ClientWorker::pending->servers.setLock );
		__ERROR__( "ClientWorker", "handleDegradedSetResponse", "Cannot find a pending server SET request that matches the response. This message will be discarded. (Key: %.*s; ID: (%u, %u); list ID: %u, chunk ID: %u)", header.keySize, header.key, event.instanceId, event.requestId, header.listId, header.chunkId );
		return false;
	}
	// Check pending server SET requests //
	pending = ClientWorker::pending->count( PT_SERVER_DEGRADED_SET, pid.instanceId, pid.requestId, false, true );

	// Mark the elapse time as latency //
	Client *client = Client::getInstance();
	if ( ClientWorker::updateInterval ) {
		struct timespec elapsedTime;
		RequestStartTime rst;

		if ( ! ClientWorker::pending->eraseRequestStartTime( PT_SERVER_DEGRADED_SET, pid.instanceId, pid.requestId, ( void * ) event.socket, elapsedTime, 0, &rst ) ) {
			__ERROR__( "ClientWorker", "handleDegradedSetResponse", "Cannot find a pending stats SET request that matches the response." );
		} else {
			int index = -1;
			LOCK( &client->serverLoading.lock );
			std::set<Latency> *latencyPool = client->serverLoading.past.set.get( rst.addr, &index );
			// init. the set if it is not there
			if ( index == -1 ) {
				client->serverLoading.past.set.set( rst.addr, new std::set<Latency>() );
			}
			// insert the latency to the set
			// TODO use time when Response came, i.e. event created for latency cal.
			Latency latency = Latency ( elapsedTime );
			if ( index == -1 )
				latencyPool = client->serverLoading.past.set.get( rst.addr );
			latencyPool->insert( latency );
			UNLOCK( &client->serverLoading.lock );
		}
	}

	if ( pending == 0 ) {
		// Only send application SET response when the number of pending server SET requests equal 0
		if ( ! ClientWorker::pending->eraseKeyValue( PT_APPLICATION_SET, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &keyValue, true, true, true, header.key ) ) {
			__ERROR__( "ClientWorker", "handleDegradedSetResponse", "Cannot find a pending application SET request that matches the response. This message will be discarded." );
			return false;
		}
		key = keyValue.key();

		applicationEvent.resSet( ( ApplicationSocket * ) pid.ptr, pid.instanceId, pid.requestId, keyValue, success );
		assert( pid.ptr );
		this->dispatch( applicationEvent );
	}

	return true;
}
