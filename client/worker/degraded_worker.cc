#include "worker.hh"
#include "../main/client.hh"

bool ClientWorker::sendDegradedLockRequest(
	uint16_t parentInstanceId, uint32_t parentRequestId, uint8_t opcode,
	uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
	char *key, uint8_t keySize,
	uint32_t valueUpdateSize, uint32_t valueUpdateOffset, char *valueUpdate
) {
	uint16_t instanceId = Client::instanceId;
	uint32_t requestId = ClientWorker::idGenerator->nextVal( this->workerId );
	CoordinatorSocket *socket = Client::getInstance()->sockets.coordinators.values[ 0 ];

	// Add the degraded lock request to the pending set
	DegradedLockData degradedLockData;
	degradedLockData.set(
		opcode,
		original, reconstructed, reconstructedCount,
		keySize, key
	);

	if ( valueUpdateSize != 0 && valueUpdate )
		degradedLockData.set( valueUpdateSize, valueUpdateOffset, valueUpdate );

	if ( ! ClientWorker::pending->insertDegradedLockData( PT_COORDINATOR_DEGRADED_LOCK_DATA, instanceId, parentInstanceId, requestId, parentRequestId, ( void * ) socket, degradedLockData ) ) {
		__ERROR__( "ClientWorker", "handleDegradedRequest", "Cannot insert into server degraded lock pending map." );
	}

	// Get degraded lock from the coordinator
	struct {
		size_t size;
		char *data;
	} buffer;
	ssize_t sentBytes;
	bool connected;

	buffer.data = this->protocol.reqDegradedLock(
		buffer.size, instanceId, requestId,
		original, reconstructed, reconstructedCount,
		key, keySize
	);

	// Send degraded lock request
	sentBytes = socket->send( buffer.data, buffer.size, connected );
	if ( sentBytes != ( ssize_t ) buffer.size ) {
		__ERROR__( "ClientWorker", "handleDegradedRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
		return false;
	}

	return true;
}

bool ClientWorker::handleDegradedLockResponse( CoordinatorEvent event, bool success, char *buf, size_t size ) {
	uint32_t originalListId, originalChunkId;
	ServerSocket *socket = 0;
	struct DegradedLockResHeader header;
	if ( ! this->protocol.parseDegradedLockResHeader( header, buf, size ) ) {
		__ERROR__( "ClientWorker", "handleDegradedLockResponse", "Invalid DEGRADED_LOCK response." );
		return false;
	}

	socket = this->getServers( header.key, header.keySize, originalListId, originalChunkId );

	switch( header.type ) {
		case PROTO_DEGRADED_LOCK_RES_IS_LOCKED:
		case PROTO_DEGRADED_LOCK_RES_WAS_LOCKED:
			__DEBUG__(
				BLUE, "ClientWorker", "handleDegradedLockResponse",
				"[%s Locked] [%u, %u, %u] Key: %.*s (key size = %u); Is Sealed? %s.",
				header.type == PROTO_DEGRADED_LOCK_RES_IS_LOCKED ? "Is" : "Was",
				originalListId, header.stripeId, originalChunkId,
				( int ) header.keySize, header.key, header.keySize,
				header.isSealed ? "true" : "false"
			);

			// Get redirected data server socket
			if ( header.reconstructedCount ) {
				for ( uint32_t i = 0; i < header.reconstructedCount; i++ ) {
					if ( header.original[ i * 2     ] == originalListId &&
					     header.original[ i * 2 + 1 ] == originalChunkId ) {
						socket = this->getServers(
							header.reconstructed[ i * 2     ],
							header.reconstructed[ i * 2 + 1 ]
						);
						break;
					}
				}
			}
			break;
		case PROTO_DEGRADED_LOCK_RES_NOT_LOCKED:
			__DEBUG__(
				BLUE, "ClientWorker", "handleDegradedLockResponse",
				"[Not Locked] [%u, %u] Key: %.*s (key size = %u).",
				originalListId, originalChunkId,
				( int ) header.keySize, header.key, header.keySize
			);
			break;
		case PROTO_DEGRADED_LOCK_RES_REMAPPED:
			__DEBUG__(
				BLUE, "ClientWorker", "handleDegradedLockResponse",
				"[Remapped] [%u, %u] Key: %.*s (key size = %u).",
				originalListId, originalChunkId,
				( int ) header.keySize, header.key, header.keySize
			);

			// Get redirected data server socket
			if ( header.remappedCount ) {
				for ( uint32_t i = 0; i < header.remappedCount; i++ ) {
					if ( header.original[ i * 2     ] == originalListId &&
					     header.original[ i * 2 + 1 ] == originalChunkId ) {
						socket = this->getServers(
							header.remapped[ i * 2     ],
							header.remapped[ i * 2 + 1 ]
						);
						break;
					}
				}
			}
			break;
		case PROTO_DEGRADED_LOCK_RES_NOT_EXIST:
		default:
			__DEBUG__(
				BLUE, "ClientWorker", "handleDegradedLockResponse",
				"[Not Found] Key: %.*s (key size = %u)",
				( int ) header.keySize, header.key, header.keySize
			);
			break;
	}

	DegradedLockData degradedLockData;
	PendingIdentifier pid;
	Key key;
	KeyValueUpdate keyValueUpdate;

	struct {
		size_t size;
		char *data;
	} buffer;
	ssize_t sentBytes;
	bool connected;
	ApplicationEvent applicationEvent;
	uint16_t instanceId = Client::instanceId;
	uint32_t requestId = ClientWorker::idGenerator->nextVal( this->workerId );
	uint32_t requestTimestamp = 0;

	// Find the corresponding request
	if ( ! ClientWorker::pending->eraseDegradedLockData( PT_COORDINATOR_DEGRADED_LOCK_DATA, event.instanceId, event.requestId, event.socket, &pid, &degradedLockData ) ) {
		__ERROR__( "ClientWorker", "handleDegradedLockResponse", "Cannot find a pending coordinator DEGRADED_LOCK request that matches the response. This message will be discarded (key = %.*s).", header.keySize, header.key );
		return false;
	}

	// Send the degraded request to the server
	switch( degradedLockData.opcode ) {
		case PROTO_OPCODE_GET:
			// Prepare GET request
			switch( header.type ) {
				case PROTO_DEGRADED_LOCK_RES_IS_LOCKED:
				case PROTO_DEGRADED_LOCK_RES_WAS_LOCKED:
					buffer.data = this->protocol.reqDegradedGet(
						buffer.size, instanceId, requestId,
						header.isSealed, header.stripeId,
						header.original, header.reconstructed, header.reconstructedCount,
						header.ongoingAtChunk, header.numSurvivingChunkIds, header.survivingChunkIds,
						degradedLockData.key, degradedLockData.keySize
					);
					break;
				case PROTO_DEGRADED_LOCK_RES_NOT_LOCKED:
				case PROTO_DEGRADED_LOCK_RES_REMAPPED:
					buffer.data = this->protocol.reqGet(
						buffer.size, instanceId, requestId,
						header.key, header.keySize
					);
					if ( ClientWorker::updateInterval ) {
						ClientWorker::pending->recordRequestStartTime( PT_SERVER_GET, instanceId, pid.parentInstanceId, requestId, pid.parentRequestId, ( void * ) socket, socket->getAddr() );
					}
					break;
				case PROTO_DEGRADED_LOCK_RES_NOT_EXIST:
					if ( ! ClientWorker::pending->eraseKey( PT_APPLICATION_GET, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &key, true, true, true, header.key ) ) {
						__ERROR__( "ClientWorker", "handleDegradedLockResponse", "Cannot find a pending application GET request that matches the response. This message will be discarded (key = %.*s).", header.keySize, header.key );
						return false;
					}
					applicationEvent.resGet( ( ApplicationSocket * ) pid.ptr, pid.instanceId, pid.requestId, key );
					// printf( "handleDegradedLockResponse(): Key %.*s not found.\n", key.size, key.data );
					ClientWorker::eventQueue->insert( applicationEvent );
					return true;
			}

			// Insert into server GET pending map
			key.set( degradedLockData.keySize, degradedLockData.key, ( void * ) original );
			if ( ! ClientWorker::pending->insertKey( PT_SERVER_GET, instanceId, pid.parentInstanceId, requestId, pid.parentRequestId, ( void * ) socket, key ) ) {
				__ERROR__( "ClientWorker", "handleDegradedLockResponse", "Cannot insert into server GET pending map." );
			}
			break;
		case PROTO_OPCODE_UPDATE:
			requestTimestamp = socket->timestamp.current.nextVal();
			switch( header.type ) {
				case PROTO_DEGRADED_LOCK_RES_IS_LOCKED:
				case PROTO_DEGRADED_LOCK_RES_WAS_LOCKED:
					buffer.data = this->protocol.reqDegradedUpdate(
						buffer.size, instanceId, requestId,
						header.isSealed, header.stripeId,
						header.original, header.reconstructed, header.reconstructedCount,
						header.ongoingAtChunk, header.numSurvivingChunkIds, header.survivingChunkIds,
						degradedLockData.key, degradedLockData.keySize,
						degradedLockData.valueUpdate, degradedLockData.valueUpdateOffset, degradedLockData.valueUpdateSize,
						requestTimestamp
					);
					break;
				case PROTO_DEGRADED_LOCK_RES_NOT_LOCKED:
				case PROTO_DEGRADED_LOCK_RES_REMAPPED:
					buffer.data = this->protocol.reqUpdate(
						buffer.size, instanceId, requestId,
						degradedLockData.key, degradedLockData.keySize,
						degradedLockData.valueUpdate, degradedLockData.valueUpdateOffset, degradedLockData.valueUpdateSize,
						requestTimestamp,
						true
					);
					break;
				case PROTO_DEGRADED_LOCK_RES_NOT_EXIST:
					if ( ! ClientWorker::pending->eraseKeyValueUpdate( PT_APPLICATION_UPDATE, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &keyValueUpdate, true, true, true, header.key ) ) {
						__ERROR__( "ClientWorker", "handleDegradedLockResponse", "Cannot find a pending application UPDATE request that matches the response. This message will be discarded (key = %.*s).", header.keySize, header.key );
						return false;
					}
					delete[] ( ( char * )( keyValueUpdate.ptr ) );
					applicationEvent.resUpdate( ( ApplicationSocket * ) pid.ptr, pid.parentInstanceId, pid.parentRequestId, keyValueUpdate, false );
					ClientWorker::eventQueue->insert( applicationEvent );
					return true;
			}

			// Insert into server UPDATE pending map
			keyValueUpdate.set( degradedLockData.keySize, degradedLockData.key, ( void * ) original );
			keyValueUpdate.offset = degradedLockData.valueUpdateOffset;
			keyValueUpdate.length = degradedLockData.valueUpdateSize;
			if ( ! ClientWorker::pending->insertKeyValueUpdate( PT_SERVER_UPDATE, instanceId, pid.parentInstanceId, requestId, pid.parentRequestId, ( void * ) socket, keyValueUpdate, true, true, requestTimestamp ) ) {
				__ERROR__( "ClientWorker", "handleUpdateRequest", "Cannot insert into server UPDATE pending map." );
			}
			break;
		case PROTO_OPCODE_DELETE:
			requestTimestamp = socket->timestamp.current.nextVal();
			switch( header.type ) {
				case PROTO_DEGRADED_LOCK_RES_IS_LOCKED:
				case PROTO_DEGRADED_LOCK_RES_WAS_LOCKED:
					buffer.data = this->protocol.reqDegradedDelete(
						buffer.size, instanceId, requestId,
						header.isSealed, header.stripeId,
						header.original, header.reconstructed, header.reconstructedCount,
						header.ongoingAtChunk, header.numSurvivingChunkIds, header.survivingChunkIds,
						degradedLockData.key, degradedLockData.keySize,
						requestTimestamp
					);
					break;
				case PROTO_DEGRADED_LOCK_RES_NOT_LOCKED:
				case PROTO_DEGRADED_LOCK_RES_REMAPPED:
					buffer.data = this->protocol.reqDelete(
						buffer.size, instanceId, requestId,
						degradedLockData.key, degradedLockData.keySize,
						requestTimestamp,
						true
					);
					break;
				case PROTO_DEGRADED_LOCK_RES_NOT_EXIST:
					if ( ! ClientWorker::pending->eraseKey( PT_APPLICATION_DEL, pid.parentInstanceId, pid.parentRequestId, 0, &pid, &key, true, true, true, header.key ) ) {
						__ERROR__( "ClientWorker", "handleDegradedLockResponse", "Cannot find a pending application DELETE request that matches the response. This message will be discarded (key = %.*s).", header.keySize, header.key );
						return false;
					}
					applicationEvent.resDelete( ( ApplicationSocket * ) pid.ptr, pid.parentInstanceId, pid.parentRequestId, key, false );
					ClientWorker::eventQueue->insert( applicationEvent );
					return true;
			}

			// Insert into server DELETE pending map
			key.set( degradedLockData.keySize, degradedLockData.key, ( void * ) original );
			if ( ! ClientWorker::pending->insertKey( PT_SERVER_DEL, instanceId, pid.parentInstanceId, requestId, pid.parentRequestId, ( void * ) socket, key, true, true, requestTimestamp ) ) {
				__ERROR__( "ClientWorker", "handleDegradedLockResponse", "Cannot insert into server DELETE pending map." );
			}
			break;
		default:
			__ERROR__( "ClientWorker", "handleDegradedLockResponse", "Invalid opcode in DegradedLockData." );
			return false;
	}

	degradedLockData.free();

	// Send request
	if ( Client::getInstance()->isDegraded( socket ) ) {
		printf( "ERROR Sending to failed server socket (opcode = %u, key = %.*s)!\n", degradedLockData.opcode, header.keySize, header.key );
		switch ( header.type ) {
			case PROTO_DEGRADED_LOCK_RES_IS_LOCKED:
				printf( "PROTO_DEGRADED_LOCK_RES_IS_LOCKED\n" );
				for ( uint32_t i = 0; i < header.reconstructedCount; i++ ) {
					printf(
						"%s(%u, %u) |-> (%u, %u)%s",
						i == 0 ? "Original: " : "; ",
						header.original[ i * 2     ],
						header.original[ i * 2 + 1 ],
						header.reconstructed[ i * 2     ],
						header.reconstructed[ i * 2 + 1 ],
						i == header.reconstructedCount - 1 ? " || " : ""
					);
				}
				printf( "\n" );
				fflush( stdout );
				break;
			case PROTO_DEGRADED_LOCK_RES_WAS_LOCKED:
				printf( "PROTO_DEGRADED_LOCK_RES_WAS_LOCKED: %.*s\n", header.keySize, header.key );
				for ( uint32_t i = 0; i < header.reconstructedCount; i++ ) {
					printf(
						"%s(%u, %u) |-> (%u, %u)%s",
						i == 0 ? "Original: " : "; ",
						header.original[ i * 2     ],
						header.original[ i * 2 + 1 ],
						header.reconstructed[ i * 2     ],
						header.reconstructed[ i * 2 + 1 ],
						i == header.reconstructedCount - 1 ? " || " : ""
					);
				}
				printf( "\n" );
				fflush( stdout );
				break;
			case PROTO_DEGRADED_LOCK_RES_NOT_LOCKED:
				printf( "PROTO_DEGRADED_LOCK_RES_NOT_LOCKED\n" );
				break;
			case PROTO_DEGRADED_LOCK_RES_REMAPPED:
				printf( "PROTO_DEGRADED_LOCK_RES_REMAPPED\n" );
				break;
			case PROTO_DEGRADED_LOCK_RES_NOT_EXIST:
				printf( "PROTO_DEGRADED_LOCK_RES_NOT_EXIST\n" );
				break;
		}

		assert( false );
	}
	sentBytes = socket->send( buffer.data, buffer.size, connected );
	if ( sentBytes != ( ssize_t ) buffer.size ) {
		__ERROR__( "ClientWorker", "handleGetRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
		return false;
	}

	return true;
}
