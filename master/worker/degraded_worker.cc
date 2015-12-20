#include "worker.hh"
#include "../main/master.hh"

bool MasterWorker::sendDegradedLockRequest( uint32_t parentId, uint8_t opcode, uint32_t listId, uint32_t dataChunkId, uint32_t newDataChunkId, uint32_t parityChunkId, uint32_t newParityChunkId, char *key, uint8_t keySize, uint32_t valueUpdateSize, uint32_t valueUpdateOffset, char *valueUpdate ) {
	uint32_t requestId = MasterWorker::idGenerator->nextVal( this->workerId );
	CoordinatorSocket *socket = Master::getInstance()->sockets.coordinators.values[ 0 ];

	// Add the degraded lock request to the pending set
	DegradedLockData degradedLockData;

	if ( parityChunkId == 0 ) {
		degradedLockData.set(
			opcode,
			listId, 0 /* srcStripeId */, dataChunkId,
			listId, newDataChunkId,
			keySize, key
		);
	} else {
		degradedLockData.set(
			opcode,
			listId, 0 /* srcStripeId */, parityChunkId,
			listId, newParityChunkId,
			keySize, key
		);
	}
	if ( valueUpdateSize != 0 && valueUpdate ) {
		degradedLockData.set( valueUpdateSize, valueUpdateOffset, valueUpdate );
	}

	if ( ! MasterWorker::pending->insertDegradedLockData( PT_COORDINATOR_DEGRADED_LOCK_DATA, requestId, parentId, ( void * ) socket, degradedLockData ) ) {
		__ERROR__( "MasterWorker", "handleDegradedRequest", "Cannot insert into slave degraded lock pending map." );
	}

	// Get degraded lock from the coordinator
	struct {
		size_t size;
		char *data;
	} buffer;
	ssize_t sentBytes;
	bool connected;

	buffer.data = this->protocol.reqDegradedLock(
		buffer.size, requestId,
		listId,
		dataChunkId, newDataChunkId,
		parityChunkId, newParityChunkId,
		key, keySize
	);

	// Send degraded lock request
	sentBytes = socket->send( buffer.data, buffer.size, connected );
	if ( sentBytes != ( ssize_t ) buffer.size ) {
		__ERROR__( "MasterWorker", "handleDegradedRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
		return false;
	}

	return true;
}

bool MasterWorker::handleDegradedLockResponse( CoordinatorEvent event, bool success, char *buf, size_t size ) {
	SlaveSocket *socket = 0, *original = 0;
	int sockfd;
	struct DegradedLockResHeader header;
	if ( ! this->protocol.parseDegradedLockResHeader( header, buf, size ) ) {
		__ERROR__( "MasterWorker", "handleDegradedLockResponse", "Invalid DEGRADED_LOCK response." );
		return false;
	}
	switch( header.type ) {
		case PROTO_DEGRADED_LOCK_RES_IS_LOCKED:
		case PROTO_DEGRADED_LOCK_RES_WAS_LOCKED:
			__DEBUG__(
				BLUE, "MasterWorker", "handleDegradedLockResponse",
				"[%s Locked] Key: %.*s (key size = %u); list ID: %u, stripe ID: %u, data: (%u --> %u), parity: (%u --> %u). Is sealed? %s",
				header.type == PROTO_DEGRADED_LOCK_RES_IS_LOCKED ? "Is" : "Was",
				( int ) header.keySize, header.key, header.keySize,
				header.listId,
				header.stripeId,
				header.srcDataChunkId, header.dstDataChunkId,
				header.srcParityChunkId, header.dstParityChunkId,
				header.isSealed ? "true" : "false"
			);

			if ( header.srcParityChunkId == 0 ) {
				// Data server is redirected
				original = this->getSlaves( header.listId, header.srcDataChunkId );
				socket = this->getSlaves( header.listId, header.dstDataChunkId );
			} else {
				// Parity server is redirected; use the original data server
				original = socket = this->getSlaves( header.listId, header.srcDataChunkId );
			}
			break;
		case PROTO_DEGRADED_LOCK_RES_NOT_LOCKED:
			__DEBUG__(
				BLUE, "MasterWorker", "handleDegradedLockResponse",
				"[Not Locked] Key: %.*s (key size = %u); (%u, %u)",
				( int ) header.keySize, header.key, header.keySize,
				header.listId, header.srcDataChunkId
			);

			if ( header.srcParityChunkId == 0 ) {
				original = socket = this->getSlaves( header.listId, header.srcDataChunkId );
				sockfd = socket->getSocket();
				MasterWorker::slaveSockets->get( sockfd )->counter.decreaseDegraded();
				MasterWorker::slaveSockets->get( sockfd )->counter.increaseNormal();
				Master::getInstance()->remapMsgHandler.ackTransit( socket->getAddr() );
			} else {
				original = this->getSlaves( header.listId, header.srcDataChunkId );

				// Decrease degraded counter for the parity server
				socket = this->getSlaves( header.listId, header.srcParityChunkId );
				sockfd = socket->getSocket();
				MasterWorker::slaveSockets->get( sockfd )->counter.decreaseDegraded();
				Master::getInstance()->remapMsgHandler.ackTransit( socket->getAddr() );

				// Increase normal counter for the data server
				sockfd = original->getSocket();
				MasterWorker::slaveSockets->get( sockfd )->counter.increaseNormal();
				Master::getInstance()->remapMsgHandler.ackTransit( original->getAddr() );

				socket = original;
			}

			break;
		case PROTO_DEGRADED_LOCK_RES_REMAPPED:
			__DEBUG__(
				BLUE, "MasterWorker", "handleDegradedLockResponse",
				"[Remapped] Key: %.*s (key size = %u); (%u, %u)",
				( int ) header.keySize, header.key, header.keySize,
				header.dstListId, header.dstChunkId
			);

			if ( header.srcParityChunkId == 0 ) {
				// data server is remapped
				socket = this->getSlaves( header.listId, header.srcDataChunkId );
				sockfd = socket->getSocket();
				MasterWorker::slaveSockets->get( sockfd )->counter.decreaseDegraded();

				original = socket = this->getSlaves( header.listId, header.dstDataChunkId );
				MasterWorker::slaveSockets->get( sockfd )->counter.increaseNormal();
				Master::getInstance()->remapMsgHandler.ackTransit( socket->getAddr() );
			} else {
				// parity server is remapped
				original = this->getSlaves( header.listId, header.srcDataChunkId );

				// Decrease degraded counter for the parity server
				socket = this->getSlaves( header.listId, header.srcParityChunkId );
				sockfd = socket->getSocket();
				MasterWorker::slaveSockets->get( sockfd )->counter.decreaseDegraded();
				Master::getInstance()->remapMsgHandler.ackTransit( socket->getAddr() );

				// Increase normal counter for the data server
				sockfd = original->getSocket();
				MasterWorker::slaveSockets->get( sockfd )->counter.increaseNormal();
				Master::getInstance()->remapMsgHandler.ackTransit( original->getAddr() );

				socket = original;
			}
			break;
		case PROTO_DEGRADED_LOCK_RES_NOT_EXIST:
		default:
			__DEBUG__(
				BLUE, "MasterWorker", "handleDegradedLockResponse",
				"[Not Fonud] Key: %.*s (key size = %u)",
				( int ) header.keySize, header.key, header.keySize
			);

			if ( header.srcParityChunkId == 0 ) {
				socket = this->getSlaves( header.listId, header.srcDataChunkId );
				sockfd = socket->getSocket();
				MasterWorker::slaveSockets->get( sockfd )->counter.decreaseDegraded();
				Master::getInstance()->remapMsgHandler.ackTransit( socket->getAddr() );
			} else {
				original = this->getSlaves( header.listId, header.srcDataChunkId );

				// Decrease degraded counter for the parity server
				socket = this->getSlaves( header.listId, header.srcParityChunkId );
				sockfd = socket->getSocket();
				MasterWorker::slaveSockets->get( sockfd )->counter.decreaseDegraded();
				Master::getInstance()->remapMsgHandler.ackTransit( socket->getAddr() );

				socket = original;
			}
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
	uint32_t requestId = MasterWorker::idGenerator->nextVal( this->workerId );

	// Find the corresponding request
	if ( ! MasterWorker::pending->eraseDegradedLockData( PT_COORDINATOR_DEGRADED_LOCK_DATA, event.id, event.socket, &pid, &degradedLockData ) ) {
		__ERROR__( "MasterWorker", "handleDegradedLockResponse", "Cannot find a pending coordinator DEGRADED_LOCK request that matches the response. This message will be discarded (key = %.*s).", header.keySize, header.key );
		return false;
	}

	// Send the degraded request to the slave
	switch( degradedLockData.opcode ) {
		case PROTO_OPCODE_GET:
			// Prepare GET request
			switch( header.type ) {
				case PROTO_DEGRADED_LOCK_RES_IS_LOCKED:
				case PROTO_DEGRADED_LOCK_RES_WAS_LOCKED:
					buffer.data = this->protocol.reqDegradedGet(
						buffer.size, requestId,
						header.listId, header.stripeId,
						header.srcDataChunkId, header.dstDataChunkId,
						header.dstParityChunkId, header.dstParityChunkId,
						header.isSealed,
						degradedLockData.key, degradedLockData.keySize
					);
					break;
				case PROTO_DEGRADED_LOCK_RES_NOT_LOCKED:
				case PROTO_DEGRADED_LOCK_RES_REMAPPED:
					buffer.data = this->protocol.reqGet(
						buffer.size, requestId,
						degradedLockData.key, degradedLockData.keySize
					);
					if ( MasterWorker::updateInterval )
						MasterWorker::pending->recordRequestStartTime( PT_SLAVE_GET, requestId, pid.parentId, ( void * ) socket, socket->getAddr() );
					break;
				case PROTO_DEGRADED_LOCK_RES_NOT_EXIST:
					if ( ! MasterWorker::pending->eraseKey( PT_APPLICATION_GET, pid.parentId, 0, &pid, &key, true, true, true, header.key ) ) {
						__ERROR__( "MasterWorker", "handleDegradedLockResponse", "Cannot find a pending application GET request that matches the response. This message will be discarded (key = %.*s).", header.keySize, header.key );
						return false;
					}
					applicationEvent.resGet( ( ApplicationSocket * ) key.ptr, pid.parentId, key );
					MasterWorker::eventQueue->insert( applicationEvent );
					return true;
			}

			// Insert into slave GET pending map
			key.set( degradedLockData.keySize, degradedLockData.key, ( void * ) original );
			if ( ! MasterWorker::pending->insertKey( PT_SLAVE_GET, requestId, pid.parentId, ( void * ) socket, key ) ) {
				__ERROR__( "MasterWorker", "handleDegradedLockResponse", "Cannot insert into slave GET pending map." );
			}
			break;
		case PROTO_OPCODE_UPDATE:
			switch( header.type ) {
				case PROTO_DEGRADED_LOCK_RES_IS_LOCKED:
				case PROTO_DEGRADED_LOCK_RES_WAS_LOCKED:
					buffer.data = this->protocol.reqDegradedUpdate(
						buffer.size, requestId,
						header.listId, header.stripeId,
						header.srcDataChunkId, header.dstDataChunkId,
						header.dstParityChunkId, header.dstParityChunkId,
						header.isSealed,
						degradedLockData.key, degradedLockData.keySize,
						degradedLockData.valueUpdate, degradedLockData.valueUpdateOffset, degradedLockData.valueUpdateSize
					);
					break;
				case PROTO_DEGRADED_LOCK_RES_NOT_LOCKED:
				case PROTO_DEGRADED_LOCK_RES_REMAPPED:
					buffer.data = this->protocol.reqUpdate(
						buffer.size, requestId,
						degradedLockData.key, degradedLockData.keySize,
						degradedLockData.valueUpdate, degradedLockData.valueUpdateOffset, degradedLockData.valueUpdateSize
					);
					break;
				case PROTO_DEGRADED_LOCK_RES_NOT_EXIST:
					if ( ! MasterWorker::pending->eraseKeyValueUpdate( PT_APPLICATION_UPDATE, pid.parentId, 0, &pid, &keyValueUpdate, true, true, true, header.key ) ) {
						__ERROR__( "MasterWorker", "handleDegradedLockResponse", "Cannot find a pending application UPDATE request that matches the response. This message will be discarded (key = %.*s).", header.keySize, header.key );
						return false;
					}
					delete[] ( ( char * )( keyValueUpdate.ptr ) );
					applicationEvent.resUpdate( ( ApplicationSocket * ) pid.ptr, pid.parentId, keyValueUpdate, false );
					MasterWorker::eventQueue->insert( applicationEvent );
					return true;
			}

			// Insert into slave UPDATE pending map
			keyValueUpdate.set( degradedLockData.keySize, degradedLockData.key, ( void * ) original );
			keyValueUpdate.offset = degradedLockData.valueUpdateOffset;
			keyValueUpdate.length = degradedLockData.valueUpdateSize;
			if ( ! MasterWorker::pending->insertKeyValueUpdate( PT_SLAVE_UPDATE, requestId, pid.parentId, ( void * ) socket, keyValueUpdate ) ) {
				__ERROR__( "MasterWorker", "handleUpdateRequest", "Cannot insert into slave UPDATE pending map." );
			}
			break;
		case PROTO_OPCODE_DELETE:
			switch( header.type ) {
				case PROTO_DEGRADED_LOCK_RES_IS_LOCKED:
				case PROTO_DEGRADED_LOCK_RES_WAS_LOCKED:
					buffer.data = this->protocol.reqDegradedDelete(
						buffer.size, requestId,
						header.listId, header.stripeId,
						header.srcDataChunkId, header.dstDataChunkId,
						header.dstParityChunkId, header.dstParityChunkId,
						header.isSealed,
						degradedLockData.key, degradedLockData.keySize
					);
					break;
				case PROTO_DEGRADED_LOCK_RES_NOT_LOCKED:
				case PROTO_DEGRADED_LOCK_RES_REMAPPED:
					buffer.data = this->protocol.reqDelete(
						buffer.size, requestId,
						degradedLockData.key, degradedLockData.keySize
					);
					break;
				case PROTO_DEGRADED_LOCK_RES_NOT_EXIST:
					if ( ! MasterWorker::pending->eraseKey( PT_APPLICATION_DEL, pid.parentId, 0, &pid, &key, true, true, true, header.key ) ) {
						__ERROR__( "MasterWorker", "handleDegradedLockResponse", "Cannot find a pending application DELETE request that matches the response. This message will be discarded (key = %.*s).", header.keySize, header.key );
						return false;
					}
					applicationEvent.resDelete( ( ApplicationSocket * ) key.ptr, pid.parentId, key, false );
					MasterWorker::eventQueue->insert( applicationEvent );
					return true;
			}

			// Insert into slave DELETE pending map
			key.set( degradedLockData.keySize, degradedLockData.key, ( void * ) original );
			if ( ! MasterWorker::pending->insertKey( PT_SLAVE_DEL, requestId, pid.parentId, ( void * ) socket, key ) ) {
				__ERROR__( "MasterWorker", "handleDegradedLockResponse", "Cannot insert into slave DELETE pending map." );
			}
			break;
		default:
			__ERROR__( "MasterWorker", "handleDegradedLockResponse", "Invalid opcode in DegradedLockData." );
			return false;
	}

	// Send request
	sentBytes = socket->send( buffer.data, buffer.size, connected );
	if ( sentBytes != ( ssize_t ) buffer.size ) {
		__ERROR__( "MasterWorker", "handleGetRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", sentBytes, buffer.size );
		return false;
	}

	return true;
}
