#include "worker.hh"
#include "../main/slave.hh"

#define BATCH_THRESHOLD		20
static struct timespec BATCH_INTVL = { 0, 500000 };

bool SlaveWorker::handleRemappedParity( CoordinatorEvent event, char *buf, size_t size ) {
	struct AddressHeader header;
	if ( ! this->protocol.parseAddressHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleRemappedParity", "Invalid Remapped Parity Sync request." );
		return false;
	}

	SlavePeerEvent slavePeerEvent;
	Slave *slave = Slave::getInstance();

	struct sockaddr_in target;
	target.sin_addr.s_addr = header.addr;
	target.sin_port = header.port;
	SlavePeerSocket *socket = 0;
	for ( uint32_t i = 0; i < slave->sockets.slavePeers.size(); i++ ) {
		if ( slave->sockets.slavePeers.values[ i ]->getAddr() == target ) {
			socket = slave->sockets.slavePeers.values[ i ];
			break;
		}
	}
	if ( socket == 0 )
		__ERROR__( "SlaveWorker", "handleRemappedparity", "Cannot find slave sccket to send remapped parity back." );

	std::set<PendingData> *remappedParity;
	bool found = SlaveWorker::pending->eraseRemapData( target, &remappedParity );

	if ( found && socket ) {
		uint32_t requestId = SlaveWorker::idGenerator->nextVal( this->workerId );
		// TODO insert into pending set to wait for acknowledgement
		SlaveWorker::pending->insertRemapDataRequest( requestId, event.id, remappedParity->size(), socket );
		// dispatch one event for each key
		uint32_t requestSent = 0;
		for ( PendingData pendingData : *remappedParity ) {
			slavePeerEvent.reqSet( socket, requestId, pendingData.listId, pendingData.chunkId, pendingData.key, pendingData.value );
			slave->eventQueue.insert( slavePeerEvent );
			requestSent++;
			if ( requestSent % BATCH_THRESHOLD == 0 ) {
				nanosleep( &BATCH_INTVL, 0 );
				requestSent = 0;
			}
		}
		delete remappedParity;
	} else {
		event.resRemappedParity();
		this->dispatch( event );
		// slave not found, but remapped parity found.. discard the parity
		if ( found ) delete remappedParity;
	}
	return false;
}

bool SlaveWorker::handleRemappingSetRequest( MasterEvent event, char *buf, size_t size ) {
	struct RemappingSetHeader header;
	struct sockaddr_in targetAddr;
	targetAddr.sin_addr.s_addr = 0;
	if ( ! this->protocol.parseRemappingSetHeader( header, buf, size, 0, &targetAddr ) ) {
		__ERROR__( "SlaveWorker", "handleRemappingSetRequest", "Invalid REMAPPING_SET request (size = %lu).", size );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleRemappingSetRequest",
		"[SET] Key: %.*s (key size = %u); Value: (value size = %u); list ID = %u, chunk ID = %u; needs forwarding? %s (ID: %u)",
		( int ) header.keySize, header.key, header.keySize, header.valueSize,
		header.listId, header.chunkId, header.needsForwarding ? "true" : "false",
		event.id
	);

	uint32_t originalListId, originalChunkId;
	SlavePeerSocket *dataSlaveSocket = this->getSlaves( header.key, header.keySize, originalListId, originalChunkId );
	struct sockaddr_in selfAddr = Slave::getInstance()->sockets.self.getAddr();
	bool bufferRemapData = true;
	if ( ! header.remapped /* data rempped ?*/ ) {
		if ( selfAddr == dataSlaveSocket->getAddr() ) {
			bufferRemapData = false;
		}
	} else if ( targetAddr.sin_addr.s_addr == 0 || targetAddr == selfAddr ) { /* parity remapped ?*/
		bufferRemapData = false;
	}

	if ( header.remapped && bufferRemapData ) { // only buffer remapped parity for the time being
		Key key;
		key.set( header.keySize, header.key );
		Value value;
		value.set( header.valueSize, header.value );
		SlaveWorker::pending->insertRemapData( targetAddr, header.listId, header.chunkId, key, value );
	} else {
		SlaveWorker::chunkBuffer->at( header.listId )->set(
			this,
			header.key, header.keySize,
			header.value, header.valueSize,
			PROTO_OPCODE_REMAPPING_SET, header.chunkId,
			this->chunks, this->dataChunk, this->parityChunk
		);
	}

	Key key;
	key.set( header.keySize, header.key );
	event.resRemappingSet( event.socket, event.id, key, header.listId, header.chunkId, true, false, header.sockfd, header.remapped );
	this->dispatch( event );

	return true;
}

bool SlaveWorker::handleRemappingSetRequest( SlavePeerEvent event, char *buf, size_t size ) {
	struct RemappingSetHeader header;
	if ( ! this->protocol.parseRemappingSetHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleRemappingSetRequest", "Invalid REMAPPING_SET request (size = %lu).", size );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleRemappingSetRequest",
		"[SET] Key: %.*s (key size = %u); Value: (value size = %u); list ID = %u, chunk ID = %u; needs forwarding? %s",
		( int ) header.keySize, header.key, header.keySize, header.valueSize,
		header.listId, header.chunkId, header.needsForwarding ? "true" : "false"
	);

	SlaveWorker::chunkBuffer->at( header.listId )->set(
		this,
		header.key, header.keySize,
		header.value, header.valueSize,
		PROTO_OPCODE_REMAPPING_SET,
		SlaveWorker::chunkBuffer->at( header.listId )->getChunkId(),
		this->chunks, this->dataChunk, this->parityChunk
	);

	Key key;
	key.set( header.keySize, header.key );
	event.resRemappingSet( event.socket, event.id, key, header.listId, header.chunkId, true );
	this->dispatch( event );

	return true;
}

bool SlaveWorker::handleRemappingSetResponse( SlavePeerEvent event, bool success, char *buf, size_t size ) {
	struct RemappingLockHeader header;
	if ( ! this->protocol.parseRemappingLockHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleRemappingSetResponse", "Invalid REMAPPING_SET response (size = %lu).", size );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleRemappingSetResponse",
		"[REMAPPING_SET] Key: %.*s (key size = %u); list ID: %u, chunk ID: %u",
		( int ) header.keySize, header.key, header.keySize, header.listId, header.chunkId
	);

	int pending;
	PendingIdentifier pid;
	RemappingRecordKey record;

	if ( ! SlaveWorker::pending->eraseRemappingRecordKey( PT_SLAVE_PEER_REMAPPING_SET, event.id, event.socket, &pid, &record, true, false ) ) {
		UNLOCK( &SlaveWorker::pending->slavePeers.remappingSetLock );
		__ERROR__( "SlaveWorker", "handleRemappingSetResponse", "Cannot find a pending slave REMAPPING_SET request that matches the response. This message will be discarded. (ID: %u)", event.id );
		return false;
	}
	// Check pending slave UPDATE requests
	pending = SlaveWorker::pending->count( PT_SLAVE_PEER_REMAPPING_SET, pid.id, false, true );

	__DEBUG__( BLUE, "SlaveWorker", "handleRemappingSetResponse", "Pending slave REMAPPING_SET requests = %d (%s).", pending, success ? "success" : "fail" );
	if ( pending == 0 ) {
		// Only send master REMAPPING_SET response when the number of pending slave REMAPPING_SET requests equal 0
		MasterEvent masterEvent;

		if ( ! SlaveWorker::pending->eraseRemappingRecordKey( PT_MASTER_REMAPPING_SET, pid.parentId, 0, &pid, &record ) ) {
			__ERROR__( "SlaveWorker", "handleRemappingSetResponse", "Cannot find a pending master REMAPPING_SET request that matches the response. This message will be discarded." );
			return false;
		}

		masterEvent.resRemappingSet(
			( MasterSocket * ) pid.ptr, pid.id, record.key,
			record.remap.listId, record.remap.chunkId, success, true,
			header.sockfd, header.isRemapped
		);
		SlaveWorker::eventQueue->insert( masterEvent );
	}

	return true;
}