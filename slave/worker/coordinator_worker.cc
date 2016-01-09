#include "worker.hh"
#include "../main/slave.hh"

void SlaveWorker::dispatch( CoordinatorEvent event ) {
	bool connected, isSend;
	uint32_t requestId;
	ssize_t ret;
	struct {
		size_t size;
		char *data;
	} buffer;

	requestId = SlaveWorker::idGenerator->nextVal( this->workerId );

	switch( event.type ) {
		case COORDINATOR_EVENT_TYPE_REGISTER_REQUEST:
			requestId = SlaveWorker::idGenerator->nextVal( this->workerId );
			buffer.data = this->protocol.reqRegisterCoordinator(
				buffer.size,
				requestId,
				event.message.address.addr,
				event.message.address.port
			);
			isSend = true;
			break;
		case COORDINATOR_EVENT_TYPE_SYNC:
		{
			uint32_t sealedCount, opsCount;
			bool isCompleted;

			uint32_t timestamp = SlaveWorker::timestamp->nextVal();

			buffer.data = this->protocol.sendHeartbeat(
				buffer.size,
				event.instanceId, event.requestId,
				timestamp,
				&SlaveWorker::map->sealedLock, SlaveWorker::map->sealed, sealedCount,
				&SlaveWorker::map->opsLock, SlaveWorker::map->ops, opsCount,
				isCompleted
			);

			if ( sealedCount || opsCount )
				SlaveWorker::pendingAck->insert( timestamp );

			if ( ! isCompleted )
				SlaveWorker::eventQueue->insert( event );

			isSend = true;
		}
			break;
		case COORDINATOR_EVENT_TYPE_RELEASE_DEGRADED_LOCK_RESPONSE_SUCCESS:
			buffer.data = this->protocol.resReleaseDegradedLock(
				buffer.size,
				event.instanceId, event.requestId,
				event.message.degraded.count
			);
			isSend = true;
			break;
		case COORDINATOR_EVENT_TYPE_RECONSTRUCTION_RESPONSE_SUCCESS:
			buffer.data = this->protocol.resReconstruction(
				buffer.size,
				event.instanceId, event.requestId,
				event.message.reconstruction.listId,
				event.message.reconstruction.chunkId,
				event.message.reconstruction.numStripes
			);
			isSend = true;
			break;
		case COORDINATOR_EVENT_TYPE_PROMOTE_BACKUP_SERVER_RESPONSE_SUCCESS:
			buffer.data = this->protocol.resPromoteBackupSlave(
				buffer.size,
				event.instanceId, event.requestId,
				event.message.promote.addr,
				event.message.promote.port,
				event.message.promote.count
			);
			isSend = true;
			break;
		case COORDINATOR_EVENT_TYPE_RESPONSE_PARITY_MIGRATE:
			buffer.data = this->protocol.resRemapParity(
				buffer.size,
				event.instanceId, event.requestId
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
			__ERROR__( "SlaveWorker", "dispatch", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
	} else {
		ProtocolHeader header;
		WORKER_RECEIVE_FROM_EVENT_SOCKET();
		while ( buffer.size > 0 ) {
			WORKER_RECEIVE_WHOLE_MESSAGE_FROM_EVENT_SOCKET( "SlaveWorker" );

			buffer.data += PROTO_HEADER_SIZE;
			buffer.size -= PROTO_HEADER_SIZE;
			// Validate message
			if ( header.from != PROTO_MAGIC_FROM_COORDINATOR ) {
				__ERROR__( "SlaveWorker", "dispatch", "Invalid message source from coordinator." );
			} else {
				event.instanceId = header.instanceId;
				event.requestId = header.requestId;
				switch( header.opcode ) {
					case PROTO_OPCODE_REGISTER:
						switch( header.magic ) {
							case PROTO_MAGIC_RESPONSE_SUCCESS:
								event.socket->registered = true;
								Slave::instanceId = header.instanceId;
								break;
							case PROTO_MAGIC_RESPONSE_FAILURE:
								__ERROR__( "SlaveWorker", "dispatch", "Failed to register with coordinator." );
								break;
							default:
								__ERROR__( "SlaveWorker", "dispatch", "Invalid magic code from coordinator." );
								break;
						}
						break;
					case PROTO_OPCODE_SLAVE_CONNECTED:
						this->handleSlaveConnectedMsg( event, buffer.data, header.length );
						break;
					case PROTO_OPCODE_SLAVE_RECONSTRUCTED:
						this->handleSlaveReconstructedMsg( event, buffer.data, header.length );
						break;
					case PROTO_OPCODE_BACKUP_SLAVE_PROMOTED:
						this->handleBackupSlavePromotedMsg( event, buffer.data, header.length );
						break;
					case PROTO_OPCODE_SEAL_CHUNKS:
						Slave::getInstance()->seal();
						break;
					case PROTO_OPCODE_FLUSH_CHUNKS:
						Slave::getInstance()->flush();
						break;
					case PROTO_OPCODE_SYNC_META:
						Slave::getInstance()->sync( header.requestId );
						break;
					case PROTO_OPCODE_RELEASE_DEGRADED_LOCKS:
						this->handleReleaseDegradedLockRequest( event, buffer.data, header.length );
						break;
					case PROTO_OPCODE_RECONSTRUCTION:
						switch( header.magic ) {
							case PROTO_MAGIC_REQUEST:
								this->handleReconstructionRequest( event, buffer.data, header.length );
								break;
							case PROTO_MAGIC_RESPONSE_SUCCESS:
								this->handleCompletedReconstructionAck();
								break;
							case PROTO_MAGIC_RESPONSE_FAILURE:
							default:
								__ERROR__( "SlaveWorker", "dispatch", "Invalid magic code from coordinator." );
								break;
						}
						break;
					case PROTO_OPCODE_PARITY_MIGRATE:
						this->handleRemappedData( event, buffer.data, buffer.size );
						break;
					case PROTO_OPCODE_SYNC:
						this->handleHeartbeatAck( event, buffer.data, header.length );
						break;
					default:
						__ERROR__( "SlaveWorker", "dispatch", "Invalid opcode from coordinator." );
						break;
				}
			}

			buffer.data += header.length;
			buffer.size -= header.length;
		}
		if ( connected ) event.socket->done();
	}
	if ( ! connected )
		__ERROR__( "SlaveWorker", "dispatch", "The coordinator is disconnected." );
}

bool SlaveWorker::handleSlaveConnectedMsg( CoordinatorEvent event, char *buf, size_t size ) {
	struct AddressHeader header;
	if ( ! this->protocol.parseAddressHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleSlaveConnectedMsg", "Invalid address header." );
		return false;
	}

	char tmp[ 22 ];
	Socket::ntoh_ip( header.addr, tmp, 16 );
	Socket::ntoh_port( header.port, tmp + 16, 6 );
	__DEBUG__( YELLOW, "SlaveWorker", "handleSlaveConnectedMsg", "Slave: %s:%s is connected.", tmp, tmp + 16 );

	// Find the slave peer socket in the array map
	int index = -1;
	for ( int i = 0, len = slavePeers->size(); i < len; i++ ) {
		if ( slavePeers->values[ i ]->equal( header.addr, header.port ) ) {
			index = i;
			break;
		}
	}
	if ( index == -1 ) {
		__ERROR__( "SlaveWorker", "handleSlaveConnectedMsg", "The slave is not in the list. Ignoring this slave..." );
		return false;
	}

	// Update sockfd in the array Map
	int sockfd = slavePeers->values[ index ]->init();
	slavePeers->keys[ index ] = sockfd;

	// Connect to the slave peer
	slavePeers->values[ index ]->start();

	return true;
}

bool SlaveWorker::handleHeartbeatAck( CoordinatorEvent event, char *buf, size_t size ) {
	struct HeartbeatHeader header;
	if ( ! this->protocol.parseHeartbeatHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleHeartbeatAck", "Invalid acknowledgement header." );
		return false;
	}

	__DEBUG__( YELLOW, "SlaveWorker", "handleAcknowledgement", "Timestamp: %u.", header.timestamp );

	uint32_t fromTimestamp;
	if ( SlaveWorker::pendingAck->erase( header.timestamp, fromTimestamp ) ) {
		// Send ACK to masters
		MasterEvent masterEvent;
		uint16_t instanceId = Slave::instanceId;
		uint32_t requestId = SlaveWorker::idGenerator->nextVal( this->workerId );

		Slave *slave = Slave::getInstance();
		LOCK_T *lock = &slave->sockets.masters.lock;
		std::vector<MasterSocket *> &sockets = slave->sockets.masters.values;

		LOCK( lock );
		for ( size_t i = 0, size = sockets.size(); i < size; i++ ) {
			masterEvent.ackMetadata( sockets[ i ], instanceId, requestId, fromTimestamp, header.timestamp );
			SlaveWorker::eventQueue->insert( masterEvent );
		}
		UNLOCK( lock );
	}
	return true;
}
