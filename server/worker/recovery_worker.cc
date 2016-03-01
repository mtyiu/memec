#include "worker.hh"
#include "../main/server.hh"

bool SlaveWorker::handleSlaveReconstructedMsg( CoordinatorEvent event, char *buf, size_t size ) {
	struct AddressHeader srcHeader, dstHeader;
	if ( ! this->protocol.parseSrcDstAddressHeader( srcHeader, dstHeader, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleSlaveReconstructedMsg", "Invalid address header." );
		return false;
	}

	char srcTmp[ 22 ], dstTmp[ 22 ];
	Socket::ntoh_ip( srcHeader.addr, srcTmp, 16 );
	Socket::ntoh_port( srcHeader.port, srcTmp + 16, 6 );
	Socket::ntoh_ip( dstHeader.addr, dstTmp, 16 );
	Socket::ntoh_port( dstHeader.port, dstTmp + 16, 6 );
	// __DEBUG__(
	// 	YELLOW,
	__ERROR__(
		"SlaveWorker", "handleSlaveReconstructedMsg",
		"Slave: %s:%s is reconstructed at %s:%s.",
		srcTmp, srcTmp + 16, dstTmp, dstTmp + 16
	);

	// Find the slave peer socket in the array map
	int index = -1, sockfd = -1;
	ServerPeerSocket *original, *s;
	Slave *slave = Slave::getInstance();
	bool self = false;

	for ( int i = 0, len = slavePeers->size(); i < len; i++ ) {
		if ( slavePeers->values[ i ]->equal( srcHeader.addr, srcHeader.port ) ) {
			index = i;
			original = slavePeers->values[ i ];
			break;
		}
	}
	if ( index == -1 ) {
		__ERROR__( "SlaveWorker", "handleSlaveReconstructedMsg", "The slave is not in the list. Ignoring this slave..." );
		return false;
	}
	original->stop();

	ServerAddr serverAddr( slavePeers->values[ index ]->identifier, dstHeader.addr, dstHeader.port );
	ServerAddr &me = slave->config.slave.slave.addr;

	// Check if this is a self-socket
	self = ( dstHeader.addr == me.addr && dstHeader.port == me.port );

	if ( self ) {
		ServerAddr src( 0, srcHeader.addr, srcHeader.port );
		int mySlaveIndex = -1;

		for ( int i = 0, len = slave->config.global.slaves.size(); i < len; i++ ) {
			if ( ServerAddr::match( &slave->config.global.slaves[ i ], &src ) ) {
				mySlaveIndex = i;
				break;
			}
		}

		slave->init( mySlaveIndex );
	}

	s = new ServerPeerSocket();
	s->init(
		sockfd, serverAddr,
		&Slave::getInstance()->sockets.epoll,
		self // self-socket
	);

	// Update sockfd in the array Map
	if ( self ) {
		sockfd = original->getSocket();
	} else {
		sockfd = s->init();
	}
	slavePeers->set( index, sockfd, s );
	SlaveWorker::stripeList->update();
	delete original;

	// Connect to the slave peer
	if ( ! self )
		s->start();

	// Send response to the coordinator
	event.resSlaveReconstructedMsg( event.socket, event.instanceId, event.requestId );
	this->dispatch( event );

	return true;
}

bool SlaveWorker::handleBackupSlavePromotedMsg( CoordinatorEvent event, char *buf, size_t size ) {
	struct PromoteBackupSlaveHeader header;
	if ( ! this->protocol.parsePromoteBackupSlaveHeader( header, true /* isRequest */, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleBackupSlavePromotedMsg", "Invalid promote backup slave header." );
		return false;
	}

	char tmp[ 22 ];
	Socket::ntoh_ip( header.addr, tmp, 16 );
	Socket::ntoh_port( header.port, tmp + 16, 6 );
	// __DEBUG__(
	// 	YELLOW,
	__ERROR__(
		"SlaveWorker", "handleBackupSlavePromotedMsg",
		"This slave is promoted to replace %s:%s (chunk count = %u, unsealed key count = %u).",
		tmp, tmp + 16, header.chunkCount, header.unsealedCount
	);

	// Find the slave peer socket in the array map
	int index = -1, sockfd = -1;
	ServerPeerSocket *original, *s;
	Slave *slave = Slave::getInstance();
	bool self = false;

	for ( int i = 0, len = slavePeers->size(); i < len; i++ ) {
		if ( slavePeers->values[ i ]->equal( header.addr, header.port ) ) {
			index = i;
			original = slavePeers->values[ i ];
			break;
		}
	}
	if ( index == -1 ) {
		__ERROR__( "SlaveWorker", "handleBackupSlavePromotedMsg", "The slave is not in the list. Ignoring this slave..." );
		return false;
	}
	original->stop();

	// Initialize
	ServerAddr addr( 0, header.addr, header.port );
	slave->init( index );

	s = new ServerPeerSocket();
	s->init(
		sockfd, addr,
		&( slave->sockets.epoll ),
		true
	);

	// Update sockfd in the array Map
	if ( self ) {
		sockfd = original->getSocket();
	} else {
		sockfd = s->init();
	}
	slavePeers->set( index, sockfd, s );
	SlaveWorker::stripeList->update();
	delete original;

	// Insert the metadata into pending recovery map
	SlaveWorker::pending->insertRecovery(
		event.instanceId, event.requestId,
		event.socket,
		header.addr,
		header.port,
		header.chunkCount, header.metadata,
		header.unsealedCount, header.keys
	);

	return true;
}

bool SlaveWorker::handleReconstructionRequest( CoordinatorEvent event, char *buf, size_t size ) {
	struct ReconstructionHeader header;
	std::unordered_set<uint32_t> stripeIds;
	std::unordered_set<uint32_t>::iterator it;
	std::unordered_set<Key> unsealedKeys;
	if ( ! this->protocol.parseReconstructionHeader( header, true, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleReconstructionRequest", "Invalid RECONSTRUCTION request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleReconstructionRequest",
		"[RECONSTRUCTION] List ID: %u; chunk ID: %u; number of stripes: %u.",
		header.listId, header.chunkId, header.numStripes
	);

	if ( header.numStripes == 0 ) {
		event.resReconstruction(
			event.socket, event.instanceId, event.requestId,
			header.listId, header.chunkId, header.numStripes
		);
		this->dispatch( event );
		return true;
	}

	uint16_t instanceId = Slave::instanceId;
	uint32_t chunkId, myChunkId = SlaveWorker::chunkCount, chunkCount, requestId;
	ChunkRequest chunkRequest;
	Metadata metadata;
	Chunk *chunk;
	ServerPeerSocket *socket = 0;

	// Check whether the number of surviving nodes >= k
	SlaveWorker::stripeList->get( header.listId, this->parityServerSockets, this->dataServerSockets );
	chunkCount = 0;
	for ( uint32_t i = 0; i < SlaveWorker::chunkCount; i++ ) {
		if ( i == header.chunkId )
			continue;
		socket = ( i < SlaveWorker::dataChunkCount ) ?
				 ( this->dataServerSockets[ i ] ) :
				 ( this->parityServerSockets[ i - SlaveWorker::dataChunkCount ] );
		if ( socket->ready() ) chunkCount++;
		if ( socket->self ) myChunkId = i;
	}
	assert( myChunkId < SlaveWorker::chunkCount );
	if ( chunkCount < SlaveWorker::dataChunkCount ) {
		__ERROR__( "SlaveWorker", "handleReconstructionRequest", "The number of surviving nodes (%u) is less than k (%u). The data cannot be recovered.", chunkCount, SlaveWorker::dataChunkCount );
		return false;
	}

	// Insert into pending set
	for ( uint32_t i = 0; i < header.numStripes; i++ )
		stripeIds.insert( header.stripeIds[ i ] );
	if ( ! SlaveWorker::pending->insertReconstruction(
		event.instanceId, event.requestId,
		event.socket,
		header.listId,
		header.chunkId,
		stripeIds,
		unsealedKeys
	) ) {
		__ERROR__( "SlaveWorker", "handleReconstructionRequest", "Cannot insert into coordinator RECONSTRUCTION pending map." );
	}

	// Send GET_CHUNK requests to surviving nodes
	ServerPeerEvent slavePeerEvent;
	std::vector<uint32_t> **requestIds = new std::vector<uint32_t> *[ SlaveWorker::chunkCount ];
	std::vector<Metadata> **metadataList = new std::vector<Metadata> *[ SlaveWorker::chunkCount ];
	for ( uint32_t i = 0; i < SlaveWorker::chunkCount; i++ ) {
		requestIds[ i ] = new std::vector<uint32_t>();
		metadataList[ i ] = new std::vector<Metadata>();
	}
	chunkId = 0;
	for ( it = stripeIds.begin(); it != stripeIds.end(); it++ ) {
		// printf( "Processing (%u, %u, %u)...\n", header.listId, *it, header.chunkId );
		chunkCount = 0;
		requestId = SlaveWorker::idGenerator->nextVal( this->workerId );
		metadata.listId = header.listId;
		metadata.stripeId = *it;
		while( chunkCount < SlaveWorker::dataChunkCount - 1 ) {
			if ( chunkId != header.chunkId ) { // skip the chunk to be reconstructed
				socket = ( chunkId < SlaveWorker::dataChunkCount ) ?
						 ( this->dataServerSockets[ chunkId ] ) :
						 ( this->parityServerSockets[ chunkId - SlaveWorker::dataChunkCount ] );
				if ( socket->ready() && ! socket->self ) { // use this slave
					metadata.chunkId = chunkId;
					chunkRequest.set(
						metadata.listId, metadata.stripeId, metadata.chunkId,
						socket, 0, false
					);
					if ( ! SlaveWorker::pending->insertChunkRequest( PT_SLAVE_PEER_GET_CHUNK, instanceId, event.instanceId, requestId, event.requestId, socket, chunkRequest ) ) {
						__ERROR__( "SlaveWorker", "handleReconstructionRequest", "Cannot insert into slave CHUNK_REQUEST pending map." );
					} else {
						requestIds[ chunkId ]->push_back( requestId );
						metadataList[ chunkId ]->push_back( metadata );
						chunkCount++;
					}
				}
			}
			chunkId++;
			if ( chunkId >= SlaveWorker::chunkCount )
				chunkId = 0;
		}
		// Use own chunk
		chunk = SlaveWorker::map->findChunkById( metadata.listId, metadata.stripeId, myChunkId );
		if ( ! chunk ) {
			chunk = Coding::zeros;
		} else {
			// Check whether the chunk is sealed or not
			MixedChunkBuffer *chunkBuffer = SlaveWorker::chunkBuffer->at( metadata.listId );
			int chunkBufferIndex = chunkBuffer->lockChunk( chunk, true );
			bool isSealed = ( chunkBufferIndex == -1 );
			if ( ! isSealed )
				chunk = Coding::zeros;
			chunkBuffer->unlock( chunkBufferIndex );
		}
		chunkRequest.set(
			metadata.listId, metadata.stripeId, myChunkId,
			0, chunk, false
		);
		if ( ! SlaveWorker::pending->insertChunkRequest( PT_SLAVE_PEER_GET_CHUNK, instanceId, event.instanceId, requestId, event.requestId, 0, chunkRequest ) ) {
			__ERROR__( "SlaveWorker", "handleReconstructionRequest", "Cannot insert into slave CHUNK_REQUEST pending map." );
		}
	}
	// Send GET_CHUNK requests now
	for ( uint32_t i = 0; i < SlaveWorker::chunkCount; i++ ) {
		if ( metadataList[ i ]->size() > 0 ) {
			socket = ( i < SlaveWorker::dataChunkCount ) ?
					 ( this->dataServerSockets[ i ] ) :
					 ( this->parityServerSockets[ i - SlaveWorker::dataChunkCount ] );

			slavePeerEvent.batchGetChunks( socket, requestIds[ i ], metadataList[ i ] );
			// SlaveWorker::eventQueue->insert( slavePeerEvent );
			this->dispatch( slavePeerEvent );
		} else {
			delete requestIds[ i ];
			delete metadataList[ i ];
		}
	}
	delete[] requestIds;
	delete[] metadataList;

	return false;
}

bool SlaveWorker::handleReconstructionUnsealedRequest( CoordinatorEvent event, char *buf, size_t size ) {
	struct BatchKeyHeader header;
	if ( ! this->protocol.parseBatchKeyHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleReconstructionUnsealedRequest", "Invalid RECONSTRUCTION_UNSEALED request." );
		return false;
	}

	// printf( "handleReconstructionUnsealedRequest: %u\n", header.count );

	std::unordered_set<Key> unsealedKeys;
	std::unordered_set<Key>::iterator unsealedKeysIt;
	std::unordered_set<uint32_t> stripeIds;
	uint32_t listId, chunkId;
	ServerPeerSocket *reconstructedSlave = 0;

	std::unordered_map<Key, KeyValue> *keyValueMap;
	LOCK_T *lock;

	for ( uint32_t i = 0, offset = 0; i < header.count; i++ ) {
		uint8_t keySize;
		char *keyStr;
		Key key;

		this->protocol.nextKeyInBatchKeyHeader( header, keySize, keyStr, offset );

		if ( i == 0 ) {
			reconstructedSlave = this->getSlaves( keyStr, keySize, listId, chunkId );

			if ( ! SlaveWorker::chunkBuffer->at( listId )->getKeyValueMap( keyValueMap, lock ) ) {
				__ERROR__(
					"SlaveWorker", "handleReconstructionUnsealedRequest",
					"Cannot get key-value map."
				);
				return false;
			}
		}

		key.set( keySize, keyStr );
		key.dup();
		unsealedKeys.insert( key );
		// printf(
		// 	"[%u] (%u) %.*s / (%u) %.*s\n",
		// 	i, key.size, key.size, key.data,
		// 	keySize, keySize, keyStr
		// );
	}

	if ( ! SlaveWorker::pending->insertReconstruction(
		event.instanceId, event.requestId,
		event.socket,
		listId, chunkId,
		stripeIds,
		unsealedKeys
	) ) {
		__ERROR__( "SlaveWorker", "handleReconstructionUnsealedRequest", "Cannot insert into coordinator RECONSTRUCTION pending map." );
	}

	// Send unsealed key-value pairs
	bool isCompleted = false;
	struct {
		size_t size;
		char *data;
	} buffer;
	uint32_t requestId;
	uint32_t keyValuesCount = 0, totalKeyValuesCount = 0;

	unsealedKeysIt = unsealedKeys.begin();
	while ( ! isCompleted ) {
		requestId = SlaveWorker::idGenerator->nextVal( this->workerId );
		if ( ! SlaveWorker::pending->insert(
			PT_SLAVE_PEER_FORWARD_KEYS,
			Slave::instanceId, event.instanceId,
			requestId, event.requestId,
			( void * ) reconstructedSlave
		) ) {
			__ERROR__( "SlaveWorker", "handleReconstructionUnsealedRequest", "Cannot insert into pending set." );
		}

		buffer.data = this->protocol.sendUnsealedKeys(
			buffer.size, Slave::instanceId, requestId,
			unsealedKeys, unsealedKeysIt,
			keyValueMap, lock,
			keyValuesCount,
			isCompleted
		);
		totalKeyValuesCount += keyValuesCount;

		if ( reconstructedSlave ) {
			bool connected;
			ssize_t ret;

			ret = reconstructedSlave->send( buffer.data, buffer.size, connected );
			if ( ret != ( ssize_t ) buffer.size )
				__ERROR__( "SlaveWorker", "handleReconstructionUnsealedRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
		} else {
			__ERROR__( "SlaveWorker", "handleReconstructionUnsealedRequest", "Reconstructed slave not available!" );
		}
	}
	// printf( "Total number of unsealed key-values sent: %u\n", totalKeyValuesCount );

	return true;
}

bool SlaveWorker::handleCompletedReconstructionAck() {
	return Slave::getInstance()->initChunkBuffer();
}
