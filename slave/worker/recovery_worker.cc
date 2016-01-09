#include "worker.hh"
#include "../main/slave.hh"

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
	SlavePeerSocket *original, *s;
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

	s = new SlavePeerSocket();
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
		"This slave is promoted to replace %s:%s (chunk count = %u).",
		tmp, tmp + 16, header.count
	);

	// Find the slave peer socket in the array map
	int index = -1, sockfd = -1;
	SlavePeerSocket *original, *s;
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

	s = new SlavePeerSocket();
	s->init(
		sockfd, addr,
		&Slave::getInstance()->sockets.epoll,
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
		header.count,
		header.metadata
	);

	return true;
}

bool SlaveWorker::handleReconstructionRequest( CoordinatorEvent event, char *buf, size_t size ) {
	struct ReconstructionHeader header;
	std::unordered_set<uint32_t> stripeIds;
	std::unordered_set<uint32_t>::iterator it;
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
	SlavePeerSocket *socket = 0;

	// Check whether the number of surviving nodes >= k
	SlaveWorker::stripeList->get( header.listId, this->paritySlaveSockets, this->dataSlaveSockets );
	chunkCount = 0;
	for ( uint32_t i = 0; i < SlaveWorker::chunkCount; i++ ) {
		if ( i == header.chunkId )
			continue;
		socket = ( i < SlaveWorker::dataChunkCount ) ?
				 ( this->dataSlaveSockets[ i ] ) :
				 ( this->paritySlaveSockets[ i - SlaveWorker::dataChunkCount ] );
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
		stripeIds
	) ) {
		__ERROR__( "SlaveWorker", "handleReconstructionRequest", "Cannot insert into coordinator RECONSTRUCTION pending map." );
	}

	// Send GET_CHUNK requests to surviving nodes
	SlavePeerEvent slavePeerEvent;
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
						 ( this->dataSlaveSockets[ chunkId ] ) :
						 ( this->paritySlaveSockets[ chunkId - SlaveWorker::dataChunkCount ] );
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
			__ERROR__( "SlaveWorker", "performDegradedRead", "Cannot insert into slave CHUNK_REQUEST pending map." );
		}
	}
	// Send GET_CHUNK requests now
	for ( uint32_t i = 0; i < SlaveWorker::chunkCount; i++ ) {
		if ( metadataList[ i ]->size() > 0 ) {
			socket = ( i < SlaveWorker::dataChunkCount ) ?
					 ( this->dataSlaveSockets[ i ] ) :
					 ( this->paritySlaveSockets[ i - SlaveWorker::dataChunkCount ] );

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

bool SlaveWorker::handleCompletedReconstructionAck() {
	return Slave::getInstance()->initChunkBuffer();
}
