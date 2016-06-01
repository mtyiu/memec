#include "worker.hh"
#include "../main/server.hh"

bool ServerWorker::handleServerReconstructedMsg( CoordinatorEvent event, char *buf, size_t size ) {
	struct AddressHeader srcHeader, dstHeader;
	if ( ! this->protocol.parseSrcDstAddressHeader( srcHeader, dstHeader, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleServerReconstructedMsg", "Invalid address header." );
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
		"ServerWorker", "handleServerReconstructedMsg",
		"Server: %s:%s is reconstructed at %s:%s.",
		srcTmp, srcTmp + 16, dstTmp, dstTmp + 16
	);

	// Find the server peer socket in the array map
	int index = -1, sockfd = -1;
	ServerPeerSocket *original, *s;
	Server *server = Server::getInstance();
	bool self = false;

	for ( int i = 0, len = serverPeers->size(); i < len; i++ ) {
		if ( serverPeers->values[ i ]->equal( srcHeader.addr, srcHeader.port ) ) {
			index = i;
			original = serverPeers->values[ i ];
			break;
		}
	}
	if ( index == -1 ) {
		__ERROR__( "ServerWorker", "handleServerReconstructedMsg", "The server is not in the list. Ignoring this server..." );
		return false;
	}
	original->stop();

	ServerAddr serverAddr( serverPeers->values[ index ]->identifier, dstHeader.addr, dstHeader.port );
	ServerAddr &me = server->config.server.server.addr;

	// Check if this is a self-socket
	self = ( dstHeader.addr == me.addr && dstHeader.port == me.port );

	if ( self ) {
		ServerAddr src( 0, srcHeader.addr, srcHeader.port );
		int myServerIndex = -1;

		for ( int i = 0, len = server->config.global.servers.size(); i < len; i++ ) {
			if ( ServerAddr::match( &server->config.global.servers[ i ], &src ) ) {
				myServerIndex = i;
				break;
			}
		}

		server->init( myServerIndex );
	}

	s = new ServerPeerSocket();
	s->init(
		sockfd, serverAddr,
		&Server::getInstance()->sockets.epoll,
		self // self-socket
	);

	// Update sockfd in the array Map
	if ( self ) {
		sockfd = original->getSocket();
	} else {
		sockfd = s->init();
	}
	serverPeers->set( index, sockfd, s );
	ServerWorker::stripeList->update();
	delete original;

	// Connect to the server peer
	if ( ! self )
		s->start();

	// Send response to the coordinator
	event.resServerReconstructedMsg( event.socket, event.instanceId, event.requestId );
	this->dispatch( event );

	return true;
}

bool ServerWorker::handleBackupServerPromotedMsg( CoordinatorEvent event, char *buf, size_t size ) {
	struct PromoteBackupServerHeader header;
	if ( ! this->protocol.parsePromoteBackupServerHeader( header, true /* isRequest */, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleBackupServerPromotedMsg", "Invalid promote backup server header." );
		return false;
	}

	char tmp[ 22 ];
	Socket::ntoh_ip( header.addr, tmp, 16 );
	Socket::ntoh_port( header.port, tmp + 16, 6 );
	// __DEBUG__(
	// 	YELLOW,
	__ERROR__(
		"ServerWorker", "handleBackupServerPromotedMsg",
		"This server is promoted to replace %s:%s (chunk count = %u, unsealed key count = %u).",
		tmp, tmp + 16, header.chunkCount, header.unsealedCount
	);

	// Find the server peer socket in the array map
	int index = -1, sockfd = -1;
	ServerPeerSocket *original, *s;
	Server *server = Server::getInstance();
	bool self = false;

	for ( int i = 0, len = serverPeers->size(); i < len; i++ ) {
		if ( serverPeers->values[ i ]->equal( header.addr, header.port ) ) {
			index = i;
			original = serverPeers->values[ i ];
			break;
		}
	}
	if ( index == -1 ) {
		__ERROR__( "ServerWorker", "handleBackupServerPromotedMsg", "The server is not in the list. Ignoring this server..." );
		return false;
	}
	original->stop();

	// Initialize
	ServerAddr addr( 0, header.addr, header.port );
	server->init( index );

	s = new ServerPeerSocket();
	s->init(
		sockfd, addr,
		&( server->sockets.epoll ),
		true
	);

	// Update sockfd in the array Map
	if ( self ) {
		sockfd = original->getSocket();
	} else {
		sockfd = s->init();
	}
	serverPeers->set( index, sockfd, s );
	ServerWorker::stripeList->update();
	delete original;

	// Insert the metadata into pending recovery map
	ServerWorker::pending->insertRecovery(
		event.instanceId, event.requestId,
		event.socket,
		header.addr,
		header.port,
		header.chunkCount, header.metadata,
		header.unsealedCount, header.keys
	);

	return true;
}

bool ServerWorker::handleReconstructionRequest( CoordinatorEvent event, char *buf, size_t size ) {
	struct ReconstructionHeader header;
	std::unordered_set<uint32_t> stripeIds;
	std::unordered_set<uint32_t>::iterator it;
	std::unordered_set<Key> unsealedKeys;
	if ( ! this->protocol.parseReconstructionHeader( header, true, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleReconstructionRequest", "Invalid RECONSTRUCTION request." );
		return false;
	}
	__DEBUG__(
		BLUE, "ServerWorker", "handleReconstructionRequest",
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

	uint16_t instanceId = Server::instanceId;
	uint32_t chunkId, myChunkId = ServerWorker::chunkCount, chunkCount, requestId;
	ChunkRequest chunkRequest;
	Metadata metadata;
	Chunk *chunk;
	ServerPeerSocket *socket = 0;

	// Check whether the number of surviving nodes >= k
	ServerWorker::stripeList->get( header.listId, this->parityServerSockets, this->dataServerSockets );
	chunkCount = 0;
	for ( uint32_t i = 0; i < ServerWorker::chunkCount; i++ ) {
		if ( i == header.chunkId )
			continue;
		socket = ( i < ServerWorker::dataChunkCount ) ?
				 ( this->dataServerSockets[ i ] ) :
				 ( this->parityServerSockets[ i - ServerWorker::dataChunkCount ] );
		if ( socket->ready() ) chunkCount++;
		if ( socket->self ) myChunkId = i;
	}
	assert( myChunkId < ServerWorker::chunkCount );
	if ( chunkCount < ServerWorker::dataChunkCount ) {
		__ERROR__( "ServerWorker", "handleReconstructionRequest", "The number of surviving nodes (%u) is less than k (%u). The data cannot be recovered.", chunkCount, ServerWorker::dataChunkCount );
		return false;
	}

	// Insert into pending set
	for ( uint32_t i = 0; i < header.numStripes; i++ )
		stripeIds.insert( header.stripeIds[ i ] );
	if ( ! ServerWorker::pending->insertReconstruction(
		event.instanceId, event.requestId,
		event.socket,
		header.listId,
		header.chunkId,
		stripeIds,
		unsealedKeys
	) ) {
		__ERROR__( "ServerWorker", "handleReconstructionRequest", "Cannot insert into coordinator RECONSTRUCTION pending map." );
	}

	// Send GET_CHUNK requests to surviving nodes
	ServerPeerEvent serverPeerEvent;
	std::vector<uint32_t> **requestIds = new std::vector<uint32_t> *[ ServerWorker::chunkCount ];
	std::vector<Metadata> **metadataList = new std::vector<Metadata> *[ ServerWorker::chunkCount ];
	for ( uint32_t i = 0; i < ServerWorker::chunkCount; i++ ) {
		requestIds[ i ] = new std::vector<uint32_t>();
		metadataList[ i ] = new std::vector<Metadata>();
	}
	chunkId = 0;
	for ( it = stripeIds.begin(); it != stripeIds.end(); it++ ) {
		// printf( "Processing (%u, %u, %u)...\n", header.listId, *it, header.chunkId );
		chunkCount = 0;
		requestId = ServerWorker::idGenerator->nextVal( this->workerId );
		metadata.listId = header.listId;
		metadata.stripeId = *it;
		while( chunkCount < ServerWorker::dataChunkCount - 1 ) {
			if ( chunkId != header.chunkId ) { // skip the chunk to be reconstructed
				socket = ( chunkId < ServerWorker::dataChunkCount ) ?
						 ( this->dataServerSockets[ chunkId ] ) :
						 ( this->parityServerSockets[ chunkId - ServerWorker::dataChunkCount ] );
				if ( socket->ready() && ! socket->self ) { // use this server
					metadata.chunkId = chunkId;
					chunkRequest.set(
						metadata.listId, metadata.stripeId, metadata.chunkId,
						socket, 0, false
					);
					if ( ! ServerWorker::pending->insertChunkRequest( PT_SERVER_PEER_GET_CHUNK, instanceId, event.instanceId, requestId, event.requestId, socket, chunkRequest ) ) {
						__ERROR__( "ServerWorker", "handleReconstructionRequest", "Cannot insert into server CHUNK_REQUEST pending map." );
					} else {
						requestIds[ chunkId ]->push_back( requestId );
						metadataList[ chunkId ]->push_back( metadata );
						chunkCount++;
					}
				}
			}
			chunkId++;
			if ( chunkId >= ServerWorker::chunkCount )
				chunkId = 0;
		}
		// Use own chunk
		chunk = ServerWorker::map->findChunkById( metadata.listId, metadata.stripeId, myChunkId );
		if ( ! chunk ) {
			chunk = Coding::zeros;
		} else {
			// Check whether the chunk is sealed or not
			MixedChunkBuffer *chunkBuffer = ServerWorker::chunkBuffer->at( metadata.listId );
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
		if ( ! ServerWorker::pending->insertChunkRequest( PT_SERVER_PEER_GET_CHUNK, instanceId, event.instanceId, requestId, event.requestId, 0, chunkRequest ) ) {
			__ERROR__( "ServerWorker", "handleReconstructionRequest", "Cannot insert into server CHUNK_REQUEST pending map." );
		}
	}
	// Send GET_CHUNK requests now
	for ( uint32_t i = 0; i < ServerWorker::chunkCount; i++ ) {
		if ( metadataList[ i ]->size() > 0 ) {
			socket = ( i < ServerWorker::dataChunkCount ) ?
					 ( this->dataServerSockets[ i ] ) :
					 ( this->parityServerSockets[ i - ServerWorker::dataChunkCount ] );

			serverPeerEvent.batchGetChunks( socket, requestIds[ i ], metadataList[ i ] );
			// ServerWorker::eventQueue->insert( serverPeerEvent );
			this->dispatch( serverPeerEvent );
		} else {
			delete requestIds[ i ];
			delete metadataList[ i ];
		}
	}
	delete[] requestIds;
	delete[] metadataList;

	return false;
}

bool ServerWorker::handleReconstructionUnsealedRequest( CoordinatorEvent event, char *buf, size_t size ) {
	struct BatchKeyHeader header;
	if ( ! this->protocol.parseBatchKeyHeader( header, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleReconstructionUnsealedRequest", "Invalid RECONSTRUCTION_UNSEALED request." );
		return false;
	}

	std::unordered_set<Key> unsealedKeys;
	std::unordered_set<Key>::iterator unsealedKeysIt;
	std::unordered_set<uint32_t> stripeIds;
	uint32_t listId, chunkId;
	ServerPeerSocket *reconstructedServer = 0;

	std::unordered_map<Key, KeyValue> *keyValueMap;
	LOCK_T *lock;

	for ( uint32_t i = 0, offset = 0; i < header.count; i++ ) {
		uint8_t keySize;
		char *keyStr;
		Key key;

		this->protocol.nextKeyInBatchKeyHeader( header, keySize, keyStr, offset );

		if ( i == 0 ) {
			reconstructedServer = this->getServers( keyStr, keySize, listId, chunkId );

			if ( ! ServerWorker::chunkBuffer->at( listId )->getKeyValueMap( keyValueMap, lock ) ) {
				__ERROR__(
					"ServerWorker", "handleReconstructionUnsealedRequest",
					"Cannot get key-value map."
				);
				return false;
			}
		}

		key.set( keySize, keyStr );
		key.dup();
		unsealedKeys.insert( key );
	}

	if ( ! ServerWorker::pending->insertReconstruction(
		event.instanceId, event.requestId,
		event.socket,
		listId, chunkId,
		stripeIds,
		unsealedKeys
	) ) {
		__ERROR__( "ServerWorker", "handleReconstructionUnsealedRequest", "Cannot insert into coordinator RECONSTRUCTION pending map." );
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
		requestId = ServerWorker::idGenerator->nextVal( this->workerId );
		if ( ! ServerWorker::pending->insert(
			PT_SERVER_PEER_FORWARD_KEYS,
			Server::instanceId, event.instanceId,
			requestId, event.requestId,
			( void * ) reconstructedServer
		) ) {
			__ERROR__( "ServerWorker", "handleReconstructionUnsealedRequest", "Cannot insert into pending set." );
		}

		buffer.data = this->protocol.buffer.send;
		buffer.size = this->protocol.generateBatchKeyValueHeader(
			PROTO_MAGIC_REQUEST, PROTO_MAGIC_TO_SERVER,
			PROTO_OPCODE_BATCH_KEY_VALUES,
			Server::instanceId, requestId,
			unsealedKeys, unsealedKeysIt,
			keyValueMap, lock,
			keyValuesCount,
			isCompleted
		);
		totalKeyValuesCount += keyValuesCount;

		if ( reconstructedServer ) {
			bool connected;
			ssize_t ret;

			ret = reconstructedServer->send( buffer.data, buffer.size, connected );
			if ( ret != ( ssize_t ) buffer.size )
				__ERROR__( "ServerWorker", "handleReconstructionUnsealedRequest", "The number of bytes sent (%ld bytes) is not equal to the message size (%lu bytes).", ret, buffer.size );
		} else {
			__ERROR__( "ServerWorker", "handleReconstructionUnsealedRequest", "Reconstructed server not available!" );
		}
	}

	return true;
}

bool ServerWorker::handleCompletedReconstructionAck() {
	return Server::getInstance()->initChunkBuffer();
}
