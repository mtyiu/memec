#include <cstring>
#include <ctype.h>
#include <unistd.h>
#include "slave.hh"

uint16_t Slave::instanceId;

Slave::Slave() {
	this->isRunning = false;
	Slave::instanceId = 0;
}

void Slave::free() {
	/* ID generator */
	this->idGenerator.free();
	/* Event queue */
	this->eventQueue.free();
	/* Coding */
	Coding::destroy( this->coding );
	/* Stripe list */
	delete this->stripeList;
	/* Chunk buffer */
	for ( size_t i = 0, size = this->chunkBuffer.size(); i < size; i++ ) {
		if ( this->chunkBuffer[ i ] )
			delete this->chunkBuffer[ i ];
	}
}

void Slave::sync( uint32_t requestId ) {
	CoordinatorEvent event;
	for ( int i = 0, len = this->config.global.coordinators.size(); i < len; i++ ) {
		// Can only sync with one coordinator
		event.sync( this->sockets.coordinators[ i ], Slave::instanceId, requestId );
		this->eventQueue.insert( event );
	}
}

void Slave::signalHandler( int signal ) {
	Slave *slave = Slave::getInstance();
	switch( signal ) {
		case SIGALRM:
			slave->sync();
			slave->alarm();
			break;
		default:
			Signal::setHandler(); // disable signal handler
			slave->stop();
			fclose( stdin );
	}
}

bool Slave::init( char *path, OptionList &options, bool verbose ) {
	// Parse configuration files //
	if ( ( ! this->config.global.parse( path ) ) ||
	     ( ! this->config.slave.merge( this->config.global ) ) ||
	     ( ! this->config.slave.parse( path ) ) ||
	     ( ! this->config.slave.override( options ) )
	) {
		return false;
	}
	this->mySlaveIndex = this->config.slave.validate( this->config.global.slaves );

	// Initialize modules //
	/* Socket */
	if ( ! this->sockets.epoll.init(
			this->config.slave.epoll.maxEvents,
			this->config.slave.epoll.timeout
		) || ! this->sockets.self.init(
			this->config.slave.slave.addr.type,
			this->config.slave.slave.addr.addr,
			this->config.slave.slave.addr.port,
			this->config.slave.slave.addr.name,
			&this->sockets.epoll
		) ) {
		__ERROR__( "Slave", "init", "Cannot initialize socket." );
		return false;
	}
	/* Vectors and other sockets */
	Socket::init( &this->sockets.epoll );
	CoordinatorSocket::setArrayMap( &this->sockets.coordinators );
	MasterSocket::setArrayMap( &this->sockets.masters );
	SlavePeerSocket::setArrayMap( &this->sockets.slavePeers );
	this->sockets.coordinators.reserve( this->config.global.coordinators.size() );
	for ( int i = 0, len = this->config.global.coordinators.size(); i < len; i++ ) {
		CoordinatorSocket *socket = new CoordinatorSocket();
		int fd;

		socket->init( this->config.global.coordinators[ i ], &this->sockets.epoll );
		fd = socket->getSocket();
		this->sockets.coordinators.set( fd, socket );
	}
	this->sockets.slavePeers.reserve( this->config.global.slaves.size() );
	for ( int i = 0, len = this->config.global.slaves.size(); i < len; i++ ) {
		SlavePeerSocket *socket = new SlavePeerSocket();
		int tmpfd = - ( i + 1 );
		socket->init(
			tmpfd,
			this->config.global.slaves[ i ],
			&this->sockets.epoll,
			i == mySlaveIndex && mySlaveIndex != -1 // indicate whether this is a self-socket
		);
		this->sockets.slavePeers.set( tmpfd, socket );
	}
	/* Coding */
	this->coding = Coding::instantiate(
		this->config.global.coding.scheme,
		this->config.global.coding.params,
		this->config.global.size.chunk
	);
	/* Stripe list */
	this->stripeList = new StripeList<SlavePeerSocket>(
		this->config.global.coding.params.getChunkCount(),
		this->config.global.coding.params.getDataChunkCount(),
		this->config.global.stripeList.count,
		this->sockets.slavePeers.values
	);
	/* Stripe list index */
	if ( mySlaveIndex != -1 )
		this->stripeListIndex = this->stripeList->list( mySlaveIndex );
	/* Chunk pool */
	Chunk::init( this->config.global.size.chunk );
	this->chunkPool = MemoryPool<Chunk>::getInstance();
	this->chunkPool->init(
		MemoryPool<Chunk>::getCapacity(
			this->config.slave.pool.chunks,
			this->config.global.size.chunk
		),
		Chunk::initFn,
		0
	);
	/* Chunk buffer */
	ChunkBuffer::init();
	this->chunkBuffer.reserve( this->config.global.stripeList.count );
	for ( uint32_t i = 0; i < this->config.global.stripeList.count; i++ )
		this->chunkBuffer.push_back( 0 );

	if ( mySlaveIndex != -1 ) {
		for ( uint32_t i = 0, size = this->stripeListIndex.size(); i < size; i++ ) {
			uint32_t listId = this->stripeListIndex[ i ].listId,
			         stripeId = this->stripeListIndex[ i ].stripeId,
			         chunkId = this->stripeListIndex[ i ].chunkId;
			if ( this->stripeListIndex[ i ].isParity ) {
				this->chunkBuffer[ listId ] = new MixedChunkBuffer(
					new ParityChunkBuffer(
						this->config.global.buffer.chunksPerList,
						listId, stripeId, chunkId, true
					)
				);
			} else {
				this->chunkBuffer[ listId ] = new MixedChunkBuffer(
					new DataChunkBuffer(
						this->config.global.buffer.chunksPerList,
						listId, stripeId, chunkId, true
					)
				);
			}
		}
	}
	// Map //
	this->map.setTimestamp( &this->timestamp );
	this->degradedChunkBuffer.map.init( &this->map );

	/* Workers, ID generator, packet pool and event queues */
	if ( this->config.slave.workers.type == WORKER_TYPE_MIXED ) {
		this->idGenerator.init( this->config.slave.workers.number.mixed );
		this->packetPool.init(
			this->config.slave.workers.number.mixed * this->config.global.coding.params.getChunkCount(),
			Protocol::getSuggestedBufferSize(
				this->config.global.size.key,
				this->config.global.size.chunk
			)
		);
		this->eventQueue.init(
			this->config.slave.eventQueue.block,
			this->config.slave.eventQueue.size.mixed,
			this->config.slave.eventQueue.size.pMixed
		);
		SlaveWorker::init();
		this->workers.reserve( this->config.slave.workers.number.mixed );
		for ( int i = 0, len = this->config.slave.workers.number.mixed; i < len; i++ ) {
			this->workers.push_back( SlaveWorker() );
			this->workers[ i ].init(
				this->config.global,
				this->config.slave,
				WORKER_ROLE_MIXED,
				i // worker ID
			);
		}
	} else {
		this->idGenerator.init( this->config.slave.workers.number.separated.total );
		this->packetPool.init(
			this->config.slave.workers.number.separated.total * this->config.global.coding.params.getChunkCount(),
			Protocol::getSuggestedBufferSize(
				this->config.global.size.key,
				this->config.global.size.chunk
			)
		);
		this->workers.reserve( this->config.slave.workers.number.separated.total );
		this->eventQueue.init(
			this->config.slave.eventQueue.block,
			this->config.slave.eventQueue.size.separated.coding,
			this->config.slave.eventQueue.size.separated.coordinator,
			this->config.slave.eventQueue.size.separated.io,
			this->config.slave.eventQueue.size.separated.master,
			this->config.slave.eventQueue.size.separated.slave,
			this->config.slave.eventQueue.size.separated.slavePeer
		);
		SlaveWorker::init();

		int index = 0;
#define WORKER_INIT_LOOP( _FIELD_, _CONSTANT_ ) \
		for ( int i = 0, len = this->config.slave.workers.number.separated._FIELD_; i < len; i++, index++ ) { \
			this->workers.push_back( SlaveWorker() ); \
			this->workers[ index ].init( \
				this->config.global, \
				this->config.slave, \
				_CONSTANT_, \
				index \
			); \
		}

		WORKER_INIT_LOOP( coding, WORKER_ROLE_CODING )
		WORKER_INIT_LOOP( coordinator, WORKER_ROLE_COORDINATOR )
		WORKER_INIT_LOOP( io, WORKER_ROLE_IO )
		WORKER_INIT_LOOP( master, WORKER_ROLE_MASTER )
		WORKER_INIT_LOOP( slave, WORKER_ROLE_SLAVE )
		WORKER_INIT_LOOP( slavePeer, WORKER_ROLE_SLAVE_PEER )
#undef WORKER_INIT_LOOP
	}

	// Set signal handlers //
	Signal::setHandler( Slave::signalHandler );

	// Init lock for instance id to socket mapping //
	LOCK_INIT( &this->sockets.mastersIdToSocketLock );
	LOCK_INIT( &this->sockets.slavesIdToSocketLock );

	// Show configuration //
	if ( verbose )
		this->info();
	return true;
}

bool Slave::init( int mySlaveIndex ) {
	this->mySlaveIndex = mySlaveIndex;
	if ( mySlaveIndex == -1 )
		return false;

	this->stripeListIndex = this->stripeList->list( mySlaveIndex );

	for ( uint32_t i = 0, size = this->stripeListIndex.size(); i < size; i++ ) {
		uint32_t listId = this->stripeListIndex[ i ].listId,
				 chunkId = this->stripeListIndex[ i ].chunkId;
		uint32_t stripeId = 0;
		if ( this->stripeListIndex[ i ].isParity ) {
			// The stripe ID is not used
			this->chunkBuffer[ listId ] = new MixedChunkBuffer(
				new ParityChunkBuffer(
					this->config.global.buffer.chunksPerList,
					listId, stripeId, chunkId, false
				)
			);
		} else {
			// Get minimum stripe ID
			stripeId = 0;

			this->chunkBuffer[ listId ] = new MixedChunkBuffer(
				new DataChunkBuffer(
					this->config.global.buffer.chunksPerList,
					listId, stripeId, chunkId, false
				)
			);
		}
	}

	return true;
}

bool Slave::initChunkBuffer() {
	if ( this->mySlaveIndex == -1 )
		return false;

	for ( uint32_t i = 0, size = this->stripeListIndex.size(); i < size; i++ ) {
		uint32_t listId = this->stripeListIndex[ i ].listId;
		this->chunkBuffer[ listId ]->init();
	}

	return true;
}

bool Slave::start() {
	/* Workers and event queues */
	this->eventQueue.start();
	if ( this->config.slave.workers.type == WORKER_TYPE_MIXED ) {
		for ( int i = 0, len = this->config.slave.workers.number.mixed; i < len; i++ ) {
			this->workers[ i ].start();
		}
	} else {
		for ( int i = 0, len = this->config.slave.workers.number.separated.total; i < len; i++ ) {
			this->workers[ i ].start();
		}
	}

	/* Sockets */
	// Connect to coordinators
	for ( int i = 0, len = this->config.global.coordinators.size(); i < len; i++ ) {
		if ( ! this->sockets.coordinators[ i ]->start() )
			__ERROR__( "Slave", "start", "Cannot connect to coordinator #%d.", i );
	}
	// Do not connect to slaves until a slave connected message is announcement by the coordinator

	// Start listening
	if ( ! this->sockets.self.start() ) {
		__ERROR__( "Slave", "start", "Cannot start socket." );
		return false;
	}

	this->startTime = start_timer();
	this->isRunning = true;

	/* Alarm */
	this->alarm();

	return true;
}

bool Slave::stop() {
	if ( ! this->isRunning )
		return false;

	int i, len;

	/* Sockets */
	this->sockets.self.stop();

	/* Workers */
	len = this->workers.size();
	for ( i = len - 1; i >= 0; i-- )
		this->workers[ i ].stop();

	/* Event queues */
	this->eventQueue.stop();

	/* Workers */
	for ( i = len - 1; i >= 0; i-- )
		this->workers[ i ].join();

	/* Sockets */
	for ( i = 0, len = this->sockets.coordinators.size(); i < len; i++ )
		this->sockets.coordinators[ i ]->stop();
	this->sockets.coordinators.clear();
	for ( i = 0, len = this->sockets.masters.size(); i < len; i++ )
		this->sockets.masters[ i ]->stop();
	this->sockets.masters.clear();
	for ( i = 0, len = this->sockets.slavePeers.size(); i < len; i++ ) {
		this->sockets.slavePeers[ i ]->stop();
		this->sockets.slavePeers[ i ]->free();
	}
	this->sockets.slavePeers.clear();

	/* Chunk buffer */
	for ( size_t i = 0, size = this->chunkBuffer.size(); i < size; i++ ) {
		if ( this->chunkBuffer[ i ] )
			this->chunkBuffer[ i ]->stop();
	}

	this->free();
	this->isRunning = false;
	printf( "\nBye.\n" );
	return true;
}

void Slave::seal() {
	size_t count = 0;
	SlavePeerEvent event;
	for ( int i = 0, size = this->chunkBuffer.size(); i < size; i++ ) {
		if ( this->chunkBuffer[ i ] ) {
			event.reqSealChunks( this->chunkBuffer[ i ] );
			this->eventQueue.insert( event );
			count++;
		}
	}
	printf( "\nSealing %lu chunk buffer:\n", count );
}

void Slave::flush( bool parityOnly ) {
	IOEvent ioEvent;
	std::unordered_map<Metadata, Chunk *>::iterator it;
	std::unordered_map<Metadata, Chunk *> *cache;
	LOCK_T *lock;
	Chunk *chunk;
	size_t count = 0;

	this->map.getCacheMap( cache, lock );

	LOCK( lock );
	for ( it = cache->begin(); it != cache->end(); it++ ) {
		chunk = it->second;
		if ( chunk->status == CHUNK_STATUS_DIRTY ) {
			if ( parityOnly && ! chunk->isParity )
				continue;
			ioEvent.flush( chunk, parityOnly );
			this->eventQueue.insert( ioEvent );
			count++;
		}
	}
	UNLOCK( lock );

	printf( "Flushing %lu chunks...\n", count );
}

void Slave::metadata() {
	std::unordered_map<Key, KeyMetadata>::iterator it;
	std::unordered_map<Key, KeyMetadata> *keys;
	LOCK_T *lock;
	FILE *f;
	char filename[ 64 ];

	this->map.getKeysMap( keys, lock );

	snprintf( filename, sizeof( filename ), "%s.meta", this->config.slave.slave.addr.name );
	f = fopen( filename, "w+" );
	if ( ! f ) {
		__ERROR__( "Slave", "metadata", "Cannot write to the file \"%s\".", filename );
	}

	LOCK( lock );
	for ( it = keys->begin(); it != keys->end(); it++ ) {
		Key key = it->first;
		KeyMetadata keyMetadata = it->second;

		fprintf( f, "%.*s\t%u\t%u\t%u\n", key.size, key.data, keyMetadata.listId, keyMetadata.stripeId, keyMetadata.chunkId );
	}
	UNLOCK( lock );

	fclose( f );
}

void Slave::memory( FILE *f ) {
	std::unordered_map<Metadata, Chunk *> *cache;
	std::unordered_map<Metadata, Chunk *>::iterator it;
	LOCK_T *lock;
	uint32_t numDataChunks = 0, numParityChunks = 0, numKeyValues = 0;
	uint64_t occupied = 0, allocated = 0, bytesParity = 0;

	this->map.getCacheMap( cache, lock );

	LOCK( lock );
	for ( it = cache->begin(); it != cache->end(); it++ ) {
		Chunk *chunk = it->second;
		if ( chunk->isParity ) {
			numParityChunks++;
			bytesParity += chunk->capacity;
		} else {
			numDataChunks++;
			numKeyValues += chunk->count;
			occupied += chunk->getSize();
			allocated += chunk->capacity;
		}
	}
	UNLOCK( lock );

	int width = 25;
	fprintf(
		f,
		"Parity chunks\n"
		"\t- %-*s : %u\n"
		"\t- %-*s : %lu\n"
		"Data chunks\n"
		"\t- %-*s : %u\n"
		"\t- %-*s : %u\n"
		"\t- %-*s : %lu\n"
		"\t- %-*s : %lu\n"
		"\t- %-*s : %6.4lf%%\n",
		width, "Number of parity chunks", numParityChunks,
		width, "Total size (bytes)", bytesParity,

		width, "Number of data chunks", numDataChunks,
		width, "Number of key-value pairs", numKeyValues,
		width, "Occupied size (bytes)", occupied,
		width, "Total size (bytes)", allocated,
		width, "Utilization", ( double ) occupied / allocated * 100.0
	);
}

void Slave::setDelay() {
	unsigned int delay;

	printf( "How much delay (in usec)? " );
	fflush( stdout );
	if ( scanf( "%u", &delay ) == 1 )  {
		SlaveWorker::delay = delay;
	} else {
		printf( "Invalid input.\n" );
	}
}

double Slave::getElapsedTime() {
	return get_elapsed_time( this->startTime );
}

void Slave::info( FILE *f ) {
	this->config.global.print( f );
	this->config.slave.print( f );
	this->stripeList->print( f );

	fprintf( f, "\n### Stripe List Index ###\n" );
	for ( int i = 0, size = this->stripeListIndex.size(); i < size; i++ ) {
		fprintf(
			f, "%d. List #%d: %s chunk #%d\n", i,
			this->stripeListIndex[ i ].listId,
			this->stripeListIndex[ i ].isParity ? "Parity" : "Data",
			this->stripeListIndex[ i ].chunkId
		);
	}
	fprintf( f, "\n" );
}

void Slave::debug( FILE *f ) {
	int i, len;
	fprintf( f, "Slave socket\n------------\n" );
	this->sockets.self.print( f );

	fprintf( f, "\nCoordinator sockets\n-------------------\n" );
	for ( i = 0, len = this->sockets.coordinators.size(); i < len; i++ ) {
		fprintf( f, "%d. ", i + 1 );
		this->sockets.coordinators[ i ]->print( f );
	}
	if ( len == 0 ) fprintf( f, "(None)\n" );

	fprintf( f, "\nMaster sockets\n---------------\n" );
	for ( i = 0, len = this->sockets.masters.size(); i < len; i++ ) {
		fprintf( f, "%d. ", i + 1 );
		this->sockets.masters[ i ]->print( f );
	}
	if ( len == 0 ) fprintf( f, "(None)\n" );

	fprintf( f, "\nSlave peer sockets\n------------------\n" );
	for ( i = 0, len = this->sockets.slavePeers.size(); i < len; i++ ) {
		fprintf( f, "%d. ", i + 1 );
		this->sockets.slavePeers[ i ]->print( f );
	}
	if ( len == 0 ) fprintf( f, "(None)\n" );

	fprintf( f, "\nChunk pool\n----------\n" );
	fprintf(
		f, "Count : %lu / %lu\n",
		this->chunkPool->getCount(),
		MemoryPool<Chunk>::getCapacity(
			this->config.slave.pool.chunks,
			this->config.global.size.chunk
		)
	);

	fprintf( f, "\nChunk buffer\n------------\n" );
	for ( i = 0, len = this->chunkBuffer.size(); i < len; i++ ) {
		if ( ! this->chunkBuffer[ i ] )
			continue;
		fprintf( f, "(#%d)\n", i + 1 );
		this->chunkBuffer[ i ]->print( f );
		fprintf( f, "\n" );
	}
	if ( len == 0 ) fprintf( f, "(None)\n" );

	fprintf( f, "\nWorkers\n-------\n" );
	for ( i = 0, len = this->workers.size(); i < len; i++ ) {
		fprintf( f, "%d. ", i + 1 );
		this->workers[ i ].print( f );
	}

	fprintf( f, "\nOther threads\n--------------\n" );
	this->sockets.self.printThread();

	fprintf( f, "\n" );

	fprintf( f, "\nPacket pool\n-----------\n" );
	this->packetPool.print( f );

	fprintf( f, "\nSlave event queue\n-----------------\n" );
	this->eventQueue.print( f );
}

void Slave::interactive() {
	char buf[ 4096 ];
	char *command;
	bool valid;
	int i, len;

	this->help();
	while( this->isRunning ) {
		valid = false;
		printf( "> " );
		fflush( stdout );
		if ( ! fgets( buf, sizeof( buf ), stdin ) ) {
			printf( "\n" );
			break;
		}

		// Trim
		len = strnlen( buf, sizeof( buf ) );
		for ( i = len - 1; i >= 0; i-- ) {
			if ( isspace( buf[ i ] ) )
				buf[ i ] = '\0';
			else
				break;
		}

		command = buf;
		while( isspace( command[ 0 ] ) ) {
			command++;
		}
		if ( strlen( command ) == 0 )
			continue;

		if ( strcmp( command, "help" ) == 0 ) {
			valid = true;
			this->help();
		} else if ( strcmp( command, "exit" ) == 0 ) {
			break;
		} else if ( strcmp( command, "info" ) == 0 ) {
			valid = true;
			this->info();
		} else if ( strcmp( command, "debug" ) == 0 ) {
			valid = true;
			this->debug();
		} else if ( strcmp( command, "id" ) == 0 ) {
			valid = true;
			this->printInstanceId();
		} else if ( strcmp( command, "dump" ) == 0 ) {
			valid = true;
			this->dump();
		} else if ( strcmp( command, "lookup" ) == 0 ) {
			valid = true;
			this->lookup();
		} else if ( strcmp( command, "seal" ) == 0 ) {
			valid = true;
			this->seal();
		} else if ( strcmp( command, "flush" ) == 0 ) {
			valid = true;
			this->flush();
		} else if ( strcmp( command, "p2disk" ) == 0 ) {
			valid = true;
			this->flush( true );
		} else if ( strcmp( command, "memory" ) == 0 ) {
			valid = true;
			this->memory();

			FILE *f = fopen( "memory.log", "w" );
			this->memory( f );
			fclose( f );
		} else if ( strcmp( command, "delay" ) == 0 ) {
			valid = true;
			this->setDelay();
		} else if ( strcmp( command, "metadata" ) == 0 ) {
			valid = true;
			this->metadata();
		} else if ( strcmp( command, "pending" ) == 0 ) {
			valid = true;
			this->printPending();
		} else if ( strcmp( command, "chunk" ) == 0 ) {
			valid = true;
			this->printChunk();
		} else if ( strcmp( command, "sync" ) == 0 ) {
			valid = true;
			this->sync();
		} else if ( strcmp( command, "backup" ) == 0 ) {
			valid = true;
			this->backupStat( stdout );
		} else if ( strcmp( command, "time" ) == 0 ) {
			valid = true;
			this->time();
		} else {
			valid = false;
		}

		if ( ! valid ) {
			fprintf( stderr, "Invalid command!\n" );
		}
	}
}

void Slave::printPending( FILE *f ) {
	size_t i;
	std::unordered_multimap<PendingIdentifier, Key>::iterator it;
	std::unordered_multimap<PendingIdentifier, KeyValueUpdate>::iterator keyValueUpdateIt;
	std::unordered_multimap<PendingIdentifier, ChunkUpdate>::iterator chunkUpdateIt;
	std::unordered_multimap<PendingIdentifier, ChunkRequest>::iterator chunkRequestIt;

	LOCK( &this->pending.masters.getLock );
	fprintf(
		f,
		"Pending requests for masters\n"
		"----------------------------\n"
		"[GET] Pending: %lu\n",
		this->pending.masters.get.size()
	);
	i = 1;
	for (
		it = this->pending.masters.get.begin();
		it != this->pending.masters.get.end();
		it++, i++
	) {
		const PendingIdentifier &pid = it->first;
		const Key &key = it->second;
		fprintf(
			f, "%lu. ID: (%u, %u); Key: %.*s (size = %u); source: ",
			i, it->first.instanceId, it->first.requestId,
			key.size, key.data, key.size
		);
		if ( pid.ptr )
			( ( Socket * ) pid.ptr )->printAddress( f );
		else
			fprintf( f, "(nil)\n" );
		fprintf( f, "\n" );
	}
	UNLOCK( &this->pending.masters.getLock );

	LOCK( &this->pending.masters.updateLock );
	fprintf(
		f,
		"\n[UPDATE] Pending: %lu\n",
		this->pending.masters.update.size()
	);
	i = 1;
	for (
		keyValueUpdateIt = this->pending.masters.update.begin();
		keyValueUpdateIt != this->pending.masters.update.end();
		keyValueUpdateIt++, i++
	) {
		const PendingIdentifier &pid = keyValueUpdateIt->first;
		const KeyValueUpdate &keyValueUpdate = keyValueUpdateIt->second;
		fprintf(
			f, "%lu. ID: (%u, %u); Key: %.*s (size = %u, offset = %u, length = %u); source: ",
			i, keyValueUpdateIt->first.instanceId, keyValueUpdateIt->first.requestId,
			keyValueUpdate.size, keyValueUpdate.data, keyValueUpdate.size,
			keyValueUpdate.offset, keyValueUpdate.length
		);
		if ( pid.ptr )
			( ( Socket * ) pid.ptr )->printAddress( f );
		else
			fprintf( f, "(nil)\n" );
		fprintf( f, "\n" );
	}
	UNLOCK( &this->pending.masters.updateLock );

	LOCK( &this->pending.masters.delLock );
	fprintf(
		f,
		"\n[DELETE] Pending: %lu\n",
		this->pending.masters.del.size()
	);
	i = 1;
	for (
		it = this->pending.masters.del.begin();
		it != this->pending.masters.del.end();
		it++, i++
	) {
		const PendingIdentifier &pid = it->first;
		const Key &key = it->second;
		fprintf(
			f, "%lu. ID: (%u, %u); Key: %.*s (size = %u); source: ",
			i, it->first.instanceId, it->first.requestId,
			key.size, key.data, key.size
		);
		if ( pid.ptr )
			( ( Socket * ) pid.ptr )->printAddress( f );
		else
			fprintf( f, "(nil)\n" );
		fprintf( f, "\n" );
	}
	UNLOCK( &this->pending.masters.delLock );

	LOCK( &this->pending.slavePeers.updateLock );
	fprintf(
		f,
		"\n\nPending requests for slave peers\n"
		"--------------------------------\n"
		"[UPDATE] Pending: %lu\n",
		this->pending.slavePeers.update.size()
	);
	i = 1;
	for (
		keyValueUpdateIt = this->pending.slavePeers.update.begin();
		keyValueUpdateIt != this->pending.slavePeers.update.end();
		keyValueUpdateIt++, i++
	) {
		const PendingIdentifier &pid = keyValueUpdateIt->first;
		const KeyValueUpdate &keyValueUpdate = keyValueUpdateIt->second;
		fprintf(
			f, "%lu. ID: (%u, %u); Key: %.*s (size = %u, offset = %u, length = %u); source: ",
			i, keyValueUpdateIt->first.instanceId, keyValueUpdateIt->first.requestId,
			keyValueUpdate.size, keyValueUpdate.data, keyValueUpdate.size,
			keyValueUpdate.offset, keyValueUpdate.length
		);
		if ( pid.ptr )
			( ( Socket * ) pid.ptr )->printAddress( f );
		else
			fprintf( f, "(nil)\n" );
		fprintf( f, "\n" );
	}
	UNLOCK( &this->pending.slavePeers.updateLock );

	LOCK( &this->pending.slavePeers.delLock );
	fprintf(
		f,
		"\n[DELETE] Pending: %lu\n",
		this->pending.slavePeers.del.size()
	);
	i = 1;
	for (
		it = this->pending.slavePeers.del.begin();
		it != this->pending.slavePeers.del.end();
		it++, i++
	) {
		const PendingIdentifier &pid = it->first;
		const Key &key = it->second;
		fprintf(
			f, "%lu. ID: (%u, %u); Key: %.*s (size = %u); source: ",
			i, it->first.instanceId, it->first.requestId,
			key.size, key.data, key.size
		);
		if ( pid.ptr )
			( ( Socket * ) pid.ptr )->printAddress( f );
		else
			fprintf( f, "(nil)\n" );
		fprintf( f, "\n" );
	}
	UNLOCK( &this->pending.slavePeers.delLock );

	LOCK( &this->pending.slavePeers.updateChunkLock );
	fprintf(
		f,
		"\n[UPDATE_CHUNK] Pending: %lu\n",
		this->pending.slavePeers.updateChunk.size()
	);
	i = 1;
	for (
		chunkUpdateIt = this->pending.slavePeers.updateChunk.begin();
		chunkUpdateIt != this->pending.slavePeers.updateChunk.end();
		chunkUpdateIt++, i++
	) {
		const ChunkUpdate &chunkUpdate = chunkUpdateIt->second;
		fprintf(
			f, "%lu. ID: (%u, %u); List ID: %u, stripe ID: %u, chunk ID: %u; Key: %.*s (key size = %u, offset = %u, length = %u, value update offset = %u); target: ",
			i, chunkUpdateIt->first.instanceId, chunkUpdateIt->first.requestId,
			chunkUpdate.listId, chunkUpdate.stripeId, chunkUpdate.chunkId,
			chunkUpdate.keySize, chunkUpdate.key, chunkUpdate.keySize,
			chunkUpdate.offset, chunkUpdate.length, chunkUpdate.valueUpdateOffset
		);
		if ( chunkUpdate.ptr )
			( ( Socket * ) chunkUpdate.ptr )->printAddress( f );
		else
			fprintf( f, "(nil)\n" );
		fprintf( f, "\n" );
	}
	UNLOCK( &this->pending.slavePeers.updateChunkLock );

	LOCK( &this->pending.slavePeers.delChunkLock );
	fprintf(
		f,
		"\n[DELETE_CHUNK] Pending: %lu\n",
		this->pending.slavePeers.deleteChunk.size()
	);
	i = 1;
	for (
		chunkUpdateIt = this->pending.slavePeers.deleteChunk.begin();
		chunkUpdateIt != this->pending.slavePeers.deleteChunk.end();
		chunkUpdateIt++, i++
	) {
		const ChunkUpdate &chunkUpdate = chunkUpdateIt->second;
		fprintf(
			f, "%lu. ID: (%u, %u); List ID: %u, stripe ID: %u, chunk ID: %u; Key: %.*s (key size = %u, offset = %u, length = %u); target: ",
			i, chunkUpdateIt->first.instanceId, chunkUpdateIt->first.requestId,
			chunkUpdate.listId, chunkUpdate.stripeId, chunkUpdate.chunkId,
			chunkUpdate.keySize, chunkUpdate.key, chunkUpdate.keySize,
			chunkUpdate.offset, chunkUpdate.length
		);
		if ( chunkUpdate.ptr )
			( ( Socket * ) chunkUpdate.ptr )->printAddress( f );
		else
			fprintf( f, "(nil)\n" );
		fprintf( f, "\n" );
	}
	UNLOCK( &this->pending.slavePeers.delChunkLock );

	LOCK( &this->pending.slavePeers.getChunkLock );
	fprintf(
		f,
		"\n[GET_CHUNK] Pending: %lu\n",
		this->pending.slavePeers.getChunk.size()
	);
	i = 1;
	for (
		chunkRequestIt = this->pending.slavePeers.getChunk.begin();
		chunkRequestIt != this->pending.slavePeers.getChunk.end();
		chunkRequestIt++, i++
	) {
		const ChunkRequest &chunkRequest = chunkRequestIt->second;
		fprintf(
			f, "%lu. ID: (%u, %u); List ID: %u, stripe ID: %u, chunk ID: %u; chunk: %p; target: ",
			i, chunkRequestIt->first.instanceId, chunkRequestIt->first.requestId,
			chunkRequest.listId, chunkRequest.stripeId, chunkRequest.chunkId, chunkRequest.chunk
		);
		if ( chunkRequest.socket )
			chunkRequest.socket->printAddress( f );
		else
			fprintf( f, "(nil)\n" );
		fprintf( f, "\n" );
	}
	UNLOCK( &this->pending.slavePeers.getChunkLock );

	LOCK( &this->pending.slavePeers.setChunkLock );
	fprintf(
		f,
		"\n[SET_CHUNK] Pending: %lu\n",
		this->pending.slavePeers.setChunk.size()
	);
	i = 1;
	for (
		chunkRequestIt = this->pending.slavePeers.setChunk.begin();
		chunkRequestIt != this->pending.slavePeers.setChunk.end();
		chunkRequestIt++, i++
	) {
		const ChunkRequest &chunkRequest = chunkRequestIt->second;
		fprintf(
			f, "%lu. ID: (%u, %u); List ID: %u, stripe ID: %u, chunk ID: %u; target: ",
			i, chunkRequestIt->first.instanceId, chunkRequestIt->first.requestId,
			chunkRequest.listId, chunkRequest.stripeId, chunkRequest.chunkId
		);
		if ( chunkRequest.socket )
			chunkRequest.socket->printAddress( f );
		else
			fprintf( f, "(nil)\n" );
		fprintf( f, "\n" );
	}
	UNLOCK( &this->pending.slavePeers.setChunkLock );

	// pending remapped data
	LOCK( &this->pending.slavePeers.remappedDataLock );
	fprintf(
		f,
		"\n[REMAP_DATA] Pending: %lu slaves\n",
		this->pending.slavePeers.remappedData.size()
	);
	for (
		auto slave = this->pending.slavePeers.remappedData.begin();
		slave != this->pending.slavePeers.remappedData.end();
		slave++
	) {
		char addrstr[ INET_ADDRSTRLEN ];
		Socket::ntoh_ip( slave->first.sin_addr.s_addr, addrstr, INET_ADDRSTRLEN );
		fprintf( f, "\tSlave %s:%hu\n", addrstr, ntohs( slave->first.sin_port ) );
		for ( auto record : *slave->second ) {
			fprintf(
				f,
				"\t\t List ID: %u Chunk Id: %u Key: %.*s\n",
				record.listId, record.chunkId,
				record.key.size, record.key.data
			);
		}
	}
	UNLOCK( &this->pending.slavePeers.remappedDataLock );
}

void Slave::printChunk() {
	uint32_t listId, stripeId, chunkId;
	printf( "Which chunk (List ID, Stripe ID, Chunk ID)? " );
	fflush( stdout );
	if ( scanf( "%u %u %u", &listId, &stripeId, &chunkId ) == 3 ) {
		Chunk *chunk = this->map.findChunkById( listId, stripeId, chunkId );
		if ( chunk ) {
			chunk->print();
		} else {
			printf( "Not found.\n" );
		}
	} else {
		printf( "Invalid input.\n" );
	}
}

void Slave::dump() {
	this->map.dump();
}

void Slave::printInstanceId( FILE *f ) {
	fprintf( f, "Instance ID = %u\n", Slave::instanceId );
}

void Slave::help() {
	fprintf(
		stdout,
		"Supported commands:\n"
		"- help: Show this help message\n"
		"- info: Show configuration\n"
		"- debug: Show debug messages\n"
		"- id: Print instance ID\n"
		"- lookup: Search for the metadata of an input key\n"
		"- seal: Seal all chunks in the chunk buffer\n"
		"- flush: Flush all dirty chunks to disk\n"
		"- metadata: Write metadata to disk\n"
		"- delay: Add constant delay to each client response\n"
		"- sync: Synchronize with coordinator\n"
		"- chunk: Print the debug message for a chunk\n"
		"- pending: Print all pending requests\n"
		"- dump: Dump all key-value pairs\n"
		"- memory: Print memory usage\n"
		"- backup : Show the backup stats\n"
		"- time: Show elapsed time\n"
		"- exit: Terminate this client\n"
	);
	fflush( stdout );
}

void Slave::lookup() {
	char key[ 256 ];
	uint8_t keySize;

	printf( "Input key: " );
	fflush( stdout );
	if ( ! fgets( key, sizeof( key ), stdin ) ) {
		fprintf( stderr, "Invalid input!\n" );
		return;
	}
	keySize = ( uint8_t ) strnlen( key, sizeof( key ) ) - 1;

	bool found = false;

	KeyMetadata keyMetadata;
	if ( this->map.findValueByKey( key, keySize, 0, 0, &keyMetadata, 0, 0 ) ) {
		printf(
			"Metadata: (%u, %u, %u); offset: %u, length: %u\n", keyMetadata.listId, keyMetadata.stripeId, keyMetadata.chunkId,
			keyMetadata.offset, keyMetadata.length
		);
		found = true;
	}

	RemappedKeyValue remappedKeyValue;
	if ( this->remappedBuffer.find( keySize, key, &remappedKeyValue ) ) {
		printf( "Remapped key found [%u, %u]: ", remappedKeyValue.listId, remappedKeyValue.chunkId );
		for ( uint32_t i = 0; i < remappedKeyValue.remappedCount; i++ ) {
			printf(
				"%s(%u, %u) |-> (%u, %u)%s",
				i == 0 ? "" : "; ",
				remappedKeyValue.original[ i * 2     ],
				remappedKeyValue.original[ i * 2 + 1 ],
				remappedKeyValue.remapped[ i * 2     ],
				remappedKeyValue.remapped[ i * 2 + 1 ],
				i == remappedKeyValue.remappedCount - 1 ? "\n" : ""
			);
		}
		found = true;
	}

	bool isSealed;
	if ( this->degradedChunkBuffer.map.findValueByKey( key, keySize, isSealed, 0, 0, &keyMetadata ) ) {
		printf(
			"Reconstructed chunk found: (%u, %u, %u); offset: %u, length: %u; is sealed? %s\n",
			keyMetadata.listId, keyMetadata.stripeId, keyMetadata.chunkId, keyMetadata.offset, keyMetadata.length,
			isSealed ? "yes" : "no"
		);
	}

	if ( ! found )
		printf( "Key not found.\n" );
}

void Slave::time() {
	fprintf( stdout, "Elapsed time: %12.6lf s\n", this->getElapsedTime() );
	fflush( stdout );
}

void Slave::alarm() {
	::alarm( this->config.global.sync.timeout );
}

void Slave::backupStat( FILE *f ) {
	fprintf( f, "\nSlave delta backup stats\n============================\n" );
	for ( int i = 0, len = this->sockets.masters.size(); i < len; i++ ) {
		fprintf( f,
			">> Master FD = %u\n-------------------\n",
			this->sockets.masters.keys[ i ]
		);
		this->sockets.masters.values[ i ]->backup.print( f );
		fprintf( f, "\n" );
	}
}
