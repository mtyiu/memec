#include <cstring>
#include <ctype.h>
#include <unistd.h>
#include "slave.hh"

Slave::Slave() {
	this->isRunning = false;
}

void Slave::free() {
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

void Slave::sync() {
	CoordinatorEvent event;
	for ( int i = 0, len = this->config.global.coordinators.size(); i < len; i++ ) {
		// Can only sync with one coordinator
		event.sync( &this->sockets.coordinators[ i ] );
		// this->eventQueue.insert( event );
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
	int mySlaveIndex;
	// Parse configuration files //
	if ( ( ! this->config.global.parse( path ) ) ||
	     ( ! this->config.slave.merge( this->config.global ) ) ||
	     ( ! this->config.slave.parse( path ) ) ||
	     ( ! this->config.slave.override( options ) ) ||
	     ( ( mySlaveIndex = this->config.slave.validate( this->config.global.slaves ) ) == -1 ) ) {
		return false;
	}

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
	this->sockets.coordinators.reserve( this->config.global.coordinators.size() );
	for ( int i = 0, len = this->config.global.coordinators.size(); i < len; i++ ) {
		CoordinatorSocket socket;
		int fd;

		socket.init( this->config.global.coordinators[ i ], &this->sockets.epoll );
		fd = socket.getSocket();
		this->sockets.coordinators.set( fd, socket );
	}
	this->sockets.slavePeers.reserve( this->config.global.slaves.size() );
	for ( int i = 0, len = this->config.global.slaves.size(); i < len; i++ ) {
		SlavePeerSocket socket;
		int tmpfd = - ( i + 1 );
		socket.init(
			tmpfd,
			this->config.global.slaves[ i ],
			&this->sockets.epoll,
			i == mySlaveIndex // indicate whether this is a self-socket
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
	/* Stripe pool */
	Stripe::init(
		this->config.global.coding.params.getDataChunkCount(),
		this->config.global.coding.params.getParityChunkCount()
	);
	this->stripePool = MemoryPool<Stripe>::getInstance();
	this->stripePool->init(
		MemoryPool<Stripe>::getCapacity(
			this->config.slave.pool.stripe,
			this->config.global.coding.params.getChunkCount() * sizeof( Chunk * )
		)
	);
	/* Chunk buffer */
	ChunkBuffer::init();
	this->chunkBuffer.reserve( this->config.global.stripeList.count );
	for ( uint32_t i = 0; i < this->config.global.stripeList.count; i++ )
		this->chunkBuffer.push_back( 0 );
	for ( uint32_t i = 0, size = this->stripeListIndex.size(); i < size; i++ ) {
		uint32_t listId = this->stripeListIndex[ i ].listId,
		         stripeId = this->stripeListIndex[ i ].stripeId,
		         chunkId = this->stripeListIndex[ i ].chunkId;
		if ( this->stripeListIndex[ i ].isParity ) {
			this->chunkBuffer[ listId ] = new MixedChunkBuffer(
				new ParityChunkBuffer(
					this->config.global.buffer.chunksPerList,
					listId, stripeId, chunkId
				)
			);
		} else {
			this->chunkBuffer[ listId ] = new MixedChunkBuffer(
				new DataChunkBuffer(
					this->config.global.buffer.chunksPerList,
					listId, stripeId, chunkId
				)
			);
		}
	}
	/* Workers and event queues */
	if ( this->config.slave.workers.type == WORKER_TYPE_MIXED ) {
		this->eventQueue.init(
			this->config.slave.eventQueue.block,
			this->config.slave.eventQueue.size.mixed
		);
		SlaveWorker::init();
		this->workers.reserve( this->config.slave.workers.number.mixed );
		for ( int i = 0, len = this->config.slave.workers.number.mixed; i < len; i++ ) {
			this->workers.push_back( SlaveWorker() );
			this->workers[ i ].init(
				this->config.global,
				this->config.slave,
				WORKER_ROLE_MIXED
			);
		}
	} else {
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
				_CONSTANT_ \
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

	// Show configuration //
	if ( verbose )
		this->info();
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
		if ( ! this->sockets.coordinators[ i ].start() )
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
		this->sockets.coordinators[ i ].stop();
	for ( i = 0, len = this->sockets.masters.size(); i < len; i++ )
		this->sockets.masters[ i ].stop();
	for ( i = 0, len = this->sockets.slavePeers.size(); i < len; i++ ) {
		this->sockets.slavePeers[ i ].stop();
		this->sockets.slavePeers[ i ].free();
	}

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
		this->sockets.coordinators[ i ].print( f );
	}
	if ( len == 0 ) fprintf( f, "(None)\n" );

	fprintf( f, "\nMaster sockets\n---------------\n" );
	for ( i = 0, len = this->sockets.masters.size(); i < len; i++ ) {
		fprintf( f, "%d. ", i + 1 );
		this->sockets.masters[ i ].print( f );
	}
	if ( len == 0 ) fprintf( f, "(None)\n" );

	fprintf( f, "\nSlave peer sockets\n------------------\n" );
	for ( i = 0, len = this->sockets.slavePeers.size(); i < len; i++ ) {
		fprintf( f, "%d. ", i + 1 );
		this->sockets.slavePeers[ i ].print( f );
	}
	if ( len == 0 ) fprintf( f, "(None)\n" );

	fprintf( f, "\nChunk buffer\n------------\n" );
	for ( i = 0, len = this->chunkBuffer.size(); i < len; i++ ) {
		if ( ! this->chunkBuffer[ i ] )
			continue;
		fprintf( f, "(#%d)\n", i + 1 );
		this->chunkBuffer[ i ]->print( f );
		fprintf( f, "\n" );
	}
	if ( len == 0 ) fprintf( f, "(None)\n" );

	fprintf( f, "\nSlave event queue\n-----------------\n" );
	this->eventQueue.print( f );

	fprintf( f, "\nWorkers\n-------\n" );
	for ( i = 0, len = this->workers.size(); i < len; i++ ) {
		fprintf( f, "%d. ", i + 1 );
		this->workers[ i ].print( f );
	}

	fprintf( f, "\nOther threads\n--------------\n" );
	this->sockets.self.printThread();

	fprintf( f, "\n" );

	fprintf( f, "\nSlave peer sockets\n------------------\n" );
	for ( i = 0, len = this->sockets.slavePeers.size(); i < len; i++ ) {
		fprintf( f, "%d. ", i + 1 );
		this->sockets.slavePeers[ i ].print( f );
	}
	if ( len == 0 ) fprintf( f, "(None)\n" );
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
		} else if ( strcmp( command, "dump" ) == 0 ) {
			valid = true;
			this->dump();
		} else if ( strcmp( command, "pending" ) == 0 ) {
			valid = true;
			this->printPending();
		} else if ( strcmp( command, "sync" ) == 0 ) {
			valid = true;
			this->sync();
		} else if ( strcmp( command, "load" ) == 0 ) {
			valid = true;
			this->aggregateLoad( stdout );
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
	std::set<Key>::iterator it;
	std::set<KeyValueUpdate>::iterator keyValueUpdateIt;
	std::set<ChunkUpdate>::iterator chunkUpdateIt;
	std::set<ChunkRequest>::iterator chunkRequestIt;
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
		const Key &key = *it;
		fprintf( f, "%lu. Key: %.*s (size = %u); source: ", i, key.size, key.data, key.size );
		if ( key.ptr )
			( ( Socket * ) key.ptr )->printAddress( f );
		else
			fprintf( f, "(nil)\n" );
		fprintf( f, "\n" );
	}

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
		const KeyValueUpdate &keyValueUpdate = *keyValueUpdateIt;
		fprintf(
			f, "%lu. Key: %.*s (size = %u, offset = %u, length = %u); source: ",
			i, keyValueUpdate.size, keyValueUpdate.data, keyValueUpdate.size,
			keyValueUpdate.offset, keyValueUpdate.length
		);
		if ( keyValueUpdate.ptr )
			( ( Socket * ) keyValueUpdate.ptr )->printAddress( f );
		else
			fprintf( f, "(nil)\n" );
		fprintf( f, "\n" );
	}

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
		const Key &key = *it;
		fprintf( f, "%lu. Key: %.*s (size = %u); source: ", i, key.size, key.data, key.size );
		if ( key.ptr )
			( ( Socket * ) key.ptr )->printAddress( f );
		else
			fprintf( f, "(nil)\n" );
		fprintf( f, "\n" );
	}


	fprintf(
		f,
		"\n\nPending requests for slave peers\n"
		"--------------------------------\n"
		"[UPDATE_CHUNK] Pending: %lu\n",
		this->pending.slavePeers.updateChunk.size()
	);
	i = 1;
	for (
		chunkUpdateIt = this->pending.slavePeers.updateChunk.begin();
		chunkUpdateIt != this->pending.slavePeers.updateChunk.end();
		chunkUpdateIt++, i++
	) {
		const ChunkUpdate &chunkUpdate = *chunkUpdateIt;
		fprintf(
			f, "%lu. List ID: %u, stripe ID: %u, chunk ID: %u; Key: %.*s (key size = %u, offset = %u, length = %u, value update offset = %u); target: ",
			i, chunkUpdate.listId, chunkUpdate.stripeId, chunkUpdate.chunkId,
			chunkUpdate.keySize, chunkUpdate.key, chunkUpdate.keySize,
			chunkUpdate.offset, chunkUpdate.length, chunkUpdate.valueUpdateOffset
		);
		if ( chunkUpdate.ptr )
			( ( Socket * ) chunkUpdate.ptr )->printAddress( f );
		else
			fprintf( f, "(nil)\n" );
		fprintf( f, "\n" );
	}

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
		const ChunkUpdate &chunkUpdate = *chunkUpdateIt;
		fprintf(
			f, "%lu. List ID: %u, stripe ID: %u, chunk ID: %u; Key: %.*s (key size = %u, offset = %u, length = %u); target: ",
			i, chunkUpdate.listId, chunkUpdate.stripeId, chunkUpdate.chunkId,
			chunkUpdate.keySize, chunkUpdate.key, chunkUpdate.keySize,
			chunkUpdate.offset, chunkUpdate.length
		);
		if ( chunkUpdate.ptr )
			( ( Socket * ) chunkUpdate.ptr )->printAddress( f );
		else
			fprintf( f, "(nil)\n" );
		fprintf( f, "\n" );
	}

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
		const ChunkRequest &chunkRequest = *chunkRequestIt;
		fprintf(
			f, "%lu. List ID: %u, stripe ID: %u, chunk ID: %u; target: ",
			i, chunkRequest.listId, chunkRequest.stripeId, chunkRequest.chunkId
		);
		if ( chunkRequest.socket )
			chunkRequest.socket->printAddress( f );
		else
			fprintf( f, "(nil)\n" );
		fprintf( f, "\n" );
	}

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
		const ChunkRequest &chunkRequest = *chunkRequestIt;
		fprintf(
			f, "%lu. List ID: %u, stripe ID: %u, chunk ID: %u; target: ",
			i, chunkRequest.listId, chunkRequest.stripeId, chunkRequest.chunkId
		);
		if ( chunkRequest.socket )
			chunkRequest.socket->printAddress( f );
		else
			fprintf( f, "(nil)\n" );
		fprintf( f, "\n" );
	}
}

void Slave::dump() {
	/*
	fprintf( stdout, "List of key-value pairs:\n------------------------\n" );
	if ( ! this->map.keys.size() ) {
		fprintf( stdout, "(None)\n" );
	} else {
		for ( std::map<Key, KeyMetadata>::iterator it = this->map.keys.begin(); it != this->map.keys.end(); it++ ) {
			fprintf(
				stdout, "%.*s --> (list ID: %u, stripe ID: %u, chunk ID: %u, offset: %u, length: %u)\n",
				it->first.size, it->first.data,
				it->second.listId, it->second.stripeId, it->second.chunkId,
				it->second.offset, it->second.length
			);
		}
	}
	fprintf( stdout, "\n" );
	*/
	fprintf( stdout, "Number of key-value pairs: %lu\n\n", this->map.keys.size() );

	fprintf( stdout, "List of chunks in the cache:\n----------------------------\n" );
	if ( ! this->map.cache.size() ) {
		fprintf( stdout, "(None)\n" );
	} else {
		for ( std::map<Metadata, Chunk *>::iterator it = this->map.cache.begin(); it != this->map.cache.end(); it++ ) {
			fprintf(
				stdout, "(list ID: %u, stripe ID: %u, chunk ID: %u) --> %p (type: %s chunk, status: %s, count: %u, size: %u)\n",
				it->first.listId, it->first.stripeId, it->first.chunkId,
				it->second, it->second->isParity ? "parity" : "data",
				( it->second->status == CHUNK_STATUS_EMPTY ? "empty" :
					( it->second->status == CHUNK_STATUS_DIRTY ? "dirty" : "cached" )
				),
				it->second->count, it->second->size
			);
		}
	}
	fprintf( stdout, "\n" );
}

void Slave::help() {
	fprintf(
		stdout,
		"Supported commands:\n"
		"- help: Show this help message\n"
		"- info: Show configuration\n"
		"- debug: Show debug messages\n"
		"- dump: Dump all key-value pairs\n"
		"- sync: Synchronize with coordinator\n"
		"- load: Show the load of each worker\n"
		"- time: Show elapsed time\n"
		"- exit: Terminate this client\n"
	);
	fflush( stdout );
}

void Slave::time() {
	fprintf( stdout, "Elapsed time: %12.6lf s\n", this->getElapsedTime() );
	fflush( stdout );
}

void Slave::alarm() {
	::alarm( this->config.global.sync.timeout );
}

Load &Slave::aggregateLoad( FILE *f ) {
	this->load.reset();
	for ( int i = 0, len = this->workers.size(); i < len; i++ ) {
		SlaveLoad &load = this->workers[ i ].load;
		this->load.aggregate( load );

		if ( f ) {
			fprintf( f,	"Load of Worker #%d:\n-------------------\n", i + 1 );
			load.print( f );
			fprintf( f, "\n" );
		}
	}
	if ( f ) {
		fprintf( f,	"Aggregated load:\n----------------\n" );
		this->load.print( f );
	}
	return this->load;
}
