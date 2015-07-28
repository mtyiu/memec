#include <cstring>
#include <ctype.h>
#include "slave.hh"

Slave::Slave() {
	this->isRunning = false;
}

void Slave::free() {
	this->eventQueue.free();
	delete this->stripeList;
	delete this->chunkBuffer;
}

void Slave::signalHandler( int signal ) {
	Signal::setHandler();
	Slave::getInstance()->stop();
	fclose( stdin );
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
			&this->sockets.epoll
		) ) {
		__ERROR__( "Slave", "init", "Cannot initialize socket." );
		return false;
	}
	/* Vectors and other sockets */
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
		int fd;

		socket.init( this->config.global.slaves[ i ], &this->sockets.epoll );
		fd = socket.getSocket();
		this->sockets.slavePeers.set( fd, socket );
	}
	/* Stripe list */
	this->stripeList = new StripeList<SlavePeerSocket>(
		this->config.global.coding.params.getChunkCount(),
		this->config.global.coding.params.getDataChunkCount(),
		this->config.global.stripeList.count,
		this->sockets.slavePeers.values
	);
	/* Chunk buffer */
	this->chunkBuffer = new ChunkBuffer(
		this->config.global.buffer.chunksPerList,
		this->config.slave.cache.chunks
	);
	/* Chunk pool */
	this->chunkPool = MemoryPool<Chunk>::getInstance();
	/* Workers and event queues */
	if ( this->config.slave.workers.type == WORKER_TYPE_MIXED ) {
		this->eventQueue.init(
			this->config.slave.eventQueue.block,
			this->config.slave.eventQueue.size.mixed
		);
		this->workers.reserve( this->config.slave.workers.number.mixed );
		for ( int i = 0, len = this->config.slave.workers.number.mixed; i < len; i++ ) {
			this->workers.push_back( SlaveWorker() );
			this->workers[ i ].init(
				this->config.global,
				WORKER_ROLE_MIXED,
				&this->eventQueue
			);
		}
	} else {
		this->workers.reserve( this->config.slave.workers.number.separated.total );
		this->eventQueue.init(
			this->config.slave.eventQueue.block,
			this->config.slave.eventQueue.size.separated.coordinator,
			this->config.slave.eventQueue.size.separated.master,
			this->config.slave.eventQueue.size.separated.slave,
			this->config.slave.eventQueue.size.separated.slavePeer
		);

		int index = 0;
#define WORKER_INIT_LOOP( _FIELD_, _CONSTANT_ ) \
		for ( int i = 0, len = this->config.slave.workers.number.separated._FIELD_; i < len; i++, index++ ) { \
			this->workers.push_back( SlaveWorker() ); \
			this->workers[ index ].init( \
				this->config.global, \
				_CONSTANT_, \
				&this->eventQueue \
			); \
		}

		WORKER_INIT_LOOP( coordinator, WORKER_ROLE_COORDINATOR )
		WORKER_INIT_LOOP( master, WORKER_ROLE_MASTER )
		WORKER_INIT_LOOP( slave, WORKER_ROLE_SLAVE )
		WORKER_INIT_LOOP( slave, WORKER_ROLE_SLAVE_PEER )
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
			return false;
	}
	// Start listening
	if ( ! this->sockets.self.start() ) {
		__ERROR__( "Slave", "start", "Cannot start socket." );
		return false;
	}

	this->startTime = start_timer();
	this->isRunning = true;

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
	for ( i = 0, len = this->sockets.slavePeers.size(); i < len; i++ )
		this->sockets.slavePeers[ i ].stop();

	/* Chunk buffer */
	this->chunkBuffer->stop();

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

void Slave::help() {
	fprintf(
		stdout,
		"Supported commands:\n"
		"- help: Show this help message\n"
		"- info: Show configuration\n"
		"- debug: Show debug messages\n"
		"- time: Show elapsed time\n"
		"- exit: Terminate this client\n"
	);
	fflush( stdout );
}

void Slave::time() {
	fprintf( stdout, "Elapsed time: %12.6lf s\n", this->getElapsedTime() );
	fflush( stdout );
}
