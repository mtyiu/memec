#include <cstring>
#include <ctype.h>
#include "coordinator.hh"

Coordinator::Coordinator() {
	this->isRunning = false;
}

void Coordinator::free() {
	this->eventQueue.free();
}

void Coordinator::signalHandler( int signal ) {
	Signal::setHandler();
	Coordinator::getInstance()->stop();
	fclose( stdin );
}

bool Coordinator::init( char *path, OptionList &options, bool verbose ) {
	// Parse configuration files //
	if ( ( ! this->config.global.parse( path ) ) ||
	     ( ! this->config.coordinator.merge( this->config.global ) ) ||
	     ( ! this->config.coordinator.parse( path ) ) ||
	     ( ! this->config.coordinator.override( options ) ) ||
	     ( ! this->config.coordinator.validate( this->config.global.coordinators ) ) ) {
		return false;
	}

	// Initialize modules //
	/* Socket */
	if ( ! this->sockets.epoll.init(
			this->config.coordinator.epoll.maxEvents,
			this->config.coordinator.epoll.timeout
		) || ! this->sockets.self.init(
			this->config.coordinator.coordinator.addr.type,
			this->config.coordinator.coordinator.addr.addr,
			this->config.coordinator.coordinator.addr.port,
			this->config.global.slaves.size(),
			&this->sockets.epoll
		) ) {
		__ERROR__( "Coordinator", "init", "Cannot initialize socket." );
		return false;
	}
	/* Vectors and other sockets */
	Socket::init( &this->sockets.epoll );
	this->sockets.masters.reserve( this->config.global.slaves.size() );
	this->sockets.slaves.reserve( this->config.global.slaves.size() );
	/* Workers and event queues */
	if ( this->config.coordinator.workers.type == WORKER_TYPE_MIXED ) {
		this->eventQueue.init(
			this->config.coordinator.eventQueue.block,
			this->config.coordinator.eventQueue.size.mixed
		);
		CoordinatorWorker::init();
		this->workers.reserve( this->config.coordinator.workers.number.mixed );
		for ( int i = 0, len = this->config.coordinator.workers.number.mixed; i < len; i++ ) {
			this->workers.push_back( CoordinatorWorker() );
			this->workers[ i ].init(
				this->config.global,
				WORKER_ROLE_MIXED
			);
		}
	} else {
		this->workers.reserve( this->config.coordinator.workers.number.separated.total );
		this->eventQueue.init(
			this->config.coordinator.eventQueue.block,
			this->config.coordinator.eventQueue.size.separated.coordinator,
			this->config.coordinator.eventQueue.size.separated.master,
			this->config.coordinator.eventQueue.size.separated.slave
		);
		CoordinatorWorker::init();

		int index = 0;
#define WORKER_INIT_LOOP( _FIELD_, _CONSTANT_ ) \
		for ( int i = 0, len = this->config.coordinator.workers.number.separated._FIELD_; i < len; i++, index++ ) { \
			this->workers.push_back( CoordinatorWorker() ); \
			this->workers[ index ].init( \
				this->config.global, \
				_CONSTANT_ \
			); \
		}

		WORKER_INIT_LOOP( coordinator, WORKER_ROLE_COORDINATOR )
		WORKER_INIT_LOOP( master, WORKER_ROLE_MASTER )
		WORKER_INIT_LOOP( slave, WORKER_ROLE_SLAVE )
#undef WORKER_INIT_LOOP
	}

	/* Remapping message handler */
	char coordName[ 11 ];
	memset( coordName, 0, 11 );
	sprintf( coordName, "%s%03d", COORD_PREFIX, this->config.coordinator.coordinator.addr.port % 1000 );
	remapMsgHandler.init( this->config.global.spreadd.addr, this->config.global.spreadd.port, coordName );

	// Set signal handlers //
	Signal::setHandler( Coordinator::signalHandler );

	// Show configuration //
	if ( verbose )
		this->info();
	return true;
}

bool Coordinator::start() {
	/* Workers and event queues */
	this->eventQueue.start();
	if ( this->config.coordinator.workers.type == WORKER_TYPE_MIXED ) {
		for ( int i = 0, len = this->config.coordinator.workers.number.mixed; i < len; i++ ) {
			this->workers[ i ].start();
		}
	} else {
		for ( int i = 0, len = this->config.coordinator.workers.number.separated.total; i < len; i++ ) {
			this->workers[ i ].start();
		}
	}

	/* Sockets */
	if ( ! this->sockets.self.start() ) {
		__ERROR__( "Coordinator", "start", "Cannot start socket." );
		return false;
	}

	/* Remapping message handler */
	if ( ! this->remapMsgHandler.start() ) {
		__ERROR__( "Coordinator", "start", "Cannot start remapping message handler." );
		return false;
	}

	this->startTime = start_timer();
	this->isRunning = true;

	return true;
}

bool Coordinator::stop() {
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
	pthread_mutex_lock( &this->sockets.masters.lock );
	for ( i = 0, len = this->sockets.masters.size(); i < len; i++ )
		this->sockets.masters[ i ].stop();
	pthread_mutex_unlock( &this->sockets.masters.lock );

	pthread_mutex_lock( &this->sockets.slaves.lock );
	for ( i = 0, len = this->sockets.slaves.size(); i < len; i++ )
		this->sockets.slaves[ i ].stop();
	pthread_mutex_unlock( &this->sockets.slaves.lock );

	/* Remapping message handler */
	this->remapMsgHandler.stop();
	this->remapMsgHandler.quit();

	this->free();
	this->isRunning = false;
	printf( "\nBye.\n" );
	return true;
}

double Coordinator::getElapsedTime() {
	return get_elapsed_time( this->startTime );
}

void Coordinator::info( FILE *f ) {
	this->config.global.print( f );
	this->config.coordinator.print( f );
}

void Coordinator::debug( FILE *f ) {
	int i, len;

	fprintf( f, "Coordinator socket\n------------------\n" );
	this->sockets.self.print( f );

	fprintf( f, "\nMaster sockets\n--------------\n" );
	for ( i = 0, len = this->sockets.masters.size(); i < len; i++ ) {
		fprintf( f, "%d. ", i + 1 );
		this->sockets.masters[ i ].print( f );
	}
	if ( len == 0 ) fprintf( f, "(None)\n" );

	fprintf( f, "\nSlave sockets\n-------------\n" );
	for ( i = 0, len = this->sockets.slaves.size(); i < len; i++ ) {
		fprintf( f, "%d. ", i + 1 );
		this->sockets.slaves[ i ].print( f );
	}
	if ( len == 0 ) fprintf( f, "(None)\n" );

	fprintf( f, "\nCoordinator event queue\n-----------------------\n" );
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

void Coordinator::interactive() {
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

void Coordinator::dump() {
	FILE *f = stdout;
	for ( size_t i = 0, len = this->sockets.slaves.size(); i < len; i++ ) {
		std::map<Key, OpMetadata> &map = this->sockets.slaves[ i ].keys;

		fprintf( f, "Slave #%lu: ", i + 1 );
		this->sockets.slaves[ i ].printAddress( f );
		fprintf( f, "\n----------------------------------------\n" );

		fprintf( f, "[Load]\n" );
		this->sockets.slaves[ i ].load.print( f );

		fprintf( f, "\n[List of metadata]\n" );
		if ( ! map.size() ) {
			fprintf( f, "(None)\n" );
		} else {
			for ( std::map<Key, OpMetadata>::iterator it = map.begin(); it != map.end(); it++ ) {
				fprintf(
					f, "%.*s --> (list: %u, stripe: %u, chunk: %u)\n",
					it->first.size, it->first.data,
					it->second.listId, it->second.stripeId, it->second.chunkId
				);
			}
		}
		fprintf( f, "\n" );
	}
}

void Coordinator::help() {
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

void Coordinator::time() {
	fprintf( stdout, "Elapsed time: %12.6lf s\n", this->getElapsedTime() );
	fflush( stdout );
}
