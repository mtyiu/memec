#include <cstring>
#include <ctype.h>
#include "master.hh"

Master::Master() {
	this->isRunning = false;
	memset( &this->eventQueue, 0, sizeof( this->eventQueue ) );
}

void Master::free() {
	if ( this->config.master.workers.type == WORKER_TYPE_MIXED ) {
		delete this->eventQueue.mixed;
	} else {
		delete this->eventQueue.separated.application;
		delete this->eventQueue.separated.coordinator;
		delete this->eventQueue.separated.master;
		delete this->eventQueue.separated.slave;
	}
}

void Master::signalHandler( int signal ) {
	Signal::setHandler();
	Master::getInstance()->stop();
	fclose( stdin );
}

bool Master::init( char *path, bool verbose ) {
	bool ret;
	// Parse configuration files //
	if ( ( ! ( ret = this->config.global.parse( path ) ) ) ||
	     ( ! ( ret = this->config.master.merge( this->config.global ) ) ) ||
	     ( ! ( ret = this->config.master.parse( path ) ) ) ) {
		return false;
	}

	// Initialize modules //
	/* Socket */
	if ( ! this->sockets.epoll.init(
			this->config.master.epoll.maxEvents,
			this->config.master.epoll.timeout
		) || ! this->sockets.self.init(
			this->config.master.master.addr.type,
			this->config.master.master.addr.addr,
			this->config.master.master.addr.port,
			&this->sockets.epoll
		) ) {
		__ERROR__( "Master", "init", "Cannot initialize socket." );
		return false;
	}
	/* Vectors and other sockets */
	this->sockets.coordinators.reserve( this->config.global.coordinators.size() );
	for ( int i = 0, len = this->config.global.coordinators.size(); i < len; i++ ) {
		this->sockets.coordinators.push_back( CoordinatorSocket() );
		this->sockets.coordinators[ i ].init( this->config.global.coordinators[ i ] );
	}
	this->sockets.slaves.reserve( this->config.global.slaves.size() );
	for ( int i = 0, len = this->config.global.slaves.size(); i < len; i++ ) {
		this->sockets.slaves.push_back( SlaveSocket() );
		this->sockets.slaves[ i ].init( this->config.global.slaves[ i ] );
	}
	/* Workers and event queues */
	if ( this->config.master.workers.type == WORKER_TYPE_MIXED ) {
		this->eventQueue.mixed = new EventQueue<MixedEvent>(
			this->config.master.eventQueue.size.mixed,
			this->config.master.eventQueue.block
		);
		this->workers.reserve( this->config.master.workers.number.mixed );
		for ( int i = 0, len = this->config.master.workers.number.mixed; i < len; i++ ) {
			this->workers.push_back( MasterWorker() );
			this->workers[ i ].init( WORKER_ROLE_MIXED, &this->eventQueue );
		}
	} else {
		this->workers.reserve( this->config.master.workers.number.separated.total );

		this->eventQueue.separated.application = new EventQueue<ApplicationEvent>(
			this->config.master.eventQueue.size.separated.application,
			this->config.master.eventQueue.block
		);
		this->eventQueue.separated.coordinator = new EventQueue<CoordinatorEvent>(
			this->config.master.eventQueue.size.separated.coordinator,
			this->config.master.eventQueue.block
		);
		this->eventQueue.separated.master = new EventQueue<MasterEvent>(
			this->config.master.eventQueue.size.separated.master,
			this->config.master.eventQueue.block
		);
		this->eventQueue.separated.slave = new EventQueue<SlaveEvent>(
			this->config.master.eventQueue.size.separated.slave,
			this->config.master.eventQueue.block
		);

		int index = 0;
		for ( int i = 0, len = this->config.master.workers.number.separated.application; i < len; i++, index++ ) {
			this->workers.push_back( MasterWorker() );
			this->workers[ index ].init( WORKER_ROLE_APPLICATION, &this->eventQueue );
		}
		for ( int i = 0, len = this->config.master.workers.number.separated.coordinator; i < len; i++, index++ ) {
			this->workers.push_back( MasterWorker() );
			this->workers[ index ].init( WORKER_ROLE_COORDINATOR, &this->eventQueue );
		}
		for ( int i = 0, len = this->config.master.workers.number.separated.master; i < len; i++, index++ ) {
			this->workers.push_back( MasterWorker() );
			this->workers[ index ].init( WORKER_ROLE_MASTER, &this->eventQueue );
		}
		for ( int i = 0, len = this->config.master.workers.number.separated.slave; i < len; i++, index++ ) {
			this->workers.push_back( MasterWorker() );
			this->workers[ index ].init( WORKER_ROLE_SLAVE, &this->eventQueue );
		}
	}

	// Set signal handlers //
	Signal::setHandler( Master::signalHandler );

	// Show configuration //
	if ( verbose ) {
		this->config.global.print();
		this->config.master.print();
	}
	return true;
}

bool Master::start() {
	/* Sockets */
	// Connect to coordinators
	for ( int i = 0, len = this->config.global.coordinators.size(); i < len; i++ ) {
		if ( ! this->sockets.coordinators[ i ].start() )
			return false;
	}
	// Connect to slaves
	for ( int i = 0, len = this->config.global.slaves.size(); i < len; i++ ) {
		if ( ! this->sockets.slaves[ i ].start() )
			return false;
	}

	/* Workers and event queues */
	if ( this->config.master.workers.type == WORKER_TYPE_MIXED ) {
		this->eventQueue.mixed->start();
		for ( int i = 0, len = this->config.master.workers.number.mixed; i < len; i++ ) {
			if ( this->workers[ i ].start() ) {
				this->workers[ i ].debug();
			}
		}
	} else {
		this->eventQueue.separated.application->start();
		this->eventQueue.separated.coordinator->start();
		this->eventQueue.separated.master->start();
		this->eventQueue.separated.slave->start();
		for ( int i = 0, len = this->config.master.workers.number.separated.total; i < len; i++ ) {
			if ( this->workers[ i ].start() ) {
				this->workers[ i ].debug();
			}
		}
	}

	/* Socket */
	// Start listening
	if ( ! this->sockets.self.start() ) {
		__ERROR__( "Master", "start", "Cannot start socket." );
		return false;
	} else {
		this->sockets.self.debug();
	}

	this->startTime = start_timer();
	this->isRunning = true;

	return true;
}

bool Master::stop() {
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
	if ( this->config.master.workers.type == WORKER_TYPE_MIXED ) {
		this->eventQueue.mixed->stop();
	} else {
		this->eventQueue.separated.application->stop();
		this->eventQueue.separated.coordinator->stop();
		this->eventQueue.separated.master->stop();
		this->eventQueue.separated.slave->stop();
	}

	/* Workers */
	for ( i = len - 1; i >= 0; i-- )
		this->workers[ i ].join();

	/* Sockets */
	for ( i = 0, len = this->sockets.coordinators.size(); i < len; i++ )
		this->sockets.coordinators[ i ].stop();
	for ( i = 0, len = this->sockets.slaves.size(); i < len; i++ )
		this->sockets.slaves[ i ].stop();

	this->free();
	this->isRunning = false;
	printf( "\nBye.\n" );
	return true;
}

double Master::getElapsedTime() {
	return get_elapsed_time( this->startTime );
}

void Master::print( FILE *f ) {
	this->config.global.print( f );
	this->config.master.print( f );
}


void Master::debug( FILE *f ) {

}

void Master::interactive() {
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
			this->print();
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

void Master::help() {

}

void Master::info() {

}

void Master::time() {
	printf( "Elapsed time: %12.6lf s\n", this->getElapsedTime() );
}
