#include <cstring>
#include <ctype.h>
#include "slave.hh"

Slave::Slave() {
	this->isRunning = false;
	memset( &this->eventQueue, 0, sizeof( this->eventQueue ) );
}

void Slave::free() {
	if ( this->config.slave.workers.type == WORKER_TYPE_MIXED ) {
		delete this->eventQueue.mixed;
	} else {
		delete this->eventQueue.separated.application;
		delete this->eventQueue.separated.coordinator;
		delete this->eventQueue.separated.master;
		delete this->eventQueue.separated.slave;
	}
}

void Slave::signalHandler( int signal ) {
	Signal::setHandler();
	Slave::getInstance()->stop();
	fclose( stdin );
}

bool Slave::init( char *path, bool verbose ) {
	bool ret;
	// Parse configuration files //
	if ( ( ! ( ret = this->config.global.parse( path ) ) ) ||
	     ( ! ( ret = this->config.slave.merge( this->config.global ) ) ) ||
	     ( ! ( ret = this->config.slave.parse( path ) ) ) ||
	     ( ! this->config.slave.validate( this->config.global.slaves ) ) ) {
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
		this->sockets.coordinators.push_back( CoordinatorSocket() );
		this->sockets.coordinators[ i ].init( this->config.global.coordinators[ i ] );
	}
	/*
	this->sockets.slaves.reserve( this->config.global.slaves.size() );
	for ( int i = 0, len = this->config.global.slaves.size(); i < len; i++ ) {
		this->sockets.slaves.push_back( SlaveSocket() );
		this->sockets.slaves[ i ].init( this->config.global.slaves[ i ] );
	}
	*/
	/* Workers and event queues */
	if ( this->config.slave.workers.type == WORKER_TYPE_MIXED ) {
		this->eventQueue.mixed = new EventQueue<MixedEvent>(
			this->config.slave.eventQueue.size.mixed,
			this->config.slave.eventQueue.block
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

		this->eventQueue.separated.application = new EventQueue<ApplicationEvent>(
			this->config.slave.eventQueue.size.separated.application,
			this->config.slave.eventQueue.block
		);
		this->eventQueue.separated.coordinator = new EventQueue<CoordinatorEvent>(
			this->config.slave.eventQueue.size.separated.coordinator,
			this->config.slave.eventQueue.block
		);
		this->eventQueue.separated.master = new EventQueue<MasterEvent>(
			this->config.slave.eventQueue.size.separated.master,
			this->config.slave.eventQueue.block
		);
		this->eventQueue.separated.slave = new EventQueue<SlaveEvent>(
			this->config.slave.eventQueue.size.separated.slave,
			this->config.slave.eventQueue.block
		);

		int index = 0;
		for ( int i = 0, len = this->config.slave.workers.number.separated.application; i < len; i++, index++ ) {
			this->workers.push_back( SlaveWorker() );
			this->workers[ index ].init(
				this->config.global,
				WORKER_ROLE_APPLICATION,
				&this->eventQueue
			);
		}
		for ( int i = 0, len = this->config.slave.workers.number.separated.coordinator; i < len; i++, index++ ) {
			this->workers.push_back( SlaveWorker() );
			this->workers[ index ].init(
				this->config.global,
				WORKER_ROLE_COORDINATOR,
				&this->eventQueue
			);
		}
		for ( int i = 0, len = this->config.slave.workers.number.separated.master; i < len; i++, index++ ) {
			this->workers.push_back( SlaveWorker() );
			this->workers[ index ].init(
				this->config.global,
				WORKER_ROLE_MASTER,
				&this->eventQueue
			);
		}
		for ( int i = 0, len = this->config.slave.workers.number.separated.slave; i < len; i++, index++ ) {
			this->workers.push_back( SlaveWorker() );
			this->workers[ index ].init(
				this->config.global,
				WORKER_ROLE_SLAVE,
				&this->eventQueue
			);
		}
	}

	// Set signal handlers //
	Signal::setHandler( Slave::signalHandler );

	// Show configuration //
	if ( verbose ) {
		this->config.global.print();
		this->config.slave.print();
	}
	return true;
}

bool Slave::start() {
	/* Sockets */
	// Connect to coordinators
	for ( int i = 0, len = this->config.global.coordinators.size(); i < len; i++ ) {
		if ( ! this->sockets.coordinators[ i ].start() )
			return false;
	}

	/* Workers and event queues */
	if ( this->config.slave.workers.type == WORKER_TYPE_MIXED ) {
		this->eventQueue.mixed->start();
		for ( int i = 0, len = this->config.slave.workers.number.mixed; i < len; i++ ) {
			if ( this->workers[ i ].start() ) {
				this->workers[ i ].debug();
			}
		}
	} else {
		this->eventQueue.separated.application->start();
		this->eventQueue.separated.coordinator->start();
		this->eventQueue.separated.master->start();
		this->eventQueue.separated.slave->start();
		for ( int i = 0, len = this->config.slave.workers.number.separated.total; i < len; i++ ) {
			if ( this->workers[ i ].start() ) {
				this->workers[ i ].debug();
			}
		}
	}

	/* Socket */
	// Start listening
	if ( ! this->sockets.self.start() ) {
		__ERROR__( "Slave", "start", "Cannot start socket." );
		return false;
	} else {
		this->sockets.self.debug();
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
	if ( this->config.slave.workers.type == WORKER_TYPE_MIXED ) {
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

	this->free();
	this->isRunning = false;
	printf( "\nBye.\n" );
	return true;
}

double Slave::getElapsedTime() {
	return get_elapsed_time( this->startTime );
}

void Slave::print( FILE *f ) {
	this->config.global.print( f );
	this->config.slave.print( f );
}

void Slave::debug( FILE *f ) {

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

void Slave::help() {

}

void Slave::info() {

}

void Slave::time() {
	printf( "Elapsed time: %12.6lf s\n", this->getElapsedTime() );
}
