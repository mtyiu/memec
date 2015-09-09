#include <cstring>
#include <ctype.h>
#include "master.hh"

Master::Master() {
	this->isRunning = false;
}

void Master::free() {
	this->eventQueue.free();
	delete this->stripeList;
}

void Master::signalHandler( int signal ) {
	Signal::setHandler();
	Master::getInstance()->stop();
	fclose( stdin );
}

bool Master::init( char *path, OptionList &options, bool verbose ) {
	// Parse configuration files //
	if ( ( ! this->config.global.parse( path ) ) ||
	     ( ! this->config.master.merge( this->config.global ) ) ||
	     ( ! this->config.master.parse( path ) ) ||
	     ( ! this->config.master.override( options ) ) ) {
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
	Socket::init( &this->sockets.epoll );
	ApplicationSocket::setArrayMap( &this->sockets.applications );
	CoordinatorSocket::setArrayMap( &this->sockets.coordinators );
	SlaveSocket::setArrayMap( &this->sockets.slaves );
	// this->sockets.applications.reserve( 20000 );
	this->sockets.coordinators.reserve( this->config.global.coordinators.size() );
	for ( int i = 0, len = this->config.global.coordinators.size(); i < len; i++ ) {
		CoordinatorSocket *socket = new CoordinatorSocket();
		int fd;

		socket->init( this->config.global.coordinators[ i ], &this->sockets.epoll );
		fd = socket->getSocket();
		this->sockets.coordinators.set( fd, socket );
	}
	this->sockets.slaves.reserve( this->config.global.slaves.size() );
	for ( int i = 0, len = this->config.global.slaves.size(); i < len; i++ ) {
		SlaveSocket *socket = new SlaveSocket();
		int fd;

		socket->init( this->config.global.slaves[ i ], &this->sockets.epoll );
		fd = socket->getSocket();
		this->sockets.slaves.set( fd, socket );
	}
	/* Stripe list */
	this->stripeList = new StripeList<SlaveSocket>(
		this->config.global.coding.params.getChunkCount(),
		this->config.global.coding.params.getDataChunkCount(),
		this->config.global.stripeList.count,
		this->sockets.slaves.values
	);
	/* Workers, packet pool and event queues */
	if ( this->config.master.workers.type == WORKER_TYPE_MIXED ) {
		this->packetPool.init(
			this->config.master.workers.number.mixed,
			Protocol::getSuggestedBufferSize(
				this->config.global.size.key,
				this->config.global.size.chunk
			)
		);
		this->eventQueue.init(
			this->config.master.eventQueue.block,
			this->config.master.eventQueue.size.mixed,
			this->config.master.eventQueue.size.pMixed
		);
		this->workers.reserve( this->config.master.workers.number.mixed );
		MasterWorker::init();
		for ( int i = 0, len = this->config.master.workers.number.mixed; i < len; i++ ) {
			this->workers.push_back( MasterWorker() );
			this->workers[ i ].init(
				this->config.global,
				WORKER_ROLE_MIXED
			);
		}
	} else {
		this->packetPool.init(
			this->config.master.workers.number.mixed,
			Protocol::getSuggestedBufferSize(
				this->config.global.size.key,
				this->config.global.size.chunk
			)
		);
		this->workers.reserve( this->config.master.workers.number.separated.total );
		this->eventQueue.init(
			this->config.master.eventQueue.block,
			this->config.master.eventQueue.size.separated.application,
			this->config.master.eventQueue.size.separated.coordinator,
			this->config.master.eventQueue.size.separated.master,
			this->config.master.eventQueue.size.separated.slave
		);

		int index = 0;
#define WORKER_INIT_LOOP( _FIELD_, _CONSTANT_ ) \
		for ( int i = 0, len = this->config.master.workers.number.separated._FIELD_; i < len; i++, index++ ) { \
			this->workers.push_back( MasterWorker() ); \
			this->workers[ index ].init( \
				this->config.global, \
				_CONSTANT_ \
			); \
		}

		MasterWorker::init();
		WORKER_INIT_LOOP( application, WORKER_ROLE_APPLICATION )
		WORKER_INIT_LOOP( coordinator, WORKER_ROLE_COORDINATOR )
		WORKER_INIT_LOOP( master, WORKER_ROLE_MASTER )
		WORKER_INIT_LOOP( slave, WORKER_ROLE_SLAVE )
#undef WORKER_INIT_LOOP
	}

	// Set signal handlers //
	Signal::setHandler( Master::signalHandler );

	// Show configuration //
	if ( verbose )
		this->info();
	return true;
}

bool Master::start() {
	bool ret = true;
	/* Workers and event queues */
	this->eventQueue.start();
	if ( this->config.master.workers.type == WORKER_TYPE_MIXED ) {
		for ( int i = 0, len = this->config.master.workers.number.mixed; i < len; i++ ) {
			this->workers[ i ].start();
		}
	} else {
		for ( int i = 0, len = this->config.master.workers.number.separated.total; i < len; i++ ) {
			this->workers[ i ].start();
		}
	}

	/* Socket */
	// Connect to coordinators
	for ( int i = 0, len = this->config.global.coordinators.size(); i < len; i++ ) {
		if ( ! this->sockets.coordinators[ i ]->start() )
			ret = false;
	}
	// Connect to slaves
	for ( int i = 0, len = this->config.global.slaves.size(); i < len; i++ ) {
		if ( ! this->sockets.slaves[ i ]->start() )
			ret = false;
	}
	// Start listening
	if ( ! this->sockets.self.start() ) {
		__ERROR__( "Master", "start", "Cannot start socket." );
		ret = false;
	}

	this->startTime = start_timer();
	this->isRunning = true;

	return ret;
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
	this->eventQueue.stop();

	/* Workers */
	for ( i = len - 1; i >= 0; i-- )
		this->workers[ i ].join();

	/* Sockets */
	for ( i = 0, len = this->sockets.applications.size(); i < len; i++ )
		this->sockets.applications[ i ]->stop();
	this->sockets.applications.clear();
	for ( i = 0, len = this->sockets.coordinators.size(); i < len; i++ )
		this->sockets.coordinators[ i ]->stop();
	this->sockets.coordinators.clear();
	for ( i = 0, len = this->sockets.slaves.size(); i < len; i++ )
		this->sockets.slaves[ i ]->stop();
	this->sockets.slaves.clear();

	this->free();
	this->isRunning = false;
	printf( "\nBye.\n" );
	return true;
}

double Master::getElapsedTime() {
	return get_elapsed_time( this->startTime );
}

void Master::info( FILE *f ) {
	this->config.global.print( f );
	this->config.master.print( f );
	this->stripeList->print( f );
}

void Master::debug( FILE *f ) {
	int i, len;

	fprintf( f, "Master socket\n-------------\n" );
	this->sockets.self.print( f );

	fprintf( f, "\nApplication sockets\n-------------------\n" );
	for ( i = 0, len = this->sockets.applications.size(); i < len; i++ ) {
		fprintf( f, "%d. ", i + 1 );
		this->sockets.applications[ i ]->print( f );
	}
	if ( len == 0 ) fprintf( f, "(None)\n" );

	fprintf( f, "\nCoordinator sockets\n-------------------\n" );
	for ( i = 0, len = this->sockets.coordinators.size(); i < len; i++ ) {
		fprintf( f, "%d. ", i + 1 );
		this->sockets.coordinators[ i ]->print( f );
	}
	if ( len == 0 ) fprintf( f, "(None)\n" );

	fprintf( f, "\nSlave sockets\n-------------\n" );
	for ( i = 0, len = this->sockets.slaves.size(); i < len; i++ ) {
		fprintf( f, "%d. ", i + 1 );
		this->sockets.slaves[ i ]->print( f );
	}
	if ( len == 0 ) fprintf( f, "(None)\n" );

	fprintf( f, "\nMaster event queue\n------------------\n" );
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
			this->info();
		} else if ( strcmp( command, "debug" ) == 0 ) {
			valid = true;
			this->debug();
		} else if ( strcmp( command, "pending" ) == 0 ) {
			valid = true;
			this->printPending();
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

void Master::printPending( FILE *f ) {
	size_t i;
	std::set<Key>::iterator it;
	std::set<KeyValueUpdate>::iterator keyValueUpdateIt;

	pthread_mutex_lock( &this->pending.applications.setLock );
	fprintf(
		f,
		"Pending requests for applications\n"
		"---------------------------------\n"
		"[SET] Pending: %lu\n",
		this->pending.applications.set.size()
	);
	i = 1;
	for (
		it = this->pending.applications.set.begin();
		it != this->pending.applications.set.end();
		it++, i++
	) {
		const Key &key = *it;
		fprintf( f, "%lu. Key: %.*s (size = %u); source: ", i, key.size, key.data, key.size );
		( ( Socket * ) key.ptr )->printAddress( f );

		for ( uint8_t i = 0; i < key.size; i++ ) {
			if ( ! isprint( key.data[ i ] ) ) {
				fprintf( f, " %u", i );
			}
		}

		fprintf( f, "\n" );
	}
	pthread_mutex_unlock( &this->pending.applications.setLock );

	pthread_mutex_lock( &this->pending.applications.getLock );
	fprintf(
		f,
		"\n[GET] Pending: %lu\n",
		this->pending.applications.get.size()
	);
	i = 1;
	for (
		it = this->pending.applications.get.begin();
		it != this->pending.applications.get.end();
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
	pthread_mutex_unlock( &this->pending.applications.getLock );

	pthread_mutex_lock( &this->pending.applications.updateLock );
	fprintf(
		f,
		"\n[UPDATE] Pending: %lu\n",
		this->pending.applications.update.size()
	);
	i = 1;
	for (
		keyValueUpdateIt = this->pending.applications.update.begin();
		keyValueUpdateIt != this->pending.applications.update.end();
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
	pthread_mutex_unlock( &this->pending.applications.updateLock );

	pthread_mutex_lock( &this->pending.applications.delLock );
	fprintf(
		f,
		"\n[DELETE] Pending: %lu\n",
		this->pending.applications.del.size()
	);
	i = 1;
	for (
		it = this->pending.applications.del.begin();
		it != this->pending.applications.del.end();
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
	pthread_mutex_unlock( &this->pending.applications.delLock );

	pthread_mutex_lock( &this->pending.slaves.setLock );
	fprintf(
		f,
		"\n\nPending requests for slaves\n"
		"---------------------------\n"
		"[SET] Pending: %lu\n",
		this->pending.slaves.set.size()
	);

	i = 1;
	for (
		it = this->pending.slaves.set.begin();
		it != this->pending.slaves.set.end();
		it++, i++
	) {
		const Key &key = *it;
		fprintf( f, "%lu. Key: %.*s (size = %u); target: ", i, key.size, key.data, key.size );
		( ( Socket * ) key.ptr )->printAddress( f );
		fprintf( f, "\n" );
	}
	pthread_mutex_unlock( &this->pending.slaves.setLock );

	pthread_mutex_lock( &this->pending.slaves.getLock );
	fprintf(
		f,
		"\n[GET] Pending: %lu\n",
		this->pending.slaves.get.size()
	);
	i = 1;
	for (
		it = this->pending.slaves.get.begin();
		it != this->pending.slaves.get.end();
		it++, i++
	) {
		const Key &key = *it;
		fprintf( f, "%lu. Key: %.*s (size = %u); target: ", i, key.size, key.data, key.size );
		( ( Socket * ) key.ptr )->printAddress( f );
		fprintf( f, "\n" );
	}
	pthread_mutex_unlock( &this->pending.slaves.getLock );

	pthread_mutex_lock( &this->pending.slaves.updateLock );
	fprintf(
		f,
		"\n[UPDATE] Pending: %lu\n",
		this->pending.slaves.update.size()
	);
	i = 1;
	for (
		keyValueUpdateIt = this->pending.slaves.update.begin();
		keyValueUpdateIt != this->pending.slaves.update.end();
		keyValueUpdateIt++, i++
	) {
		const KeyValueUpdate &keyValueUpdate = *keyValueUpdateIt;
		fprintf(
			f, "%lu. Key: %.*s (size = %u, offset = %u, length = %u); target: ",
			i, keyValueUpdate.size, keyValueUpdate.data, keyValueUpdate.size,
			keyValueUpdate.offset, keyValueUpdate.length
		);
		if ( keyValueUpdate.ptr )
			( ( Socket * ) keyValueUpdate.ptr )->printAddress( f );
		else
			fprintf( f, "(nil)\n" );
		fprintf( f, "\n" );
	}
	pthread_mutex_unlock( &this->pending.slaves.updateLock );

	pthread_mutex_lock( &this->pending.slaves.delLock );
	fprintf(
		f,
		"\n[DELETE] Pending: %lu\n",
		this->pending.slaves.del.size()
	);
	i = 1;
	for (
		it = this->pending.slaves.del.begin();
		it != this->pending.slaves.del.end();
		it++, i++
	) {
		const Key &key = *it;
		fprintf( f, "%lu. Key: %.*s (size = %u); target: ", i, key.size, key.data, key.size );
		( ( Socket * ) key.ptr )->printAddress( f );
		fprintf( f, "\n" );
	}
	pthread_mutex_unlock( &this->pending.slaves.delLock );
}

void Master::help() {
	fprintf(
		stdout,
		"Supported commands:\n"
		"- help: Show this help message\n"
		"- info: Show configuration\n"
		"- debug: Show debug messages\n"
		"- pending: Show all pending requests\n"
		"- time: Show elapsed time\n"
		"- exit: Terminate this client\n"
	);
	fflush( stdout );
}

void Master::time() {
	fprintf( stdout, "Elapsed time: %12.6lf s\n", this->getElapsedTime() );
	fflush( stdout );
}
