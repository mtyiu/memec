#include <cstring>
#include <cerrno>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "application.hh"

Application::Application() {
	this->isRunning = false;
}

void Application::free() {
	this->eventQueue.free();
}

void Application::signalHandler( int signal ) {
	Signal::setHandler();
	Application::getInstance()->stop();
	fclose( stdin );
}

void *Application::run( void *argv ) {
	Application *application = ( Application * ) argv;
	application->sockets.epoll.start( Application::epollHandler, application );
	pthread_exit( 0 );
	return 0;
}

bool Application::epollHandler( int fd, uint32_t events, void *data ) {
	Application *application = ( Application * ) data;
	MasterSocket *masterSocket = application->sockets.masters.get( fd );

	if ( ! masterSocket ) {
		__ERROR__( "Application", "epollHandler", "Unknown socket." );
		return false;
	}

	if ( ( events & EPOLLERR ) || ( events & EPOLLHUP ) || ( events & EPOLLRDHUP ) ) {
		masterSocket->stop();
	} else {
		MasterEvent event;
		event.pending( masterSocket );
		application->eventQueue.insert( event );
	}
	return true;
}

bool Application::init( char *path, OptionList &options, bool verbose ) {
	// Parse configuration files //
	if ( ( ! this->config.application.parse( path ) ) ||
	     ( ! this->config.application.override( options ) ) ) {
		return false;
	}

	// Initialize modules //
	/* Socket */
	if ( ! this->sockets.epoll.init(
			this->config.application.epoll.maxEvents,
			this->config.application.epoll.timeout
		) ) {
		__ERROR__( "Application", "init", "Cannot initialize socket." );
		return false;
	}
	/* Vectors and other sockets */
	this->sockets.masters.reserve( this->config.application.masters.size() );
	for ( int i = 0, len = this->config.application.masters.size(); i < len; i++ ) {
		MasterSocket socket;
		int fd;

		socket.init( this->config.application.masters[ i ], &this->sockets.epoll );
		fd = socket.getSocket();
		this->sockets.masters.set( fd, socket );
	}
	/* Workers and event queues */
	if ( this->config.application.workers.type == WORKER_TYPE_MIXED ) {
		this->eventQueue.init(
			this->config.application.eventQueue.block,
			this->config.application.eventQueue.size.mixed
		);
		ApplicationWorker::init();
		this->workers.reserve( this->config.application.workers.number.mixed );
		for ( int i = 0, len = this->config.application.workers.number.mixed; i < len; i++ ) {
			this->workers.push_back( ApplicationWorker() );
			this->workers[ i ].init(
				this->config.application,
				WORKER_ROLE_MIXED
			);
		}
	} else {
		this->workers.reserve( this->config.application.workers.number.separated.total );
		this->eventQueue.init(
			this->config.application.eventQueue.block,
			this->config.application.eventQueue.size.separated.application,
			this->config.application.eventQueue.size.separated.master
		);
		ApplicationWorker::init();

		int index = 0;
#define WORKER_INIT_LOOP( _FIELD_, _CONSTANT_ ) \
		for ( int i = 0, len = this->config.application.workers.number.separated._FIELD_; i < len; i++, index++ ) { \
			this->workers.push_back( ApplicationWorker() ); \
			this->workers[ index ].init( \
				this->config.application, \
				_CONSTANT_ \
			); \
		}

		WORKER_INIT_LOOP( application, WORKER_ROLE_APPLICATION )
		WORKER_INIT_LOOP( master, WORKER_ROLE_MASTER )
#undef WORKER_INIT_LOOP
	}

	// Set signal handlers //
	Signal::setHandler( Application::signalHandler );

	// Show configuration //
	if ( verbose )
		this->info();
	return true;
}

bool Application::start() {
	/* Workers and event queues */
	this->eventQueue.start();
	if ( this->config.application.workers.type == WORKER_TYPE_MIXED ) {
		for ( int i = 0, len = this->config.application.workers.number.mixed; i < len; i++ ) {
			this->workers[ i ].start();
		}
	} else {
		for ( int i = 0, len = this->config.application.workers.number.separated.total; i < len; i++ ) {
			this->workers[ i ].start();
		}
	}

	/* Start epoll thread */
	if ( pthread_create( &this->tid, NULL, Application::run, ( void * ) this ) != 0 ) {
		__ERROR__( "Application", "start", "Cannot start epoll thread." );
		return false;
	}

	/* Socket */
	// Connect to masters
	for ( int i = 0, len = this->config.application.masters.size(); i < len; i++ ) {
		if ( ! this->sockets.masters[ i ].start() )
			return false;
	}

	this->startTime = start_timer();
	this->isRunning = true;

	return true;
}

bool Application::stop() {
	if ( ! this->isRunning )
		return false;

	int i, len;

	/* Workers */
	len = this->workers.size();
	for ( i = len - 1; i >= 0; i-- )
		this->workers[ i ].stop();

	/* Event queues */
	this->eventQueue.stop();

	/* epoll thread */
	this->sockets.epoll.stop( this->tid );
	pthread_join( this->tid, 0 );

	/* Workers */
	for ( i = len - 1; i >= 0; i-- )
		this->workers[ i ].join();

	/* Sockets */
	for ( i = 0, len = this->sockets.masters.size(); i < len; i++ )
		this->sockets.masters[ i ].stop();

	this->free();
	this->isRunning = false;
	printf( "\nBye.\n" );
	return true;
}

double Application::getElapsedTime() {
	return get_elapsed_time( this->startTime );
}

void Application::info( FILE *f ) {
	this->config.application.print( f );
}

void Application::debug( FILE *f ) {
	int i, len;

	fprintf( f, "\nMaster sockets\n-------------\n" );
	for ( i = 0, len = this->sockets.masters.size(); i < len; i++ ) {
		fprintf( f, "%d. ", i + 1 );
		this->sockets.masters[ i ].print( f );
	}
	if ( len == 0 ) fprintf( f, "(None)\n" );

	fprintf( f, "\nApplication event queue\n------------------\n" );
	this->eventQueue.print( f );

	fprintf( f, "\nWorkers\n-------\n" );
	for ( i = 0, len = this->workers.size(); i < len; i++ ) {
		fprintf( f, "%d. ", i + 1 );
		this->workers[ i ].print( f );
	}

	fprintf( f, "\n" );
}

void Application::interactive() {
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
		} else if ( strcmp( command, "pending" ) == 0 ) {
			valid = true;
			this->printPending();
		} else {
			// Get or set
			enum {
				ACTION_UNDEFINDED,
				ACTION_SET,
				ACTION_GET,
				ACTION_UPDATE,
				ACTION_DELETE
			} action = ACTION_UNDEFINDED;
			char *token = 0, *key = 0, *path = 0;
			uint32_t offset = 0;
			int count = 0, expectedTokens = 0;
			bool ret;

			while ( 1 ) {
				token = strtok( ( token == NULL ? command : NULL ), " " );

				if ( token ) {
					if ( strlen( token ) == 0 ) {
						continue;
					}
				} else {
					break;
				}

				if ( count == 0 ) {
					if ( strcmp( token, "get" ) == 0 ) {
						action = ACTION_GET;
						expectedTokens = 3;
					} else if ( strcmp( token, "set" ) == 0 ) {
						action = ACTION_SET;
						expectedTokens = 3;
					} else if ( strcmp( token, "update" ) == 0 ) {
						action = ACTION_UPDATE;
						expectedTokens = 4;
					} else if ( strcmp( token, "delete" ) == 0 ) {
						action = ACTION_DELETE;
						expectedTokens = 2;
					} else {
						break;
					}
				} else if ( count == 1 ) {
					key = token;
				} else if ( count == 2 ) {
					path = token;
				} else if ( count == 3 ) {
					offset = atoi( token );
				} else {
					break;
				}

				count++;
			}

			if ( count != expectedTokens ) {
				valid = false;
			} else {
				valid = true;
				switch( action ) {
					case ACTION_SET:
						printf( "[SET] {%s} %s\n", key, path );
						ret = this->set( key, path );
						break;
					case ACTION_GET:
						printf( "[GET] {%s} %s\n", key, path );
						ret = this->get( key, path );
						break;
					case ACTION_UPDATE:
						printf( "[UPDATE] {%s} %s at offset %u\n", key, path, offset );
						ret = this->update( key, path, offset );
						break;
					case ACTION_DELETE:
						printf( "[DELETE] {%s}\n", key );
						ret = this->del( key );
						break;
					default:
						ret = -1;
				}

				if ( ! ret ) {
					fprintf( stderr, "\nPlease check your input and try again.\n" );
				}
			}
		}

		if ( ! valid ) {
			fprintf( stderr, "Invalid command!\n" );
		}
	}
}

void Application::printPending( FILE *f ) {
	size_t i;
	std::set<Key>::iterator it;
	std::set<KeyValueUpdate>::iterator keyValueUpdateIt;
	fprintf(
		f,
		"Pending requests for application\n"
		"--------------------------------\n"
		"[SET] Pending: %lu\n",
		this->pending.application.set.size()
	);

	i = 1;
	for (
		it = this->pending.application.set.begin();
		it != this->pending.application.set.end();
		it++, i++
	) {
		const Key &key = *it;
		fprintf( f, "%lu. Key: %.*s (size = %u)\n", i, key.size, key.data, key.size );
	}

	fprintf(
		f,
		"\n[GET] Pending: %lu\n",
		this->pending.application.get.size()
	);
	i = 1;
	for (
		it = this->pending.application.get.begin();
		it != this->pending.application.get.end();
		it++, i++
	) {
		const Key &key = *it;
		fprintf( f, "%lu. Key: %.*s (size = %u)\n", i, key.size, key.data, key.size );
	}

	fprintf(
		f,
		"\n[UPDATE] Pending: %lu\n",
		this->pending.application.update.size()
	);
	i = 1;
	for (
		keyValueUpdateIt = this->pending.application.update.begin();
		keyValueUpdateIt != this->pending.application.update.end();
		keyValueUpdateIt++, i++
	) {
		const KeyValueUpdate &keyValueUpdate = *keyValueUpdateIt;
		fprintf(
			f, "%lu. Key: %.*s (size = %u, offset = %u, length = %u)\n",
			i, keyValueUpdate.size, keyValueUpdate.data, keyValueUpdate.size,
			keyValueUpdate.offset, keyValueUpdate.length
		);
	}

	fprintf(
		f,
		"\n[DELETE] Pending: %lu\n",
		this->pending.application.del.size()
	);
	i = 1;
	for (
		it = this->pending.application.del.begin();
		it != this->pending.application.del.end();
		it++, i++
	) {
		const Key &key = *it;
		fprintf( f, "%lu. Key: %.*s (size = %u)\n", i, key.size, key.data, key.size );
	}


	fprintf(
		f,
		"\n\nPending requests for master\n"
		"---------------------------\n"
		"[SET] Pending: %lu\n",
		this->pending.masters.set.size()
	);
	i = 1;
	for (
		it = this->pending.masters.set.begin();
		it != this->pending.masters.set.end();
		it++, i++
	) {
		const Key &key = *it;
		fprintf( f, "%lu. Key: %.*s (size = %u); target: ", i, key.size, key.data, key.size );
		( ( Socket * ) key.ptr )->printAddress( f );
		fprintf( f, "\n" );
	}

	fprintf(
		f,
		"\n[GET] Pending: %lu\n",
		this->pending.masters.get.size()
	);
	i = 1;
	for (
		it = this->pending.masters.get.begin();
		it != this->pending.masters.get.end();
		it++, i++
	) {
		const Key &key = *it;
		fprintf( f, "%lu. Key: %.*s (size = %u); target: ", i, key.size, key.data, key.size );
		( ( Socket * ) key.ptr )->printAddress( f );
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
			f, "%lu. Key: %.*s (size = %u, offset = %u, length = %u); target: ",
			i, keyValueUpdate.size, keyValueUpdate.data, keyValueUpdate.size,
			keyValueUpdate.offset, keyValueUpdate.length
		);
		( ( Socket * ) keyValueUpdate.ptr )->printAddress( f );
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
		fprintf( f, "%lu. Key: %.*s (size = %u); target: ", i, key.size, key.data, key.size );
		( ( Socket * ) key.ptr )->printAddress( f );
		fprintf( f, "\n" );
	}
}

bool Application::set( char *key, char *path ) {
	int fd;
	MasterEvent event;

	fd = ::open( path, O_RDONLY );
	if ( fd == -1 ) {
		__ERROR__( "Application", "set", "%s", strerror( errno ) );
		return false;
	}

	key = strdup( key );
	if ( ! key ) {
		__ERROR__( "Application", "set", "Cannot allocate memory." );
		return false;
	}

	event.reqSet( &this->sockets.masters[ 0 ], key, strlen( key ), fd );
	this->eventQueue.insert( event );

	return true;
}

bool Application::get( char *key, char *path ) {
	int fd;
	MasterEvent event;

	fd = ::open( path, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR );
	if ( fd == -1 ) {
		__ERROR__( "Application", "get", "%s", strerror( errno ) );
		return false;
	}

	key = strdup( key );
	if ( ! key ) {
		__ERROR__( "Application", "get", "Cannot allocate memory." );
		return false;
	}

	event.reqGet( &this->sockets.masters[ 0 ], key, strlen( key ), fd );
	this->eventQueue.insert( event );

	return true;
}

bool Application::update( char *key, char *path, uint32_t offset ) {
	int fd;
	MasterEvent event;

	fd = ::open( path, O_RDONLY );
	if ( fd == -1 ) {
		__ERROR__( "Application", "update", "%s", strerror( errno ) );
		return false;
	}

	key = strdup( key );
	if ( ! key ) {
		__ERROR__( "Application", "update", "Cannot allocate memory." );
		return false;
	}

	event.reqUpdate( &this->sockets.masters[ 0 ], key, strlen( key ), fd, offset );
	this->eventQueue.insert( event );

	return true;
}

bool Application::del( char *key ) {
	MasterEvent event;

	key = strdup( key );
	if ( ! key ) {
		__ERROR__( "Application", "get", "Cannot allocate memory." );
		return false;
	}

	event.reqDelete( &this->sockets.masters[ 0 ], key, strlen( key ) );
	this->eventQueue.insert( event );

	return true;
}

void Application::help() {
	fprintf(
		stdout,
		"Supported commands:\n"
		"- help: Show this help message\n"
		"- info: Show configuration\n"
		"- debug: Show debug messages\n"
		"- pending: Show all pending requests\n"
		"- time: Show elapsed time\n"
		"- exit: Terminate this client\n"
		"- set [key] [src]: Upload the file at [src] with key [key]\n"
		"- get [key] [dest]: Download the file with key [key] to the "
		"destination [dest]\n"
		"- update [key] [src] [offset]: Update the data at [offset] with "
		"the contents in [src]\n"
		"- delete [key]: Delete the key [key]\n\n"
	);
	fflush( stdout );
}

void Application::time() {
	fprintf( stdout, "Elapsed time: %12.6lf s\n", this->getElapsedTime() );
	fflush( stdout );
}
