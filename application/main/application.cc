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

bool Application::init( char *path, bool verbose ) {
	bool ret;
	// Parse configuration files //
	if ( ! ( ret = this->config.application.parse( path ) ) ) {
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
		this->workers.reserve( this->config.application.workers.number.mixed );
		for ( int i = 0, len = this->config.application.workers.number.mixed; i < len; i++ ) {
			this->workers.push_back( ApplicationWorker() );
			this->workers[ i ].init(
				this->config.application,
				WORKER_ROLE_MIXED,
				&this->eventQueue
			);
		}
	} else {
		this->workers.reserve( this->config.application.workers.number.separated.total );
		this->eventQueue.init(
			this->config.application.eventQueue.block,
			this->config.application.eventQueue.size.separated.application,
			this->config.application.eventQueue.size.separated.master
		);

		int index = 0;
#define WORKER_INIT_LOOP( _FIELD_, _CONSTANT_ ) \
		for ( int i = 0, len = this->config.application.workers.number.separated._FIELD_; i < len; i++, index++ ) { \
			this->workers.push_back( ApplicationWorker() ); \
			this->workers[ index ].init( \
				this->config.application, \
				_CONSTANT_, \
				&this->eventQueue \
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
		} else {
			// Get or set
			enum {
				ACTION_UNDEFINDED,
				ACTION_SET,
				ACTION_GET
			} action = ACTION_UNDEFINDED;
			char *token = 0, *key = 0, *path = 0;
			int count = 0;
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
					if ( strcmp( token, "get" ) == 0 )
						action = ACTION_GET;
					else if ( strcmp( token, "set" ) == 0 )
						action = ACTION_SET;
					else
						break;
				} else if ( count == 1 ) {
					key = token;
				} else if ( count == 2 ) {
					path = token;
				} else {
					break;
				}

				count++;
			}

			if ( count < 3 ) {
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

bool Application::set( char *key, char *path ) {
	ssize_t size;
	MasterEvent event;
	char *value;

	FILE *f = fopen( path, "rb" );
	if ( ! f ) {
		__ERROR__( "Application", "set", "Cannot read file: \"%s\" for key \"%s\".", path, key );
		return false;
	}

	// Prepare read buffer
	fseek( f, 0L, SEEK_END );
	size = ftell( f );
	fseek( f, 0L, SEEK_SET );

	if ( size == -1 ) {
		__ERROR__( "Application", "set", "Cannot get file size." );
		return false;
	}

	value = ( char * ) ::malloc( size );
	if ( ! value ) {
		__ERROR__( "Application", "set", "Cannot allocate memory." );
		return false;
	}
	key = strdup( key );
	if ( ! key ) {
		__ERROR__( "Application", "set", "Cannot allocate memory." );
		return false;
	}

	if ( fread( value, 1, size, f ) != ( size_t ) size ) {
		__ERROR__( "Application", "set", "Cannot read the file contents." );
		::free( value );
		return false;
	}

	fclose( f );

	// Put the data into event queue
	event.reqSet( &this->sockets.masters[ 0 ], key, value, ( size_t ) size );
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

	event.reqGet( &this->sockets.masters[ 0 ], key, fd );
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
		"- time: Show elapsed time\n"
		"- exit: Terminate this client\n"
		"- set [key] [src]: Upload the file at [src] with key [key]\n"
		"- get [key] [dest]: Download the file with key [key] to the "
		"destination [dest]\n\n"
	);
	fflush( stdout );
}

void Application::time() {
	fprintf( stdout, "Elapsed time: %12.6lf s\n", this->getElapsedTime() );
	fflush( stdout );
}
