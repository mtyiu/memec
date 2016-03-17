#include <cstdlib>
#include "application_config.hh"

ApplicationConfig::ApplicationConfig() {
	// Set default values
	this->size.key = 255;
	this->size.chunk = 4096;

	this->epoll.maxEvents = 64;
	this->epoll.timeout = -1;

	this->workers.count = 8;

	this->eventQueue.block = true;
	this->eventQueue.size = 1048576;
}

bool ApplicationConfig::parse( const char *path ) {
	return Config::parse( path, "application.ini" );
}

bool ApplicationConfig::set( const char *section, const char *name, const char *value ) {
	if ( match( section, "size" ) ) {
		if ( match( name, "key" ) )
			this->size.key = atoi( value );
		else if ( match( name, "chunk" ) )
			this->size.chunk = atoi( value );
		else
			return false;
	} else if ( match( section, "epoll" ) ) {
		if ( match( name, "max_events" ) )
			this->epoll.maxEvents = atoi( value );
		else if ( match( name, "timeout" ) )
			this->epoll.timeout = atoi( value );
		else
			return false;
	} else if ( match( section, "workers" ) ) {
		if ( match( name, "count" ) )
			this->workers.count = atoi( value );
		else
			return false;
	} else if ( match( section, "event_queue" ) ) {
		if ( match( name, "block" ) )
			this->eventQueue.block = ! match( value, "false" );
		else if ( match( name, "size" ) )
			this->eventQueue.size = atoi( value );
		else
			return false;
	} else if ( match( section, "clients" ) ) {
		ServerAddr addr;
		if ( addr.parse( name, value ) )
			this->clients.push_back( addr );
		else
			return false;
	} else {
		return false;
	}
	return true;
}

bool ApplicationConfig::validate() {
	if ( this->size.key < 8 )
		CFG_PARSE_ERROR( "ApplicationConfig", "Key size should be at least 8 bytes." );
	if ( this->size.key > 255 )
		CFG_PARSE_ERROR( "ApplicationConfig", "Key size should be at most 255 bytes." );
	if ( this->size.chunk < 32 )
		CFG_PARSE_ERROR( "ApplicationConfig", "Chunk size should be at least 32 bytes." );
	if ( this->size.chunk % 8 != 0 )
		CFG_PARSE_ERROR( "ApplicationConfig", "Chunk size should be a multiple of 8." );
	if ( this->size.chunk < this->size.key + 4 ) // 2^24 bytes
		CFG_PARSE_ERROR( "ApplicationConfig", "Chunk size should be at least %u bytes.", this->size.key + 4 );
	if ( this->size.chunk > 16777216 ) // 2^24 bytes
		CFG_PARSE_ERROR( "ApplicationConfig", "Key size should be at most 16777216 bytes." );

	if ( this->epoll.maxEvents < 1 )
		CFG_PARSE_ERROR( "ApplicationConfig", "Maximum number of events in epoll should be at least 1." );
	if ( this->epoll.timeout < -1 )
		CFG_PARSE_ERROR( "ApplicationConfig", "The timeout value of epoll should be either -1 (infinite blocking), 0 (non-blocking) or a positive value (representing the number of milliseconds to block)." );

	if ( this->workers.count < 1 )
		CFG_PARSE_ERROR( "ApplicationConfig", "The number of workers should be at least 1." );

	if ( this->eventQueue.size < this->workers.count )
		CFG_PARSE_ERROR( "ApplicationConfig", "The size of the event queue should be at least the number of workers." );

	if ( this->clients.empty() )
		CFG_PARSE_ERROR( "ApplicationConfig", "There should be at least one client." );

	return true;
}

void ApplicationConfig::print( FILE *f ) {
	int width = 29;
	fprintf(
		f,
		"### Application Configuration ###\n"
		"- Size\n"
		"\t- %-*s : %u\n"
		"\t- %-*s : %u\n"
		"- epoll settings\n"
		"\t- %-*s : %u\n"
		"\t- %-*s : %d\n"
		"- Workers\n"
		"\t- %-*s : %u\n"
		"- Event queues\n"
		"\t- %-*s : %s\n"
		"\t- %-*s : %u\n",
		width, "Maximum key size", this->size.key,
		width, "Chunk size", this->size.chunk,
		width, "Maximum number of events", this->epoll.maxEvents,
		width, "Timeout", this->epoll.timeout,
		width, "Count", this->workers.count,
		width, "Blocking?", this->eventQueue.block ? "Yes" : "No",
		width, "Size", this->eventQueue.size
	);

	fprintf( f, "- Clients\n" );
	for ( int i = 0, len = this->clients.size(); i < len; i++ ) {
		fprintf( f, "\t%d. ", ( i + 1 ) );
		this->clients[ i ].print( f );
	}

	fprintf( f, "\n" );
}
