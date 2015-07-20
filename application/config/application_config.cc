#include <cstdlib>
#include "application_config.hh"

bool ApplicationConfig::parse( const char *path ) {
	if ( Config::parse( path, "application.ini" ) ) {
		if ( this->workers.type == WORKER_TYPE_SEPARATED )
			this->workers.number.separated.total = 
				this->workers.number.separated.application +
				this->workers.number.separated.master;
		return true;
	}
	return false;
}

bool ApplicationConfig::set( const char *section, const char *name, const char *value ) {
	if ( match( section, "size" ) ) {
		if ( match( name, "key_size" ) )
			this->size.key = atoi( value );
		else if ( match( name, "chunk_size" ) )
			this->size.chunk = atoi( value );
		else
			return false;
	} else if ( match( section, "master" ) ) {
		ServerAddr addr;
		if ( addr.parse( name, value ) )
			this->masters.push_back( addr );
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
		if ( match( name, "type" ) ) {
			if ( match( value, "mixed" ) )
				this->workers.type = WORKER_TYPE_MIXED;
			else if ( match( value, "separated" ) )
				this->workers.type = WORKER_TYPE_SEPARATED;
			else
				this->workers.type = WORKER_TYPE_UNDEFINED;
		} else if ( match( name, "mixed" ) )
			this->workers.number.mixed = atoi( value );
		else if ( match( name, "application" ) )
			this->workers.number.separated.application = atoi( value );
		else if ( match( name, "master" ) )
			this->workers.number.separated.master = atoi( value );
		else
			return false;
	} else if ( match( section, "event_queue" ) ) {
		if ( match( name, "block" ) )
			this->eventQueue.block = ! match( value, "false" );
		else if ( match( name, "mixed" ) )
			this->eventQueue.size.mixed = atoi( value );
		else if ( match( name, "application" ) )
			this->eventQueue.size.separated.application = atoi( value );
		else if ( match( name, "master" ) )
			this->eventQueue.size.separated.master = atoi( value );
		else
			return false;
	} else {
		return false;
	}
	return true;
}

bool ApplicationConfig::validate() {
	if ( this->size.key < 8 )
		CFG_PARSE_ERROR( "GlobalConfig", "Key size should be at least 8 bytes." );
	if ( this->size.key > 255 )
		CFG_PARSE_ERROR( "GlobalConfig", "Key size should be at most 255 bytes." );

	if ( this->epoll.maxEvents < 1 )
		CFG_PARSE_ERROR( "ApplicationConfig", "Maximum number of events in epoll should be at least 1." );

	if ( this->epoll.timeout < -1 )
		CFG_PARSE_ERROR( "ApplicationConfig", "The timeout value of epoll should be either -1 (infinite blocking), 0 (non-blocking) or a positive value (representing the number of milliseconds to block)." );

	switch( this->workers.type ) {
		case WORKER_TYPE_MIXED:
			if ( this->workers.number.mixed < 1 )
				CFG_PARSE_ERROR( "ApplicationConfig", "The number of workers should be at least 1." );
			if ( this->eventQueue.size.mixed < this->workers.number.mixed )
				CFG_PARSE_ERROR( "ApplicationConfig", "The size of the event queue should be at least the number of workers." );
			break;
		case WORKER_TYPE_SEPARATED:
			if ( this->workers.number.separated.application < 1 )
				CFG_PARSE_ERROR( "ApplicationConfig", "The number of application workers should be at least 1." );
			if ( this->workers.number.separated.master < 1 )
				CFG_PARSE_ERROR( "ApplicationConfig", "The number of Master workers should be at least 1." );

			if ( this->eventQueue.size.separated.application < this->workers.number.separated.application )
				CFG_PARSE_ERROR( "ApplicationConfig", "The size of the application event queue should be at least the number of workers." );
			if ( this->eventQueue.size.separated.master < this->workers.number.separated.master )
				CFG_PARSE_ERROR( "ApplicationConfig", "The size of the master event queue should be at least the number of workers." );
			break;
		default:
			CFG_PARSE_ERROR( "ApplicationConfig", "The type of event queue should be either \"mixed\" or \"separated\"." );
	}

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
		"\t- %-*s : %s\n",
		width, "Key size", this->size.key,
		width, "Chunk size", this->size.chunk,
		width, "Maximum number of events", this->epoll.maxEvents,
		width, "Timeout", this->epoll.timeout,
		width, "Type", this->workers.type == WORKER_TYPE_MIXED ? "Mixed" : "Separated"
	);

	if ( this->workers.type == WORKER_TYPE_MIXED ) {
		fprintf(
			f,
			"\t- %-*s : %u\n"
			"- Event queues\n"
			"\t- %-*s : %s\n"
			"\t- %-*s : %u\n",
			width, "Number of workers", this->workers.number.mixed,
			width, "Blocking?", this->eventQueue.block ? "Yes" : "No",
			width, "Size", this->eventQueue.size.mixed
		);
	} else {
		fprintf(
			f,
			"\t- %-*s : %u\n"
			"\t- %-*s : %u\n"
			"- Event queues\n"
			"\t- %-*s : %s\n"
			"\t- %-*s : %u\n"
			"\t- %-*s : %u\n",
			width, "Number of application workers", this->workers.number.separated.application,
			width, "Number of master workers", this->workers.number.separated.master,
			width, "Blocking?", this->eventQueue.block ? "Yes" : "No",
			width, "Size for application", this->eventQueue.size.separated.application,
			width, "Size for master", this->eventQueue.size.separated.master
		);
	}

	fprintf( f, "- Masters\n" );
	for ( int i = 0, len = this->masters.size(); i < len; i++ ) {
		fprintf( f, "\t%d. ", ( i + 1 ) );
		this->masters[ i ].print( f );
	}

	fprintf( f, "\n" );
}
