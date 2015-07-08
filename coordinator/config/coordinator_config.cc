#include <cstdlib>
#include "coordinator_config.hh"

bool CoordinatorConfig::merge( GlobalConfig &globalConfig ) {
	this->epoll.maxEvents = globalConfig.epoll.maxEvents;
	this->epoll.timeout = globalConfig.epoll.timeout;
	return true;
}

bool CoordinatorConfig::parse( const char *path ) {
	return Config::parse( path, "coordinator.ini" );
}

bool CoordinatorConfig::set( const char *section, const char *name, const char *value ) {
	if ( match( section, "coordinator" ) ) {
		if ( match( name, "workers" ) )
			this->coordinator.workers = atoi( value );
		else
			return this->coordinator.addr.parse( name, value );
	} else if ( match( section, "epoll" ) ) {
		if ( match( name, "max_events" ) )
			this->epoll.maxEvents = atoi( value );
		else if ( match( name, "timeout" ) )
			this->epoll.timeout = atoi( value );
		else
			return false;
	} else if ( match( section, "event_queue" ) ) {
		if ( match( name, "type" ) ) {
			if ( match( value, "mixed" ) )
				this->eventQueue.type = EVENT_QUEUE_TYPE_MIXED;
			else if ( match( value, "separated" ) )
				this->eventQueue.type = EVENT_QUEUE_TYPE_SEPARATED;
			else
				this->eventQueue.type = EVENT_QUEUE_TYPE_UNDEFINED;
		} else if ( match( name, "block" ) )
			this->eventQueue.block = ! match( value, "false" );
		else if ( match( name, "mixed" ) )
			this->eventQueue.size.mixed = atoi( value );
		else if ( match( name, "application" ) )
			this->eventQueue.size.application = atoi( value );
		else if ( match( name, "coordinator" ) )
			this->eventQueue.size.coordinator = atoi( value );
		else if ( match( name, "master" ) )
			this->eventQueue.size.master = atoi( value );
		else if ( match( name, "slave" ) )
			this->eventQueue.size.slave = atoi( value );
		else
			return false;
	} else {
		return false;
	}
	return true;
}

bool CoordinatorConfig::validate() {
	if ( ! this->coordinator.addr.isInitialized() )
		CFG_PARSE_ERROR( "CoordinatorConfig", "The coordinator is not assigned with an valid address." );

	if ( this->coordinator.workers < 1 )
		CFG_PARSE_ERROR( "CoordinatorConfig", "The number of workers should be at least 1." );

	if ( this->epoll.maxEvents < 1 )
		CFG_PARSE_ERROR( "CoordinatorConfig", "Maximum number of events in epoll should be at least 1." );

	if ( this->epoll.timeout < -1 )
		CFG_PARSE_ERROR( "CoordinatorConfig", "The timeout value of epoll should be either -1 (infinite blocking), 0 (non-blocking) or a positive value (representing the number of milliseconds to block)." );

	switch( this->eventQueue.type ) {
		case EVENT_QUEUE_TYPE_MIXED:
			if ( this->eventQueue.size.mixed < this->coordinator.workers )
				CFG_PARSE_ERROR( "CoordinatorConfig", "The size of the event queue should be at least the number of workers." );
			break;
		case EVENT_QUEUE_TYPE_SEPARATED:
			if ( this->eventQueue.size.application < this->coordinator.workers )
				CFG_PARSE_ERROR( "CoordinatorConfig", "The size of the application event queue should be at least the number of workers." );
			if ( this->eventQueue.size.coordinator < this->coordinator.workers )
				CFG_PARSE_ERROR( "CoordinatorConfig", "The size of the coordinator event queue should be at least the number of workers." );
			if ( this->eventQueue.size.master < this->coordinator.workers )
				CFG_PARSE_ERROR( "CoordinatorConfig", "The size of the master event queue should be at least the number of workers." );
			if ( this->eventQueue.size.slave < this->coordinator.workers )
				CFG_PARSE_ERROR( "CoordinatorConfig", "The size of the slave event queue should be at least the number of workers." );
			break;
		default:
			CFG_PARSE_ERROR( "CoordinatorConfig", "The type of event queue should be either \"mixed\" or \"separated\"." );
	}

	return true;
}

bool CoordinatorConfig::validate( std::vector<ServerAddr> coordinators ) {
	if ( this->validate() ) {
		for ( int i = 0, len = coordinators.size(); i < len; i++ ) {
			if ( this->coordinator.addr == coordinators[ i ] )
				return true;
		}
		CFG_PARSE_ERROR( "CoordinatorConfig", "The assigned address does not match with the global coordinator list." );
	}
	return false;
}

void CoordinatorConfig::print( FILE *f ) {
	int width = 24;
	fprintf(
		f,
		"### Coordinator Configuration ###\n"
		"- Coordinator\n"
		"\t- %-*s : ",
		width, "Address"
	);
	this->coordinator.addr.print( f );
	fprintf(
		f,
		"\t- %-*s : %u\n"
		"- epoll settings\n"
		"\t- %-*s : %u\n"
		"\t- %-*s : %d\n"
		"- Event queue\n"
		"\t- %-*s : %s\n"
		"\t- %-*s : %s\n",
		width, "Number of workers", this->coordinator.workers,
		width, "Maximum number of events", this->epoll.maxEvents,
		width, "Timeout", this->epoll.timeout,
		width, "Type", this->eventQueue.type == EVENT_QUEUE_TYPE_MIXED ? "Mixed" : "Separated",
		width, "Blocking?", this->eventQueue.block ? "Yes" : "No"
	);
	if ( this->eventQueue.type == EVENT_QUEUE_TYPE_MIXED ) {
		fprintf( f, "\t- %-*s : %u\n", width, "Size", this->eventQueue.size.mixed );
	} else {
		fprintf(
			f,
			"\t- %-*s : %u\n"
			"\t- %-*s : %u\n"
			"\t- %-*s : %u\n"
			"\t- %-*s : %u\n",
			width, "Size for application", this->eventQueue.size.application,
			width, "Size for coordinator", this->eventQueue.size.coordinator,
			width, "Size for master", this->eventQueue.size.master,
			width, "Size for slave", this->eventQueue.size.slave
		);
	}

	fprintf( f, "\n" );
}
