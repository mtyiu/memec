#include <cstdlib>
#include "slave_config.hh"

bool SlaveConfig::merge( GlobalConfig &globalConfig ) {
	this->epoll.maxEvents = globalConfig.epoll.maxEvents;
	this->epoll.timeout = globalConfig.epoll.timeout;
	return true;
}

bool SlaveConfig::parse( const char *path ) {
	return Config::parse( path, "slave.ini" );
}

bool SlaveConfig::set( const char *section, const char *name, const char *value ) {
	if ( match( section, "slave" ) ) {
		if ( match( name, "workers" ) )
			this->slave.workers = atoi( value );
		else
			return this->slave.addr.parse( name, value );
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
			this->eventQueue.size.separated.application = atoi( value );
		else if ( match( name, "coordinator" ) )
			this->eventQueue.size.separated.coordinator = atoi( value );
		else if ( match( name, "master" ) )
			this->eventQueue.size.separated.master = atoi( value );
		else if ( match( name, "slave" ) )
			this->eventQueue.size.separated.slave = atoi( value );
		else
			return false;
	} else {
		return false;
	}
	return true;
}

bool SlaveConfig::validate() {
	if ( ! this->slave.addr.isInitialized() )
		CFG_PARSE_ERROR( "SlaveConfig", "The slave is not assigned with an valid address." );

	if ( this->slave.workers < 1 )
		CFG_PARSE_ERROR( "SlaveConfig", "The number of workers should be at least 1." );

	if ( this->epoll.maxEvents < 1 )
		CFG_PARSE_ERROR( "SlaveConfig", "Maximum number of events in epoll should be at least 1." );

	if ( this->epoll.timeout < -1 )
		CFG_PARSE_ERROR( "SlaveConfig", "The timeout value of epoll should be either -1 (infinite blocking), 0 (non-blocking) or a positive value (representing the number of milliseconds to block)." );

	switch( this->eventQueue.type ) {
		case EVENT_QUEUE_TYPE_MIXED:
			if ( this->eventQueue.size.mixed < this->slave.workers )
				CFG_PARSE_ERROR( "SlaveConfig", "The size of the event queue should be at least the number of workers." );
			break;
		case EVENT_QUEUE_TYPE_SEPARATED:
			if ( this->eventQueue.size.separated.application < this->slave.workers )
				CFG_PARSE_ERROR( "SlaveConfig", "The size of the application event queue should be at least the number of workers." );
			if ( this->eventQueue.size.separated.coordinator < this->slave.workers )
				CFG_PARSE_ERROR( "SlaveConfig", "The size of the coordinator event queue should be at least the number of workers." );
			if ( this->eventQueue.size.separated.master < this->slave.workers )
				CFG_PARSE_ERROR( "SlaveConfig", "The size of the master event queue should be at least the number of workers." );
			if ( this->eventQueue.size.separated.slave < this->slave.workers )
				CFG_PARSE_ERROR( "SlaveConfig", "The size of the slave event queue should be at least the number of workers." );
			break;
		default:
			CFG_PARSE_ERROR( "SlaveConfig", "The type of event queue should be either \"mixed\" or \"separated\"." );
	}

	return true;
}

bool SlaveConfig::validate( std::vector<ServerAddr> slaves ) {
	if ( this->validate() ) {
		for ( int i = 0, len = slaves.size(); i < len; i++ ) {
			if ( this->slave.addr == slaves[ i ] )
				return true;
		}
		CFG_PARSE_ERROR( "SlaveConfig", "The assigned address does not match with the global slave list." );
	}
	return false;
}

void SlaveConfig::print( FILE *f ) {
	int width = 24;
	fprintf(
		f,
		"### Slave Configuration ###\n"
		"- Slave\n"
		"\t- %-*s : ",
		width, "Address"
	);
	this->slave.addr.print( f );
	fprintf(
		f,
		"\t- %-*s : %u\n"
		"- epoll settings\n"
		"\t- %-*s : %u\n"
		"\t- %-*s : %d\n"
		"- Event queue\n"
		"\t- %-*s : %s\n"
		"\t- %-*s : %s\n",
		width, "Number of workers", this->slave.workers,
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
			width, "Size for application", this->eventQueue.size.separated.application,
			width, "Size for coordinator", this->eventQueue.size.separated.coordinator,
			width, "Size for master", this->eventQueue.size.separated.master,
			width, "Size for slave", this->eventQueue.size.separated.slave
		);
	}

	fprintf( f, "\n" );
}
