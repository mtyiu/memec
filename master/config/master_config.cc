#include <cstdlib>
#include "master_config.hh"

MasterConfig::MasterConfig() {
	this->epoll.maxEvents = 64;
	this->epoll.timeout = -1;
	this->workers.type = WORKER_TYPE_MIXED;
	this->workers.number.mixed = 8;
	this->eventQueue.block = true;
	this->eventQueue.size.mixed = 1048576;
	this->eventQueue.size.pMixed = 1024;
	this->loadingStats.updateInterval = 30;
	this->remap.forceEnabled = false;
}

bool MasterConfig::merge( GlobalConfig &globalConfig ) {
	this->epoll.maxEvents = globalConfig.epoll.maxEvents;
	this->epoll.timeout = globalConfig.epoll.timeout;
	return true;
}

bool MasterConfig::parse( const char *path ) {
	if ( Config::parse( path, "master.ini" ) ) {
		if ( this->workers.type == WORKER_TYPE_SEPARATED )
			this->workers.number.separated.total =
				this->workers.number.separated.application +
				this->workers.number.separated.coordinator +
				this->workers.number.separated.master +
				this->workers.number.separated.slave;
		return true;
	}
	return false;
}

bool MasterConfig::override( OptionList &options ) {
	bool ret = true;
	for ( int i = 0, size = options.size(); i < size; i++ ) {
		ret &= this->set(
			options[ i ].section,
			options[ i ].name,
			options[ i ].value
		);
	}
	return ret;
}

bool MasterConfig::set( const char *section, const char *name, const char *value ) {
	if ( match( section, "master" ) ) {
		return this->master.addr.parse( name, value );
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
		else if ( match( name, "coordinator" ) )
			this->workers.number.separated.coordinator = atoi( value );
		else if ( match( name, "master" ) )
			this->workers.number.separated.master = atoi( value );
		else if ( match( name, "slave" ) )
			this->workers.number.separated.slave = atoi( value );
		else
			return false;
	} else if ( match( section, "event_queue" ) ) {
		if ( match( name, "block" ) )
			this->eventQueue.block = ! match( value, "false" );
		else if ( match( name, "mixed" ) )
			this->eventQueue.size.mixed = atoi( value );
		else if ( match( name, "prioritized_mixed" ) )
			this->eventQueue.size.pMixed = atoi( value );
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
	} else if ( match( section, "loadingStats" ) ) {
		if ( match ( name, "updateInterval" ) )
			this->loadingStats.updateInterval = atoi( value );
		else
			return false;
	} else if ( match( section, "remap" ) ) {
		if ( match ( name, "force_enabled" ) )
			this->remap.forceEnabled = ! match( value, "false" );
		else
			return false;
	} else {
		return false;
	}
	return true;
}

bool MasterConfig::validate() {
	if ( ! this->master.addr.isInitialized() )
		CFG_PARSE_ERROR( "MasterConfig", "The master is not assigned with an valid address." );

	if ( this->epoll.maxEvents < 1 )
		CFG_PARSE_ERROR( "MasterConfig", "Maximum number of events in epoll should be at least 1." );

	if ( this->epoll.timeout < -1 )
		CFG_PARSE_ERROR( "MasterConfig", "The timeout value of epoll should be either -1 (infinite blocking), 0 (non-blocking) or a positive value (representing the number of milliseconds to block)." );

	switch( this->workers.type ) {
		case WORKER_TYPE_MIXED:
			if ( this->workers.number.mixed < 1 )
				CFG_PARSE_ERROR( "MasterConfig", "The number of workers should be at least 1." );
			if ( this->eventQueue.size.mixed < this->workers.number.mixed )
				CFG_PARSE_ERROR( "MasterConfig", "The size of the event queue should be at least the number of workers." );
			if ( this->eventQueue.size.pMixed < this->workers.number.mixed )
				CFG_PARSE_ERROR( "MasterConfig", "The size of the prioritized event queue should be at least the number of workers." );
			break;
		case WORKER_TYPE_SEPARATED:
			if ( this->workers.number.separated.application < 1 )
				CFG_PARSE_ERROR( "MasterConfig", "The number of application workers should be at least 1." );
			if ( this->workers.number.separated.coordinator < 1 )
				CFG_PARSE_ERROR( "MasterConfig", "The number of coordinator workers should be at least 1." );
			if ( this->workers.number.separated.master < 1 )
				CFG_PARSE_ERROR( "MasterConfig", "The number of master workers should be at least 1." );
			if ( this->workers.number.separated.slave < 1 )
				CFG_PARSE_ERROR( "MasterConfig", "The number of slave workers should be at least 1." );

			if ( this->eventQueue.size.separated.application < this->workers.number.separated.application )
				CFG_PARSE_ERROR( "MasterConfig", "The size of the application event queue should be at least the number of workers." );
			if ( this->eventQueue.size.separated.coordinator < this->workers.number.separated.coordinator )
				CFG_PARSE_ERROR( "MasterConfig", "The size of the coordinator event queue should be at least the number of workers." );
			if ( this->eventQueue.size.separated.master < this->workers.number.separated.master )
				CFG_PARSE_ERROR( "MasterConfig", "The size of the master event queue should be at least the number of workers." );
			if ( this->eventQueue.size.separated.slave < this->workers.number.separated.slave )
				CFG_PARSE_ERROR( "MasterConfig", "The size of the slave event queue should be at least the number of workers." );
			break;
		default:
			CFG_PARSE_ERROR( "MasterConfig", "The type of event queue should be either \"mixed\" or \"separated\"." );
	}

	return true;
}

void MasterConfig::print( FILE *f ) {
	int width = 29;
	fprintf(
		f,
		"### Master Configuration ###\n"
		"- Master\n"
		"\t- %-*s : ",
		width, "Address"
	);
	this->master.addr.print( f );
	fprintf(
		f,
		"- epoll settings\n"
		"\t- %-*s : %u\n"
		"\t- %-*s : %d\n"
		"- Workers\n"
		"\t- %-*s : %s\n",
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
			"\t- %-*s : %u; %u (prioritized)\n",
			width, "Number of workers", this->workers.number.mixed,
			width, "Blocking?", this->eventQueue.block ? "Yes" : "No",
			width, "Size", this->eventQueue.size.mixed, this->eventQueue.size.pMixed
		);
	} else {
		fprintf(
			f,
			"\t- %-*s : %u\n"
			"\t- %-*s : %u\n"
			"\t- %-*s : %u\n"
			"\t- %-*s : %u\n"
			"- Event queues\n"
			"\t- %-*s : %s\n"
			"\t- %-*s : %u\n"
			"\t- %-*s : %u\n"
			"\t- %-*s : %u\n"
			"\t- %-*s : %u\n",
			width, "Number of application workers", this->workers.number.separated.application,
			width, "Number of coordinator workers", this->workers.number.separated.coordinator,
			width, "Number of master workers", this->workers.number.separated.master,
			width, "Number of slave workers", this->workers.number.separated.slave,
			width, "Blocking?", this->eventQueue.block ? "Yes" : "No",
			width, "Size for application", this->eventQueue.size.separated.application,
			width, "Size for coordinator", this->eventQueue.size.separated.coordinator,
			width, "Size for master", this->eventQueue.size.separated.master,
			width, "Size for slave", this->eventQueue.size.separated.slave
		);
	}
	fprintf(
		f,
		"Loading statistics\n"
		"\t- %-*s : %u\n",
		width, "Update interval (seconds)", this->loadingStats.updateInterval
	);

	fprintf( f, "\n" );
}
