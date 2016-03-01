#include <cstdlib>
#include <sys/stat.h>
#include "server_config.hh"

ServerConfig::ServerConfig() {
	this->seal.disabled = false;
}

bool ServerConfig::merge( GlobalConfig &globalConfig ) {
	this->epoll.maxEvents = globalConfig.epoll.maxEvents;
	this->epoll.timeout = globalConfig.epoll.timeout;
	return true;
}

bool ServerConfig::parse( const char *path ) {
	if ( Config::parse( path, "server.ini" ) ) {
		if ( this->workers.type == WORKER_TYPE_SEPARATED )
			this->workers.number.separated.total =
				this->workers.number.separated.coding +
				this->workers.number.separated.coordinator +
				this->workers.number.separated.io +
				this->workers.number.separated.master +
				this->workers.number.separated.slave +
				this->workers.number.separated.slavePeer;
		return true;
	}
	return false;
}

bool ServerConfig::override( OptionList &options ) {
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

bool ServerConfig::set( const char *section, const char *name, const char *value ) {
	if ( match( section, "slave" ) ) {
		return this->slave.addr.parse( name, value );
	} else if ( match( section, "slave_peers" ) ) {
		if ( match( name, "timeout" ) )
			this->slavePeers.timeout = atoi( value );
		else
			return false;
	} else if ( match( section, "epoll" ) ) {
		if ( match( name, "max_events" ) )
			this->epoll.maxEvents = atoi( value );
		else if ( match( name, "timeout" ) )
			this->epoll.timeout = atoi( value );
		else
			return false;
	} else if ( match( section, "pool" ) ) {
		if ( match( name, "chunks" ) )
			this->pool.chunks = atoll( value );
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
		else if ( match( name, "coding" ) )
			this->workers.number.separated.coding = atoi( value );
		else if ( match( name, "coordinator" ) )
			this->workers.number.separated.coordinator = atoi( value );
		else if ( match( name, "io" ) )
			this->workers.number.separated.io = atoi( value );
		else if ( match( name, "master" ) )
			this->workers.number.separated.master = atoi( value );
		else if ( match( name, "slave" ) )
			this->workers.number.separated.slave = atoi( value );
		else if ( match( name, "slave_peer" ) )
			this->workers.number.separated.slavePeer = atoi( value );
		else
			return false;
	} else if ( match( section, "event_queue" ) ) {
		if ( match( name, "block" ) )
			this->eventQueue.block = ! match( value, "false" );
		else if ( match( name, "mixed" ) )
			this->eventQueue.size.mixed = atoi( value );
		else if ( match( name, "prioritized_mixed" ) )
			this->eventQueue.size.pMixed = atoi( value );
		else if ( match( name, "coding" ) )
			this->eventQueue.size.separated.coding = atoi( value );
		else if ( match( name, "coordinator" ) )
			this->eventQueue.size.separated.coordinator = atoi( value );
		else if ( match( name, "io" ) )
			this->eventQueue.size.separated.io = atoi( value );
		else if ( match( name, "master" ) )
			this->eventQueue.size.separated.master = atoi( value );
		else if ( match( name, "slave" ) )
			this->eventQueue.size.separated.slave = atoi( value );
		else if ( match( name, "slave_peer" ) )
			this->eventQueue.size.separated.slavePeer = atoi( value );
		else
			return false;
	} else if ( match( section, "seal" ) ) {
		if ( match( name, "disabled" ) )
			this->seal.disabled = match( value, "true" );
		else
			return false;
	} else if ( match( section, "storage" ) ) {
		if ( match( name, "type" ) ) {
			if ( match( value, "local" ) )
				this->storage.type = STORAGE_TYPE_LOCAL;
			else
				this->storage.type = STORAGE_TYPE_UNDEFINED;
		} else if ( match( name, "path" ) ) {
			if ( strlen( value ) >= STORAGE_PATH_MAX )
				return false;
			strncpy( this->storage.path, value, STORAGE_PATH_MAX );
		} else {
			return false;
		}
	} else {
		return false;
	}
	return true;
}

bool ServerConfig::validate() {
	if ( ! this->slave.addr.isInitialized() )
		CFG_PARSE_ERROR( "ServerConfig", "The slave is not assigned with an valid address." );

	if ( this->epoll.maxEvents < 1 )
		CFG_PARSE_ERROR( "ServerConfig", "Maximum number of events in epoll should be at least 1." );

	if ( this->epoll.timeout < -1 )
		CFG_PARSE_ERROR( "ServerConfig", "The timeout value of epoll should be either -1 (infinite blocking), 0 (non-blocking) or a positive value (representing the number of milliseconds to block)." );

	if ( this->pool.chunks < 1 )
		CFG_PARSE_ERROR( "ServerConfig", "The size of chunk pool should be greater than 0." );

	switch( this->workers.type ) {
		case WORKER_TYPE_MIXED:
			if ( this->workers.number.mixed < 1 )
				CFG_PARSE_ERROR( "ServerConfig", "The number of workers should be at least 1." );
			if ( this->eventQueue.size.mixed < this->workers.number.mixed )
				CFG_PARSE_ERROR( "ServerConfig", "The size of the event queue should be at least the number of workers." );
			if ( this->eventQueue.size.pMixed < this->workers.number.mixed )
				CFG_PARSE_ERROR( "ServerConfig", "The size of the prioritized event queue should be at least the number of workers." );
			break;
		case WORKER_TYPE_SEPARATED:
			if ( this->workers.number.separated.coding < 1 )
				CFG_PARSE_ERROR( "ServerConfig", "The number of coding workers should be at least 1." );
			if ( this->workers.number.separated.coordinator < 1 )
				CFG_PARSE_ERROR( "ServerConfig", "The number of coordinator workers should be at least 1." );
			if ( this->workers.number.separated.io < 1 )
				CFG_PARSE_ERROR( "ServerConfig", "The number of I/O workers should be at least 1." );
			if ( this->workers.number.separated.master < 1 )
				CFG_PARSE_ERROR( "ServerConfig", "The number of master workers should be at least 1." );
			if ( this->workers.number.separated.slave < 1 )
				CFG_PARSE_ERROR( "ServerConfig", "The number of slave workers should be at least 1." );
			if ( this->workers.number.separated.slavePeer < 1 )
				CFG_PARSE_ERROR( "ServerConfig", "The number of slave peer workers should be at least 1." );

			if ( this->eventQueue.size.separated.coding < this->workers.number.separated.coding )
				CFG_PARSE_ERROR( "ServerConfig", "The size of the coding event queue should be at least the number of workers." );
			if ( this->eventQueue.size.separated.coordinator < this->workers.number.separated.coordinator )
				CFG_PARSE_ERROR( "ServerConfig", "The size of the coordinator event queue should be at least the number of workers." );
			if ( this->eventQueue.size.separated.io < this->workers.number.separated.io )
				CFG_PARSE_ERROR( "ServerConfig", "The size of the I/O event queue should be at least the number of workers." );
			if ( this->eventQueue.size.separated.master < this->workers.number.separated.master )
				CFG_PARSE_ERROR( "ServerConfig", "The size of the master event queue should be at least the number of workers." );
			if ( this->eventQueue.size.separated.slave < this->workers.number.separated.slave )
				CFG_PARSE_ERROR( "ServerConfig", "The size of the slave event queue should be at least the number of workers." );
			if ( this->eventQueue.size.separated.slavePeer < this->workers.number.separated.slavePeer )
				CFG_PARSE_ERROR( "ServerConfig", "The size of the slave peer event queue should be at least the number of workers." );
			break;
		default:
			CFG_PARSE_ERROR( "ServerConfig", "The type of event queue should be either \"mixed\" or \"separated\"." );
	}

	if ( this->storage.type == STORAGE_TYPE_UNDEFINED )
		CFG_PARSE_ERROR( "ServerConfig", "The specified storage type is invalid." );

	struct stat st;
	if ( stat( this->storage.path, &st ) != 0 )
		CFG_PARSE_ERROR( "ServerConfig", "The specified storage path does not exist." );

	if ( ! S_ISDIR( st.st_mode ) )
		CFG_PARSE_ERROR( "ServerConfig", "The specified storage path is not a directory." );

	return true;
}

int ServerConfig::validate( std::vector<ServerAddr> slaves ) {
	if ( this->validate() ) {
		for ( int i = 0, len = slaves.size(); i < len; i++ ) {
			if ( this->slave.addr == slaves[ i ] )
				return i;
		}
		__ERROR__( "ServerConfig", "validate", "The assigned address does not match with the global slave list." );
	}
	return -1;
}

void ServerConfig::print( FILE *f ) {
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
		"- epoll settings\n"
		"\t- %-*s : %u\n"
		"\t- %-*s : %d\n"
		"- Slave peer connections settings\n"
		"\t- %-*s : %u\n"
		"- Pool\n"
		"\t- %-*s : %lu\n"
		"- Workers\n"
		"\t- %-*s : %s\n",
		width, "Maximum number of events", this->epoll.maxEvents,
		width, "Timeout", this->epoll.timeout,
		width, "Timeout", this->slavePeers.timeout,
		width, "Chunks", this->pool.chunks,
		width, "Type", this->workers.type == WORKER_TYPE_MIXED ? "Mixed" : "Separated"
	);

	if ( this->workers.type == WORKER_TYPE_MIXED ) {
		fprintf(
			f,
			"\t- %-*s : %u\n"
			"- Event queues\n"
			"\t- %-*s : %s\n"
			"\t- %-*s : %u; %u (prioritized)\n"
			"- Storage\n"
			"\t- %-*s : %s\n"
			"\t- %-*s : %s\n",
			width, "Number of workers", this->workers.number.mixed,
			width, "Blocking?", this->eventQueue.block ? "Yes" : "No",
			width, "Size", this->eventQueue.size.mixed, this->eventQueue.size.pMixed,
			width, "Type", this->storage.type == STORAGE_TYPE_LOCAL ? "Local" : "Undefined",
			width, "Path", this->storage.path
		);
	} else {
		fprintf(
			f,
			"\t- %-*s : %u\n"
			"\t- %-*s : %u\n"
			"\t- %-*s : %u\n"
			"\t- %-*s : %u\n"
			"\t- %-*s : %u\n"
			"\t- %-*s : %u\n"
			"- Event queues\n"
			"\t- %-*s : %s\n"
			"\t- %-*s : %u\n"
			"\t- %-*s : %u\n"
			"\t- %-*s : %u\n"
			"\t- %-*s : %u\n"
			"\t- %-*s : %u\n"
			"\t- %-*s : %u\n"
			"- Storage\n"
			"\t- %-*s : %s\n"
			"\t- %-*s : %s\n",
			width, "Number of coding workers", this->workers.number.separated.coding,
			width, "Number of coordinator workers", this->workers.number.separated.coordinator,
			width, "Number of io workers", this->workers.number.separated.io,
			width, "Number of master workers", this->workers.number.separated.master,
			width, "Number of slave workers", this->workers.number.separated.slave,
			width, "Number of slave peer workers", this->workers.number.separated.slavePeer,
			width, "Blocking?", this->eventQueue.block ? "Yes" : "No",
			width, "Size for coding", this->eventQueue.size.separated.coding,
			width, "Size for coordinator", this->eventQueue.size.separated.coordinator,
			width, "Size for I/O", this->eventQueue.size.separated.io,
			width, "Size for master", this->eventQueue.size.separated.master,
			width, "Size for slave", this->eventQueue.size.separated.slave,
			width, "Size for slave peer", this->eventQueue.size.separated.slavePeer,
			width, "Type", this->storage.type == STORAGE_TYPE_LOCAL ? "Local" : "Undefined",
			width, "Path", this->storage.path
		);
	}

	fprintf( f, "\n" );
}
