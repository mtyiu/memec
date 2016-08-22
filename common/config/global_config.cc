#include <cstdlib>
#include "global_config.hh"

GlobalConfig::GlobalConfig() {
	// Set default values
	this->size.key = 255;
	this->size.chunk = 4096;

	this->stripeLists.count = 16;

	this->epoll.maxEvents = 64;
	this->epoll.timeout = -1;

	this->workers.count = 8;

	this->eventQueue.block = true;
	this->eventQueue.size = 1048576;
	this->eventQueue.prioritized = 1024;

	this->pool.packets = 1024;

	this->timeout.metadata = 1000;
	this->timeout.load = 50;

	this->coding.scheme = CS_RAID0;
	this->coding.params.setN( 1 );

	this->states.disabled = true;
	this->states.workers = 4;
	this->states.queue = 256;
	this->states.smoothingFactor = 0.3;
}

bool GlobalConfig::parse( const char *path ) {
	return Config::parse( path, "global.ini" );
}

bool GlobalConfig::set( const char *section, const char *name, const char *value ) {
	if ( match( section, "size" ) ) {
		if ( match( name, "key" ) )
			this->size.key = atoi( value );
		else if ( match( name, "chunk" ) )
			this->size.chunk = atoi( value );
		else
			return false;
	} else if ( match( section, "stripe_lists" ) ) {
		if ( match( name, "count" ) )
			this->stripeLists.count = atoi( value );
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
		else if ( match( name, "prioritized" ) )
			this->eventQueue.prioritized = atoi( value );
		else
			return false;
	} else if ( match( section, "pool" ) ) {
		if ( match( name, "packets" ) )
			this->pool.packets = atoi( value );
		else
			return false;
	} else if ( match( section, "timeout" ) ) {
		if ( match( name, "metadata" ) )
			this->timeout.metadata = atoi( value );
		else if ( match( name, "load" ) )
			this->timeout.load = atoi( value );
		else
			return false;
	} else if ( match( section, "coordinators" ) ) {
		ServerAddr addr;
		if ( addr.parse( name, value ) )
			this->coordinators.push_back( addr );
		else
			return false;
	} else if ( match( section, "servers" ) ) {
		ServerAddr addr;
		if ( addr.parse( name, value ) )
			this->servers.push_back( addr );
		else
			return false;
	} else if ( match( section, "states" ) ) {
		if ( match( name, "disabled" ) ) {
			this->states.disabled = match( value, "true" );
		} else if ( match( name, "spreadd" ) ) {
			ServerAddr addr;
			if ( addr.parse( name, value ) )
				this->states.spreaddAddr = addr;
			else
				return false;
		} else if ( match( name, "workers" ) ) {
			this->states.workers = atoi( value );
		} else if ( match( name, "queue_len" ) ) {
			this->states.queue = atoi( value );
		} else if ( match( name, "smoothing_factor" ) ) {
			this->states.smoothingFactor = atof( value );
		} else {
			return false;
		}
	} else if ( match( section, "coding" ) ) {
		if ( match( value, "raid0" ) ) {
			this->coding.scheme = CS_RAID0;
		} else if ( match( value, "raid1" ) ) {
			this->coding.scheme = CS_RAID1;
		} else if ( match( value, "raid5" ) ) {
			this->coding.scheme = CS_RAID5;
		} else if ( match( value, "rs" ) ) {
			this->coding.scheme = CS_RS;
		} else if ( match( value, "rdp" ) ) {
			this->coding.scheme = CS_RDP;
		} else if ( match( value, "evenodd" ) ) {
			this->coding.scheme = CS_EVENODD;
		} else if ( match( value, "cauchy" ) ) {
			this->coding.scheme = CS_CAUCHY;
		} else {
			this->coding.scheme = CS_UNDEFINED;
			this->coding.params.setScheme( this->coding.scheme );
			return false;
		}
		this->coding.params.setScheme( this->coding.scheme );
	} else {
		if ( this->coding.scheme == CS_RAID0 && match( section, "raid0" ) ) {
			if ( match( name, "n" ) )
				this->coding.params.setN( atoi( value ) );
		} else if ( this->coding.scheme == CS_RAID1 && match( section, "raid1" ) ) {
			if ( match( name, "n" ) )
				this->coding.params.setN( atoi( value ) );
		} else if ( this->coding.scheme == CS_RAID5 && match( section, "raid5" ) ) {
			if ( match( name, "n" ) )
				this->coding.params.setN( atoi( value ) );
		} else if ( this->coding.scheme == CS_RS && match( section, "rs" ) ) {
			if ( match( name, "k" ) )
				this->coding.params.setK( atoi( value ) );
			else if ( match( name, "m" ) )
				this->coding.params.setM( atoi( value ) );
			else if ( match( name, "w" ) )
				this->coding.params.setW( atoi( value ) );
		} else if ( this->coding.scheme == CS_RDP && match( section, "rdp" ) ) {
			if ( match( name, "n" ) )
				this->coding.params.setN( atoi( value ) );
		} else if ( this->coding.scheme == CS_EVENODD && match( section, "evenodd" ) ) {
			if ( match( name, "n" ) )
				this->coding.params.setN( atoi( value ) );
		} else if ( this->coding.scheme == CS_CAUCHY && match( section, "cauchy" ) ) {
			if ( match( name, "c_k" ) )
				this->coding.params.setK( atoi( value ) );
			else if ( match( name, "c_m" ) )
				this->coding.params.setM( atoi( value ) );
			else if ( match( name, "c_w" ) )
				this->coding.params.setW( atoi( value ) );
		} else {
			return false;
		}
	}
	return true;
}

bool GlobalConfig::validate() {
	if ( this->size.key < 8 )
		CFG_PARSE_ERROR( "GlobalConfig", "Key size should be at least 8 bytes." );
	if ( this->size.key > 255 )
		CFG_PARSE_ERROR( "GlobalConfig", "Key size should be at most 255 bytes." );
	if ( this->size.chunk < 32 )
		CFG_PARSE_ERROR( "GlobalConfig", "Chunk size should be at least 32 bytes." );
	if ( this->size.chunk % 8 != 0 )
		CFG_PARSE_ERROR( "GlobalConfig", "Chunk size should be a multiple of 8." );
	if ( this->size.chunk < this->size.key + 4 ) // 2^24 bytes
		CFG_PARSE_ERROR( "GlobalConfig", "Chunk size should be at least %u bytes.", this->size.key + 4 );
	if ( this->size.chunk > 16777216 ) // 2^24 bytes
		CFG_PARSE_ERROR( "GlobalConfig", "Key size should be at most 16777216 bytes." );

	if ( this->stripeLists.count < 1 )
		CFG_PARSE_ERROR( "GlobalConfig", "The number of stripe lists should be at least 1." );

	if ( this->epoll.maxEvents < 1 )
		CFG_PARSE_ERROR( "GlobalConfig", "Maximum number of events in epoll should be at least 1." );
	if ( this->epoll.timeout < -1 )
		CFG_PARSE_ERROR( "GlobalConfig", "The timeout value of epoll should be either -1 (infinite blocking), 0 (non-blocking) or a positive value (representing the number of milliseconds to block)." );

	if ( this->workers.count < 1 )
		CFG_PARSE_ERROR( "GlobalConfig", "The number of workers should be at least 1." );

	if ( this->eventQueue.size < this->workers.count )
		CFG_PARSE_ERROR( "GlobalConfig", "The size of the event queue should be at least the number of workers." );
	if ( this->eventQueue.prioritized < this->workers.count )
		CFG_PARSE_ERROR( "GlobalConfig", "The size of the prioritized event queue should be at least the number of workers." );

	if ( this->pool.packets < 1 )
		CFG_PARSE_ERROR( "GlobalConfig", "The size of packet pool should be at least 1." );

	if ( this->timeout.metadata < 1 )
		CFG_PARSE_ERROR( "GlobalConfig", "The metadata synchronization timeout should be at least 1 ms." );
	if ( this->timeout.load < 1 )
		CFG_PARSE_ERROR( "GlobalConfig", "The load statistics synchronization timeout should be at least 1 ms." );

	if ( this->coordinators.empty() )
		CFG_PARSE_ERROR( "GlobalConfig", "There should be at least one coordinator." );

	if ( this->servers.empty() )
		CFG_PARSE_ERROR( "GlobalConfig", "There should be at least one server." );

	if ( ! this->states.disabled ) {
		if ( ! this->states.spreaddAddr.isInitialized() )
			CFG_PARSE_ERROR( "GlobalConfig", "The spread daemon address is not an valid address." );
		if ( this->states.workers < 1 )
			CFG_PARSE_ERROR( "GlobalConfig", "The number of state workers should be at least 1." );
		if ( this->states.queue < this->states.workers )
			CFG_PARSE_ERROR( "GlobalConfig", "The size of the state event queue should be at least the number of state workers." );
		if ( this->states.smoothingFactor > 1 || this->states.smoothingFactor <= 0 )
			CFG_PARSE_ERROR( "GlobalConfig", "The smoothing factor should be between 0 and 1." );
	}

	switch( this->coding.scheme ) {
		case CS_RAID0:
			if ( this->coding.params.getN() < 1 )
				CFG_PARSE_ERROR( "GlobalConfig", "RAID-0: Parameter `n' should be at least 1." );
			break;
		case CS_RAID1:
			if ( this->coding.params.getN() < 1 )
				CFG_PARSE_ERROR( "GlobalConfig", "RAID-1: Parameter `n' should be at least 1." );
			break;
		case CS_RAID5:
			if ( this->coding.params.getN() < 3 )
				CFG_PARSE_ERROR( "GlobalConfig", "RAID-5: Parameter `n' should be at least 3." );
			break;
		case CS_RS:
			{
				uint32_t k = this->coding.params.getK(),
				         m = this->coding.params.getM(),
				         w = this->coding.params.getW();
				if ( k < 1 )
					CFG_PARSE_ERROR( "GlobalConfig", "Reed-Solomon Code: Parameter `k' should be at least 1." );
				if ( m < 1 )
					CFG_PARSE_ERROR( "GlobalConfig", "Reed-Solomon Code: Parameter `m' should be at least 1." );
				if ( w != 8 && w != 16 && w != 32 )
					CFG_PARSE_ERROR( "GlobalConfig", "Reed-Solomon Code: Parameter `w' should be either 8, 16 or 32." );
				if ( w <= 16 && k + m > ( ( uint32_t ) 1 << w ) )
					CFG_PARSE_ERROR( "GlobalConfig", "Reed-Solomon Code: Parameters `k', `m', and `w' should satisfy k + m <= ( 1 << w ) when w <= 16." );
			}
			break;
		case CS_RDP:
			if ( this->coding.params.getN() < 2 )
				CFG_PARSE_ERROR( "GlobalConfig", "Row-Diagonal Parity Code: Parameter `n' should be at least 2." );
			break;
		case CS_EVENODD:
			if ( this->coding.params.getN() < 2 )
				CFG_PARSE_ERROR( "GlobalConfig", "EVENODD Code: Parameter `n' should be at least 2." );
			break;
		case CS_CAUCHY:
			{
				uint32_t k = this->coding.params.getK(),
				         m = this->coding.params.getM(),
				         w = this->coding.params.getW();
				if ( k < 1 )
					CFG_PARSE_ERROR( "GlobalConfig", "Cauchy-based Reed-Solomon Code: Parameter `c_k' should be at least 1." );
				if ( m < 1 )
					CFG_PARSE_ERROR( "GlobalConfig", "Cauchy-based Reed-Solomon Code: Parameter `c_m' should be at least 1." );
				if ( w < 1 )
					CFG_PARSE_ERROR( "GlobalConfig", "Cauchy-based Reed-Solomon Code: Parameter `c_w' should be at least 1." );
				if ( w > 32 )
					CFG_PARSE_ERROR( "GlobalConfig", "Cauchy-based Reed-Solomon Code: Parameter `c_w' should be at most 32." );
				if ( w < 30 && k + m > ( ( uint32_t ) 1 << w ) )
					CFG_PARSE_ERROR( "GlobalConfig", "Cauchy-based Reed-Solomon Code: Parameters `c_k', `c_m', and `c_w' should satisfy c_k + c_m <= ( 1 << c_w ) when c_w < 30." );
			}
			break;
		default:
			CFG_PARSE_ERROR( "GlobalConfig", "Unsupported coding scheme." );
			break;
	}
	return true;
}

void GlobalConfig::print( FILE *f ) {
	int width = 24;
	fprintf(
		f,
		"### Global Configuration ###\n"
		"- Size\n"
		"\t- %-*s : %u\n"
		"\t- %-*s : %u\n"
		"- Stripe list\n"
		"\t- %-*s : %u\n"
		"- epoll settings\n"
		"\t- %-*s : %u\n"
		"\t- %-*s : %d\n"
		"- Workers\n"
		"\t- %-*s : %u\n"
		"- Event queue\n"
		"\t- %-*s : %s\n"
		"\t- %-*s : %u; %u (prioritized)\n"
		"- Timeout\n"
		"\t- %-*s : %u\n"
		"\t- %-*s : %u\n"
		"- States\n"
		"\t- %-*s : %s\n",
		width, "Key", this->size.key,
		width, "Chunk", this->size.chunk,
		width, "Count", this->stripeLists.count,
		width, "Maximum number of events", this->epoll.maxEvents,
		width, "Timeout", this->epoll.timeout,
		width, "Count", this->workers.count,
		width, "Blocking?", this->eventQueue.block ? "Yes" : "No",
		width, "Size", this->eventQueue.size, this->eventQueue.prioritized,
		width, "Metadata", this->timeout.metadata,
		width, "Load", this->timeout.load,
		width, "Disabled?", this->states.disabled ? "Yes" : "No"
	);

	if ( ! this->states.disabled ) {
		fprintf(
			f,
			"\t- %-*s : ",
			width, "Spread daemon address"
		);
		this->states.spreaddAddr.print( f );

		fprintf(
			f,
			"\t- %-*s : %u\n"
			"\t- %-*s : %u\n"
			"\t- %-*s : %f\n",
			width, "Number of workers", this->states.workers,
			width, "Size of event queue", this->states.queue,
			width, "Smoothing factor", this->states.smoothingFactor
		);
	}

	fprintf(
		f,
		"- Coding\n"
		"\t- %-*s : ",
		width, "Coding scheme"
	);
	switch( this->coding.scheme ) {
		case CS_RAID0:
			fprintf( f, "RAID-0 (n = %u)\n", this->coding.params.getN() );
			break;
		case CS_RAID1:
			fprintf( f, "RAID-1 (n = %u)\n", this->coding.params.getN() );
			break;
		case CS_RAID5:
			fprintf( f, "RAID-5 (n = %u)\n", this->coding.params.getN() );
			break;
		case CS_RS:
			fprintf( f, "Reed-Solomon Code (k = %u, m = %u, w = %u)\n", this->coding.params.getK(), this->coding.params.getM(), this->coding.params.getW() );
			break;
		case CS_RDP:
			fprintf( f, "Row-Diagonal Parity Code (n = %u)\n", this->coding.params.getN() );
			break;
		case CS_EVENODD:
			fprintf( f, "EVENODD Code (n = %u)\n", this->coding.params.getN() );
			break;
		case CS_CAUCHY:
			fprintf( f, "Cauchy-based Reed-Solomon Code (k = %u, m = %u, w = %u)\n", this->coding.params.getK(), this->coding.params.getM(), this->coding.params.getW() );
			break;
		default:
			fprintf( f, "Undefined coding scheme\n" );
			break;
	}

	fprintf( f, "- Coordinators\n" );
	for ( int i = 0, len = this->coordinators.size(); i < len; i++ ) {
		fprintf( f, "\t%d. ", ( i + 1 ) );
		this->coordinators[ i ].print( f );
	}

	fprintf( f, "- Servers\n" );
	for ( int i = 0, len = this->servers.size(); i < len; i++ ) {
		fprintf( f, "\t%d. ", ( i + 1 ) );
		this->servers[ i ].print( f );
	}

	fprintf( f, "\n" );
}
