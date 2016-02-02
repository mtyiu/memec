#include <cstdlib>
#include "global_config.hh"

GlobalConfig::GlobalConfig() {
	this->remap.maximum = 0;
}

bool GlobalConfig::parse( const char *path ) {
	return Config::parse( path, "global.ini" );
}

bool GlobalConfig::set( const char *section, const char *name, const char *value ) {
	if ( match( section, "size" ) ) {
		if ( match( name, "key_size" ) )
			this->size.key = atoi( value );
		else if ( match( name, "chunk_size" ) )
			this->size.chunk = atoi( value );
		else
			return false;
	} else if ( match( section, "stripe_list" ) ) {
		if ( match( name, "count" ) )
			this->stripeList.count = atoi( value );
		else
			return false;
	} else if ( match( section, "epoll" ) ) {
		if ( match( name, "max_events" ) )
			this->epoll.maxEvents = atoi( value );
		else if ( match( name, "timeout" ) )
			this->epoll.timeout = atoi( value );
		else
			return false;
	} else if ( match( section, "sync" ) ) {
		if ( match( name, "timeout" ) )
			this->sync.timeout = atoi( value );
		else
			return false;
	} else if ( match( section, "coordinators" ) ) {
		ServerAddr addr;
		if ( addr.parse( name, value ) )
			this->coordinators.push_back( addr );
		else
			return false;
	} else if ( match( section, "slaves" ) ) {
		ServerAddr addr;
		if ( addr.parse( name, value ) )
			this->slaves.push_back( addr );
		else
			return false;
	} else if ( match( section, "remap" ) ) {
		if ( match( name, "enabled" ) ) {
			this->remap.enabled = ( atoi( value ) >= 1 );
		} else if ( match( name, "spreadd" ) ) {
			ServerAddr addr;
			if ( addr.parse( name, value ) )
				this->remap.spreaddAddr = addr;
			else
				return false;
		} else if ( match( name, "startThreshold" ) ) {
			this->remap.startThreshold = atoi( value ) / 100.0;
		} else if ( match( name, "stopThreshold" ) ) {
			this->remap.stopThreshold = atoi( value ) / 100.0;
		} else if ( match( name, "overloadThreshold" ) ) {
			this->remap.overloadThreshold = atoi( value ) / 100.0;
			if ( this->remap.overloadThreshold <= 1 )
				return false;
		} else if ( match( name, "smoothingFactor" ) ) {
			this->remap.smoothingFactor = atof( value );
		} else if ( match( name, "maximum" ) ) {
			this->remap.maximum = atoi( value );
		} else if ( match( name, "manual" ) ) {
			this->remap.manual = ( atoi( value ) !=  0 );
		} else
			return false;
	} else if ( match( section, "buffer" ) ) {
		if ( match( name, "chunks_per_list" ) )
			this->buffer.chunksPerList = atoi( value );
		else
			return false;
	} else if ( match( section, "coding" ) ) {
		if ( match( value, "raid0" ) ) {
			this->coding.scheme = CS_RAID0;
		} else if ( match( value, "raid1" ) ) {
			this->coding.scheme = CS_RAID1;
		} else if ( match( value, "raid5" ) ) {
			this->coding.scheme = CS_RAID5;
		} else if ( match( value, "rs" ) ) {
			this->coding.scheme = CS_RS;
		} else if ( match( value, "embr" ) ) {
			this->coding.scheme = CS_EMBR;
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
		} else if ( this->coding.scheme == CS_EMBR && match( section, "embr" ) ) {
			if ( match( name, "n" ) )
				this->coding.params.setN( atoi( value ) );
			else if ( match( name, "k" ) )
				this->coding.params.setK( atoi( value ) );
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

	if ( this->stripeList.count < 1 )
		CFG_PARSE_ERROR( "GlobalConfig", "The number of stripe lists should be at least 1." );

	if ( this->epoll.maxEvents < 1 )
		CFG_PARSE_ERROR( "GlobalConfig", "Maximum number of events in epoll should be at least 1." );

	if ( this->epoll.timeout < -1 )
		CFG_PARSE_ERROR( "GlobalConfig", "The timeout value of epoll should be either -1 (infinite blocking), 0 (non-blocking) or a positive value (representing the number of milliseconds to block)." );

	if ( this->sync.timeout < 1 )
		CFG_PARSE_ERROR( "GlobalConfig", "The synchronization timeout should be at least 1 second." );

	if ( this->size.chunk < this->size.key + 4 ) // 2^24 bytes
		CFG_PARSE_ERROR( "GlobalConfig", "Chunk size should be at least %u bytes.", this->size.key + 4 );
	if ( this->size.chunk > 16777216 ) // 2^24 bytes
		CFG_PARSE_ERROR( "GlobalConfig", "Key size should be at most 16777216 bytes." );

	if ( this->coordinators.empty() )
		CFG_PARSE_ERROR( "GlobalConfig", "There should be at least one coordinator." );

	if ( this->slaves.empty() )
		CFG_PARSE_ERROR( "GlobalConfig", "There should be at least one slave." );

	if ( this->buffer.chunksPerList < 1 )
		CFG_PARSE_ERROR( "GlobalConfig", "The number of temporary chunks per stripe list should be at least 1." );

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
		case CS_EMBR:
			{
				uint32_t n = this->coding.params.getN(),
				         k = this->coding.params.getK(),
				         w = this->coding.params.getW();
				if ( n < 1 )
					CFG_PARSE_ERROR( "GlobalConfig", "Exact Minimum Bandwidth Regenerating (E-MBR) Code: Parameter `n' should be at least 1." );
				if ( k < 1 )
					CFG_PARSE_ERROR( "GlobalConfig", "Exact Minimum Bandwidth Regenerating (E-MBR) Code: Parameter `k' should be at least 1." );
				if ( w < 1 )
					CFG_PARSE_ERROR( "GlobalConfig", "Exact Minimum Bandwidth Regenerating (E-MBR) Code: Parameter `w' should be at least 1." );
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
		"- Synchronization\n"
		"\t- %-*s : %u\n"
		"- Buffer\n"
		"\t- %-*s : %u\n"
		"- Coding\n"
		"\t- %-*s : ",
		width, "Key size", this->size.key,
		width, "Chunk size", this->size.chunk,
		width, "Count", this->stripeList.count,
		width, "Maximum number of events", this->epoll.maxEvents,
		width, "Timeout", this->epoll.timeout,
		width, "Timeout", this->sync.timeout,
		width, "Chunks per list", this->buffer.chunksPerList,
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
		case CS_EMBR:
			fprintf( f, "Exact Minimum Bandwidth Regenerating (E-MBR) Code (n = %u, k = %u, w = %u)\n", this->coding.params.getN(), this->coding.params.getK(), this->coding.params.getW() );
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

	fprintf( f, "- Manual state transition: %s\n", ( this->remap.manual )? "On" : "Off" );

	fprintf( f, "- Coordinators\n" );
	for ( int i = 0, len = this->coordinators.size(); i < len; i++ ) {
		fprintf( f, "\t%d. ", ( i + 1 ) );
		this->coordinators[ i ].print( f );
	}

	fprintf( f, "- Slaves\n" );
	for ( int i = 0, len = this->slaves.size(); i < len; i++ ) {
		fprintf( f, "\t%d. ", ( i + 1 ) );
		this->slaves[ i ].print( f );
	}

	fprintf( f, "\n" );
}
