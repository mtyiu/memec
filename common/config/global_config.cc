#include <cstdlib>
#include "global_config.hh"

bool GlobalConfig::parse( const char *path ) {
	return Config::parse( path, "global.ini" );
}

bool GlobalConfig::set( const char *section, const char *name, const char *value ) {
	if ( match( section, "size" ) ) {
		if ( match( name, "key_size" ) )
			this->keySize = atoi( value );
		else if ( match( name, "chunk_size" ) )
			this->chunkSize = atoi( value );
		else
			return false;
	} else if ( match( section, "epoll" ) ) {
		if ( match( name, "max_events" ) )
			this->epollMaxEvents = atoi( value );
		else if ( match( name, "timeout" ) )
			this->epollTimeout = atoi( value );
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
	} else if ( match( section, "coding" ) ) {
		if ( match( value, "raid0" ) ) {
			this->codingScheme = CS_RAID0;
		} else if ( match( value, "raid1" ) ) {
			this->codingScheme = CS_RAID1;
		} else if ( match( value, "raid5" ) ) {
			this->codingScheme = CS_RAID5;
		} else if ( match( value, "rs" ) ) {
			this->codingScheme = CS_RS;
		} else if ( match( value, "embr" ) ) {
			this->codingScheme = CS_EMBR;
		} else if ( match( value, "rdp" ) ) {
			this->codingScheme = CS_RDP;
		} else if ( match( value, "evenodd" ) ) {
			this->codingScheme = CS_EVENODD;
		} else if ( match( value, "cauchy" ) ) {
			this->codingScheme = CS_CAUCHY;
		} else {
			this->codingScheme = CS_UNDEFINED;
			this->codingParams.setScheme( this->codingScheme );
			return false;
		}
		this->codingParams.setScheme( this->codingScheme );
	} else {
		if ( this->codingScheme == CS_RAID0 && match( section, "raid0" ) ) {
			if ( match( name, "n" ) )
				this->codingParams.setN( atoi( value ) );
		} else if ( this->codingScheme == CS_RAID1 && match( section, "raid1" ) ) {
			if ( match( name, "n" ) )
				this->codingParams.setN( atoi( value ) );
		} else if ( this->codingScheme == CS_RAID5 && match( section, "raid5" ) ) {
			if ( match( name, "n" ) )
				this->codingParams.setN( atoi( value ) );
		} else if ( this->codingScheme == CS_RS && match( section, "rs" ) ) {
			if ( match( name, "k" ) )
				this->codingParams.setK( atoi( value ) );
			else if ( match( name, "m" ) )
				this->codingParams.setM( atoi( value ) );
			else if ( match( name, "w" ) )
				this->codingParams.setW( atoi( value ) );
		} else if ( this->codingScheme == CS_EMBR && match( section, "embr" ) ) {
			if ( match( name, "n" ) )
				this->codingParams.setN( atoi( value ) );
			else if ( match( name, "k" ) )
				this->codingParams.setK( atoi( value ) );
			else if ( match( name, "w" ) )
				this->codingParams.setW( atoi( value ) );
		} else if ( this->codingScheme == CS_RDP && match( section, "rdp" ) ) {
			if ( match( name, "n" ) )
				this->codingParams.setN( atoi( value ) );
		} else if ( this->codingScheme == CS_EVENODD && match( section, "evenodd" ) ) {
			if ( match( name, "n" ) )
				this->codingParams.setN( atoi( value ) );
		} else if ( this->codingScheme == CS_CAUCHY && match( section, "cauchy" ) ) {
			if ( match( name, "c_k" ) )
				this->codingParams.setK( atoi( value ) );
			else if ( match( name, "c_m" ) )
				this->codingParams.setM( atoi( value ) );
			else if ( match( name, "c_w" ) )
				this->codingParams.setW( atoi( value ) );
		} else {
			return false;
		}
	}
	return true;
}

bool GlobalConfig::validate() {
	if ( this->keySize < 8 )
		CFG_PARSE_ERROR( "GlobalConfig", "Key size should be at least 8 bytes." );
	if ( this->keySize > 255 )
		CFG_PARSE_ERROR( "GlobalConfig", "Key size should be at most 255 bytes." );

	if ( this->epollMaxEvents < 1 )
		CFG_PARSE_ERROR( "GlobalConfig", "Maximum number of events in epoll should be at least 1." );

	if ( this->epollTimeout < -1 )
		CFG_PARSE_ERROR( "GlobalConfig", "The timeout value of epoll should be either -1 (infinite blocking), 0 (non-blocking) or a positive value (representing the number of milliseconds to block)." );

	if ( this->chunkSize < this->keySize + 4 ) // 2^24 bytes
		CFG_PARSE_ERROR( "GlobalConfig", "Chunk size should be at least %u bytes.", this->keySize + 4 );
	if ( this->chunkSize > 16777216 ) // 2^24 bytes
		CFG_PARSE_ERROR( "GlobalConfig", "Key size should be at most 16777216 bytes." );

	if ( this->coordinators.empty() )
		CFG_PARSE_ERROR( "GlobalConfig", "There should be at least one coordinator." );

	if ( this->slaves.empty() )
		CFG_PARSE_ERROR( "GlobalConfig", "There should be at least one slave." );

	switch( this->codingScheme ) {
		case CS_RAID0:
			if ( this->codingParams.getN() < 1 )
				CFG_PARSE_ERROR( "GlobalConfig", "RAID-0: Parameter `n' should be at least 1." );
			break;
		case CS_RAID1:
			if ( this->codingParams.getN() < 1 )
				CFG_PARSE_ERROR( "GlobalConfig", "RAID-1: Parameter `n' should be at least 1." );
			break;
		case CS_RAID5:
			if ( this->codingParams.getN() < 3 )
				CFG_PARSE_ERROR( "GlobalConfig", "RAID-5: Parameter `n' should be at least 3." );
			break;
		case CS_RS:
			{
				uint32_t k = this->codingParams.getK(),
				         m = this->codingParams.getM(),
				         w = this->codingParams.getW();
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
				uint32_t n = this->codingParams.getN(),
				         k = this->codingParams.getK(),
				         w = this->codingParams.getW();
				if ( n < 1 )
					CFG_PARSE_ERROR( "GlobalConfig", "Exact Minimum Bandwidth Regenerating (E-MBR) Code: Parameter `n' should be at least 1." );
				if ( k < 1 )
					CFG_PARSE_ERROR( "GlobalConfig", "Exact Minimum Bandwidth Regenerating (E-MBR) Code: Parameter `k' should be at least 1." );
				if ( w < 1 )
					CFG_PARSE_ERROR( "GlobalConfig", "Exact Minimum Bandwidth Regenerating (E-MBR) Code: Parameter `w' should be at least 1." );
			}
			break;
		case CS_RDP:
			if ( this->codingParams.getN() < 2 )
				CFG_PARSE_ERROR( "GlobalConfig", "Row-Diagonal Parity Code: Parameter `n' should be at least 2." );
			break;
		case CS_EVENODD:
			if ( this->codingParams.getN() < 2 )
				CFG_PARSE_ERROR( "GlobalConfig", "EVENODD Code: Parameter `n' should be at least 2." );
			break;
		case CS_CAUCHY:
			{
				uint32_t k = this->codingParams.getK(),
				         m = this->codingParams.getM(),
				         w = this->codingParams.getW();
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
	int width = 30;
	fprintf(
		f,
		"### Global Configuration ###\n"
		"- %-*s : %u\n"
		"- %-*s : %u\n"
		"- %-*s : %u\n"
		"- %-*s : %d\n"
		"- %-*s : ",
		width, "Key size", this->keySize,
		width, "Chunk size", this->chunkSize,
		width, "Maximum number of epoll events", this->epollMaxEvents,
		width, "Timeout of epoll", this->epollTimeout,
		width, "Coding scheme"
	);
	switch( this->codingScheme ) {
		case CS_RAID0:
			fprintf( f, "RAID-0 (n = %u)\n", this->codingParams.getN() );
			break;
		case CS_RAID1:
			fprintf( f, "RAID-1 (n = %u)\n", this->codingParams.getN() );
			break;
		case CS_RAID5:
			fprintf( f, "RAID-5 (n = %u)\n", this->codingParams.getN() );
			break;
		case CS_RS:
			fprintf( f, "Reed-Solomon Code (k = %u, m = %u, w = %u)\n", this->codingParams.getK(), this->codingParams.getM(), this->codingParams.getW() );
			break;
		case CS_EMBR:
			fprintf( f, "Exact Minimum Bandwidth Regenerating (E-MBR) Code (n = %u, k = %u, w = %u)\n", this->codingParams.getN(), this->codingParams.getK(), this->codingParams.getW() );
			break;
		case CS_RDP:
			fprintf( f, "Row-Diagonal Parity Code (n = %u)\n", this->codingParams.getN() );
			break;
		case CS_EVENODD:
			fprintf( f, "EVENODD Code (n = %u)\n", this->codingParams.getN() );
			break;
		case CS_CAUCHY:
			fprintf( f, "Cauchy-based Reed-Solomon Code (k = %u, m = %u, w = %u)\n", this->codingParams.getK(), this->codingParams.getM(), this->codingParams.getW() );
			break;
		default:
			fprintf( f, "Undefined coding scheme\n" );
			break;
	}

	fprintf( f, "- %-*s :\n", width, "Coordinator List" );
	for ( int i = 0, len = this->coordinators.size(); i < len; i++ ) {
		fprintf( f, "  %d. ", ( i + 1 ) );
		this->coordinators[ i ].print( f );
	}

	fprintf( f, "- %-*s :\n", width, "Slave List" );
	for ( int i = 0, len = this->slaves.size(); i < len; i++ ) {
		fprintf( f, "  %d. ", ( i + 1 ) );
		this->slaves[ i ].print( f );
	}

	fprintf( f, "\n" );
}
