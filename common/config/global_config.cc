#include <cstdlib>
#include "global_config.hh"

bool GlobalConfig::set( const char *section, const char *name, const char *value ) {
	if ( match( section, "size" ) ) {
		if ( match( name, "key_size" ) )
			this->keySize = atoi( value );
		else if ( match( name, "chunk_size" ) )
			this->chunkSize = atoi( value );
	} else if ( match( section, "slaves" ) ) {
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
		}
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
		}
	}
	return true;
}

bool GlobalConfig::validate() {
	return true;
}
