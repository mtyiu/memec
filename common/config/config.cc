#include <cstdlib>
#include <cstring>
#include "config.hh"
#include "../util/debug.hh"
#include "../../lib/inih/ini.h"

int Config::handler( void *data, const char *section, const char *name, const char *value ) {
	Config *config = ( Config * ) data;
	return config->set( section, name, value ) ? 1 : 0;
}

bool Config::parse( const char *path, const char *filename ) {
	size_t pathLength = strlen( path ), filenameLength = strlen( filename );
	char *fullPath = ( char * ) calloc( pathLength + filenameLength + 2, sizeof( char ) );
	if ( ! fullPath ) {
		__ERROR__( "Config", "parse", "Cannot allocate memory." );
		return false;
	}
	
	strcpy( fullPath, path );
	fullPath[ pathLength ] = '/';
	strcpy( fullPath + pathLength + 1, filename );

	if ( ini_parse( fullPath, handler, this ) < 0 ) {
		__ERROR__( "Config", "parse", "Cannot parse %s.", path );
		return false;
	}
	free( fullPath );

	return this->validate();
}
