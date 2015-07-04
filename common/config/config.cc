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
	char *fullPath = ( char * ) calloc( pathLength + filenameLength + ( path[ pathLength - 1 ] == '/' ? 1 : 2 ), sizeof( char ) );
	if ( ! fullPath ) {
		__ERROR__( "Config", "parse", "Cannot allocate memory." );
		return false;
	}
	
	strcpy( fullPath, path );
	if ( path[ pathLength - 1 ] == '/' ) {
		strcpy( fullPath + pathLength, filename );
	} else {
		fullPath[ pathLength ] = '/';
		strcpy( fullPath + pathLength + 1, filename );
	}

	if ( ini_parse( fullPath, handler, this ) < 0 ) {
		__ERROR__( "Config", "parse", "Cannot parse %s.", fullPath );
		return false;
	}

	// Copy the config file contents as the serialized string
	FILE *f = fopen( fullPath, "r" );
	fseek( f, 0L, SEEK_END );
	this->serializedStringLength = ( size_t ) ftell( f );
	fseek( f, 0L, SEEK_SET );
	this->serializedString = ( char * ) malloc( this->serializedStringLength );
	if ( fread( this->serializedString, 1, this->serializedStringLength, f ) != this->serializedStringLength ) {
		__ERROR__( "Config", "parse", "Cannot read the whole configuration file." );
	}
	fclose( f );

	free( fullPath );

	return this->validate();
}

const char *Config::serialize( size_t &serializedStringLength ) {
	serializedStringLength = this->serializedStringLength;
	return this->serializedString;
}

bool Config::deserialize( const char *serializedString, size_t serializedStringLength ) {
	this->serializedStringLength = serializedStringLength;
	this->serializedString = strndup( serializedString, serializedStringLength );
	return true;
}
