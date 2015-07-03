#include "config.hh"
#include "../util/debug.hh"
#include "../../lib/inih/ini.h"

int Config::handler( void *data, const char *section, const char *name, const char *value ) {
	Config *config = ( Config * ) data;
	return config->set( section, name, value ) ? 1 : 0;
}

bool Config::parse( char *filename ) {
	if ( ini_parse( filename, handler, this ) < 0 ) {
		__ERROR__( "Config", "parse", "Cannot parse %s.", filename ); 
	}
	return this->validate();
}
