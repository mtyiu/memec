#include <cstdlib>
#include "coordinator_config.hh"

bool CoordinatorConfig::parse( const char *path ) {
	return Config::parse( path, "coordinator.ini" );
}

bool CoordinatorConfig::set( const char *section, const char *name, const char *value ) {
	if ( match( section, "coordinator" ) ) {
		return this->addr.parse( name, value );
	}
	return true;
}

bool CoordinatorConfig::validate() {
	if ( ! this->addr.isInitialized() )
		CFG_PARSE_ERROR( "CoordinatorConfig", "The coordinator is not assigned with an valid address." );
	return true;
}

bool CoordinatorConfig::validate( std::vector<ServerAddr> coordinators ) {
	if ( this->validate() ) {
		for ( int i = 0, len = coordinators.size(); i < len; i++ ) {
			if ( this->addr == coordinators[ i ] )
				return true;
		}
		CFG_PARSE_ERROR( "CoordinatorConfig", "The assigned address does not match with the global coordinator list." );
	}
	return false;
}

void CoordinatorConfig::print( FILE *f ) {
	int width = 7;
	fprintf(
		f,
		"### Coordinator Configuration ###\n"
		"- %-*s : ",
		width, "Address"
	);
	this->addr.print( f );

	fprintf( f, "\n" );
}
