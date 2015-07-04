#include <cstdlib>
#include "coordinator_config.hh"

bool CoordinatorConfig::merge( GlobalConfig &globalConfig ) {
	this->epollMaxEvents = globalConfig.epollMaxEvents;
	this->epollTimeout = globalConfig.epollTimeout;
	return true;
}

bool CoordinatorConfig::parse( const char *path ) {
	return Config::parse( path, "coordinator.ini" );
}

bool CoordinatorConfig::set( const char *section, const char *name, const char *value ) {
	if ( match( section, "coordinator" ) ) {
		return this->addr.parse( name, value );
	} else if ( match( section, "epoll" ) ) {
		if ( match( name, "max_events" ) )
			this->epollMaxEvents = atoi( value );
		else if ( match( name, "timeout" ) )
			this->epollTimeout = atoi( value );
		else
			return false;
	}
	return true;
}

bool CoordinatorConfig::validate() {
	if ( ! this->addr.isInitialized() )
		CFG_PARSE_ERROR( "CoordinatorConfig", "The coordinator is not assigned with an valid address." );

	if ( this->epollMaxEvents < 1 )
		CFG_PARSE_ERROR( "GlobalConfig", "Maximum number of events in epoll should be at least 1." );

	if ( this->epollTimeout < -1 )
		CFG_PARSE_ERROR( "GlobalConfig", "The timeout value of epoll should be either -1 (infinite blocking), 0 (non-blocking) or a positive value (representing the number of milliseconds to block)." );

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
	int width = 30;
	fprintf(
		f,
		"### Coordinator Configuration ###\n"
		"- %-*s : %u\n"
		"- %-*s : %d\n"
		"- %-*s : ",
		width, "Maximum number of epoll events", this->epollMaxEvents,
		width, "Timeout of epoll", this->epollTimeout,
		width, "Address"
	);
	this->addr.print( f );

	fprintf( f, "\n" );
}
