#include <cstdlib>
#include "coordinator_config.hh"

CoordinatorConfig::CoordinatorConfig() {
	this->states.isManual = true;
	this->states.maximum = 0;
	this->states.threshold.start = 0;
	this->states.threshold.stop = 0;
	this->states.threshold.overload = 0;
}

bool CoordinatorConfig::parse( const char *path ) {
	return Config::parse( path, "coordinator.ini" );
}

bool CoordinatorConfig::set( const char *section, const char *name, const char *value ) {
	if ( match( section, "coordinator" ) ) {
		return this->coordinator.addr.parse( name, value );
	} else if ( match( section, "states" ) ) {
		if ( match( name, "manual" ) )
			this->states.isManual = match( value, "true" );
		else if ( match( name, "maximum" ) )
			this->states.maximum = atoi( value );
		else if ( match( name, "start_threshold" ) )
			this->states.threshold.start = atof( value );
		else if ( match( name, "stop_threshold" ) )
			this->states.threshold.stop = atof( value );
		else if ( match( name, "overload_threshold" ) )
			this->states.threshold.overload = atof( value );
		else
			return false;
	} else {
		return false;
	}
	return true;
}

bool CoordinatorConfig::validate() {
	if ( ! this->coordinator.addr.isInitialized() )
		CFG_PARSE_ERROR( "CoordinatorConfig", "The coordinator is not assigned with an valid address." );

	if ( ! this->states.isManual ) {
		if ( this->states.threshold.start <= this->states.threshold.stop )
			CFG_PARSE_ERROR( "CoordinatorConfig", "The start threshold should be smaller than the stop threshold." );

		if ( this->states.threshold.overload <= 0 )
			CFG_PARSE_ERROR( "CoordinatorConfig", "The overload threshold should be larger than 0." );
	}

	return true;
}

bool CoordinatorConfig::validate( std::vector<ServerAddr> coordinators ) {
	if ( this->validate() ) {
		for ( int i = 0, len = coordinators.size(); i < len; i++ ) {
			if ( this->coordinator.addr == coordinators[ i ] )
				return true;
		}
		CFG_PARSE_ERROR( "CoordinatorConfig", "The assigned address does not match with the global coordinator list." );
	}
	return false;
}

void CoordinatorConfig::print( FILE *f ) {
	int width = 24;
	fprintf(
		f,
		"### Coordinator Configuration ###\n"
		"- Coordinator\n"
		"\t- %-*s : ",
		width, "Address"
	);
	this->coordinator.addr.print( f );

	fprintf(
		f,
		"- States\n"
		"\t- %-*s : %s\n",
		width, "Is manual?", this->states.isManual ? "Yes" : "No"
	);
	if ( ! this->states.isManual ) {
		fprintf(
			f,
			"\t- %-*s : %u\n"
			"\t- %-*s : %.2f%%\n"
			"\t- %-*s : %.2f%%\n"
			"\t- %-*s : %.2f%%\n",
			width, "Maximum", this->states.maximum,
			width, "Start threshold", this->states.threshold.start,
			width, "Stop threshold", this->states.threshold.stop,
			width, "Overload threshold", this->states.threshold.overload
		);
	}
	fprintf( f, "\n" );
}
