#include <cstdlib>
#include "client_config.hh"

ClientConfig::ClientConfig() {
	this->degraded.disabled = false;
	this->states.ackTimeout = 1;
	this->backup.ackBatchSize = 10000;
}

bool ClientConfig::parse( const char *path ) {
	return Config::parse( path, "client.ini" );
}

bool ClientConfig::set( const char *section, const char *name, const char *value ) {
	if ( match( section, "client" ) ) {
		return this->client.addr.parse( name, value );
	} else if ( match( section, "degraded" ) ) {
		if ( match ( name, "disabled" ) )
			this->degraded.disabled = match( value, "true" );
		else
			return false;
	} else if ( match( section, "states" ) ) {
		if ( match ( name, "ack_timeout" ) )
			this->states.ackTimeout = atoi( value );
		else
			return false;
	} else if ( match ( section, "backup" ) ) {
		if ( match( name, "ack_batch_size" ) )
			this->backup.ackBatchSize = atoi( value );
		else
			return false;
	} else {
		return false;
	}
	return true;
}

bool ClientConfig::validate() {
	if ( ! this->client.addr.isInitialized() )
		CFG_PARSE_ERROR( "ClientConfig", "The client is not assigned with an valid address." );

	return true;
}

void ClientConfig::print( FILE *f ) {
	int width = 14;
	fprintf(
		f,
		"### Client Configuration ###\n"
		"- Client\n"
		"\t- %-*s : ",
		width, "Address"
	);
	this->client.addr.print( f );
	fprintf(
		f,
		"- Degraded operations\n"
		"\t- %-*s : %s\n"
		"- States\n"
		"\t- %-*s : %u\n"
		"- Backup\n"
		"\t- %-*s : %u\n",
		width, "Disabled", this->degraded.disabled ? "Yes" : "No",
		width, "ACK timeout", this->states.ackTimeout,
		width, "ACK batch size", this->backup.ackBatchSize
	);
	fprintf( f, "\n" );
}
