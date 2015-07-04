#include "master.hh"

Master::Master() {

}

bool Master::init( char *path, bool verbose ) {
	bool ret;
	// Parse configuration files //
	if ( ( ! ( ret = this->config.global.parse( path ) ) ) ||
	     ( ! ( ret = this->config.master.merge( this->config.global ) ) ) ||
	     ( ! ( ret = this->config.master.parse( path ) ) ) ) {
		return false;
	}

	// Initialize modules //
	/* Socket */
	if ( ! this->socket.init(
			this->config.master.addr.type,
			this->config.master.addr.addr,
			this->config.master.addr.port,
			this->config.master.epollMaxEvents,
			this->config.master.epollTimeout
		) || ! this->socket.start() ) {
		__ERROR__( "Master", "init", "Cannot initialize socket." );
		return false;
	}

	if ( verbose ) {
		this->config.global.print();
		this->config.master.print();
	}
	return false;
}

bool Master::start() {
	return false;
}

bool Master::stop() {
	return false;
}

void Master::print( FILE *f ) {
	this->config.global.print( f );
	this->config.master.print( f );
}
