#include "slave.hh"

Slave::Slave() {

}

bool Slave::init( char *path, bool verbose ) {
	bool ret;
	// Parse configuration files //
	if ( ( ! ( ret = this->config.global.parse( path ) ) ) ||
	     ( ! ( ret = this->config.slave.merge( this->config.global ) ) ) ||
	     ( ! ( ret = this->config.slave.parse( path ) ) ) ||
	     ( ! this->config.slave.validate( this->config.global.slaves ) ) ) {
		return false;
	}

	// Initialize modules //
	/* Socket */
	if ( ! this->socket.init(
			this->config.slave.addr.type,
			this->config.slave.addr.addr,
			this->config.slave.addr.port,
			this->config.slave.epollMaxEvents,
			this->config.slave.epollTimeout
		) || ! this->socket.start() ) {
		__ERROR__( "Slave", "init", "Cannot initialize socket." );
		return false;
	}

	if ( verbose ) {
		this->config.global.print();
		this->config.slave.print();
	}
	return false;
}

bool Slave::start() {
	return false;
}

bool Slave::stop() {
	return false;
}

void Slave::print( FILE *f ) {
	this->config.global.print( f );
	this->config.slave.print( f );
}
