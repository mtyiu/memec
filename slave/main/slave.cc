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
	if ( ! this->sockets.epoll.init(
			this->config.slave.epoll.maxEvents,
			this->config.slave.epoll.timeout
		) || ! this->sockets.self.init(
			this->config.slave.slave.addr.type,
			this->config.slave.slave.addr.addr,
			this->config.slave.slave.addr.port,
			&this->sockets.epoll
		) ) {
		__ERROR__( "Slave", "init", "Cannot initialize socket." );
		return false;
	}

	/* Vectors */
	this->sockets.coordinators.reserve( this->config.global.coordinators.size() );
	for ( int i = 0, len = this->config.global.coordinators.size(); i < len; i++ ) {
		this->sockets.coordinators.push_back( CoordinatorSocket() );
		this->sockets.coordinators[ i ].init( this->config.global.coordinators[ i ] );
	}

	if ( verbose ) {
		this->config.global.print();
		this->config.slave.print();
	}
	return true;
}

bool Slave::start() {
	// Connect to coordinators
	for ( int i = 0, len = this->config.global.coordinators.size(); i < len; i++ ) {
		if ( ! this->sockets.coordinators[ i ].start() )
			return false;
	}

	// Start listening
	if ( ! this->sockets.self.start() ) {
		__ERROR__( "Slave", "start", "Cannot start socket." );
		return false;
	}
	return true;
}

bool Slave::stop() {
	return false;
}

void Slave::print( FILE *f ) {
	this->config.global.print( f );
	this->config.slave.print( f );
}
