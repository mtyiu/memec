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
	if ( ! this->sockets.epoll.init(
			this->config.master.epoll.maxEvents,
			this->config.master.epoll.timeout
		) || ! this->sockets.self.init(
			this->config.master.master.addr.type,
			this->config.master.master.addr.addr,
			this->config.master.master.addr.port,
			&this->sockets.epoll
		) ) {
		__ERROR__( "Master", "init", "Cannot initialize socket." );
		return false;
	}

	/* Vectors */
	this->sockets.coordinators.reserve( this->config.global.coordinators.size() );
	for ( int i = 0, len = this->config.global.coordinators.size(); i < len; i++ ) {
		this->sockets.coordinators.push_back( CoordinatorSocket() );
		this->sockets.coordinators[ i ].init( this->config.global.coordinators[ i ] );
	}

	this->sockets.slaves.reserve( this->config.global.slaves.size() );
	for ( int i = 0, len = this->config.global.slaves.size(); i < len; i++ ) {
		this->sockets.slaves.push_back( SlaveSocket() );
		this->sockets.slaves[ i ].init( this->config.global.slaves[ i ] );
	}

	// Print debug messages //
	if ( verbose ) {
		this->config.global.print();
		this->config.master.print();
	}
	return true;
}

bool Master::start() {
	// Connect to coordinators
	for ( int i = 0, len = this->config.global.coordinators.size(); i < len; i++ ) {
		if ( ! this->sockets.coordinators[ i ].start() )
			return false;
	}

	// Connect to slaves
	for ( int i = 0, len = this->config.global.slaves.size(); i < len; i++ ) {
		if ( ! this->sockets.slaves[ i ].start() )
			return false;
	}

	// Start listening
	if ( ! this->sockets.self.start() ) {
		__ERROR__( "Master", "start", "Cannot start socket." );
		return false;
	}
	return true;
}

bool Master::stop() {
	return false;
}

void Master::print( FILE *f ) {
	this->config.global.print( f );
	this->config.master.print( f );
}
