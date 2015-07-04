#include "coordinator.hh"

Coordinator::Coordinator() {

}

bool Coordinator::init( char *path, bool verbose ) {
	bool ret;
	// Parse configuration files //
	if ( ( ! ( ret = this->config.global.parse( path ) ) ) ||
	     ( ! ( ret = this->config.coordinator.merge( this->config.global ) ) ) ||
	     ( ! ( ret = this->config.coordinator.parse( path ) ) ) ||
	     ( ! this->config.coordinator.validate( this->config.global.coordinators ) ) ) {
		return false;
	}

	// Initialize modules //
	/* Socket */
	if ( ! this->socket.init(
			this->config.coordinator.addr.type,
			this->config.coordinator.addr.addr,
			this->config.coordinator.addr.port,
			this->config.coordinator.epollMaxEvents,
			this->config.coordinator.epollTimeout,
			this->config.global.slaves.size()
		) || ! this->socket.start() ) {
		__ERROR__( "Coordinator", "init", "Cannot initialize socket." );
		return false;
	}

	if ( verbose ) {
		this->config.global.print();
		this->config.coordinator.print();
	}
	return false;
}

bool Coordinator::start() {
	return false;
}

bool Coordinator::stop() {
	return false;
}

void Coordinator::print( FILE *f ) {
	this->config.global.print( f );
	this->config.coordinator.print( f );
}
