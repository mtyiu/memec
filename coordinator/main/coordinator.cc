#include "coordinator.hh"

Coordinator::Coordinator() {

}

bool Coordinator::init( char *path, bool verbose ) {
	bool ret;
	if ( ( ! ( ret = this->config.global.parse( path ) ) ) ||
	     ( ! ( ret = this->config.coordinator.parse( path ) ) ) ||
	     ( ! this->config.coordinator.validate( this->config.global.coordinators ) ) ) {
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
}
