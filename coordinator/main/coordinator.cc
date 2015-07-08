#include <cstring>
#include "coordinator.hh"

Coordinator::Coordinator() {
	memset( &this->eventQueue, 0, sizeof( this->eventQueue ) );
}

void Coordinator::free() {
	if ( this->config.coordinator.eventQueue.type == EVENT_QUEUE_TYPE_MIXED ) {
		delete this->eventQueue.mixed;
	} else {
		delete this->eventQueue.separated.application;
		delete this->eventQueue.separated.coordinator;
		delete this->eventQueue.separated.master;
		delete this->eventQueue.separated.slave;
	}
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
	if ( ! this->sockets.epoll.init(
			this->config.coordinator.epoll.maxEvents,
			this->config.coordinator.epoll.timeout
		) || ! this->sockets.self.init(
			this->config.coordinator.coordinator.addr.type,
			this->config.coordinator.coordinator.addr.addr,
			this->config.coordinator.coordinator.addr.port,
			this->config.global.slaves.size(),
			&this->sockets.epoll
		) ) {
		__ERROR__( "Coordinator", "init", "Cannot initialize socket." );
		return false;
	}
	/* Event queues */
	if ( this->config.coordinator.eventQueue.type == EVENT_QUEUE_TYPE_MIXED ) {
		this->eventQueue.mixed = new EventQueue<MixedEvent>(
			this->config.coordinator.eventQueue.size.mixed,
			this->config.coordinator.eventQueue.block
		);
	} else {
		this->eventQueue.separated.application = new EventQueue<ApplicationEvent>(
			this->config.coordinator.eventQueue.size.separated.application,
			this->config.coordinator.eventQueue.block
		);
		this->eventQueue.separated.coordinator = new EventQueue<CoordinatorEvent>(
			this->config.coordinator.eventQueue.size.separated.coordinator,
			this->config.coordinator.eventQueue.block
		);
		this->eventQueue.separated.master = new EventQueue<MasterEvent>(
			this->config.coordinator.eventQueue.size.separated.master,
			this->config.coordinator.eventQueue.block
		);
		this->eventQueue.separated.slave = new EventQueue<SlaveEvent>(
			this->config.coordinator.eventQueue.size.separated.slave,
			this->config.coordinator.eventQueue.block
		);
	}

	if ( verbose ) {
		this->config.global.print();
		this->config.coordinator.print();
	}
	return true;
}

bool Coordinator::start() {
	if ( ! this->sockets.self.start() ) {
		__ERROR__( "Coordinator", "start", "Cannot start socket." );
		return false;
	}
	return true;
}

bool Coordinator::stop() {
	/* Workers */

	/* Event queues */
	if ( this->config.coordinator.eventQueue.type == EVENT_QUEUE_TYPE_MIXED ) {
		this->eventQueue.mixed->stop();
	} else {
		this->eventQueue.separated.application->stop();
		this->eventQueue.separated.coordinator->stop();
		this->eventQueue.separated.master->stop();
		this->eventQueue.separated.slave->stop();
	}

	/* Sockets */
	this->sockets.epoll.stop();
	this->sockets.self.stop();
	for ( int i = 0, len = this->sockets.masters.size(); i < len; i++ )
		this->sockets.masters[ i ].stop();
	for ( int i = 0, len = this->sockets.slaves.size(); i < len; i++ )
		this->sockets.slaves[ i ].stop();

	this->free();
	return true;
}

void Coordinator::print( FILE *f ) {
	this->config.global.print( f );
	this->config.coordinator.print( f );
}
