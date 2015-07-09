#include <cstring>
#include "coordinator.hh"

Coordinator::Coordinator() {
	memset( &this->eventQueue, 0, sizeof( this->eventQueue ) );
}

void Coordinator::free() {
	if ( this->config.coordinator.workers.type == WORKER_TYPE_MIXED ) {
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
	/* Sockets */
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
	/* Workers and event queues */
	if ( this->config.coordinator.workers.type == WORKER_TYPE_MIXED ) {
		this->eventQueue.mixed = new EventQueue<MixedEvent>(
			this->config.coordinator.eventQueue.size.mixed,
			this->config.coordinator.eventQueue.block
		);
		this->workers.reserve( this->config.coordinator.workers.number.mixed );
		for ( int i = 0, len = this->config.coordinator.workers.number.mixed; i < len; i++ ) {
			this->workers.push_back( CoordinatorWorker() );
			this->workers[ i ].init( WORKER_ROLE_MIXED, &this->eventQueue );
		}
	} else {
		this->workers.reserve( this->config.coordinator.workers.number.separated.total );

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

		int index = 0;
		for ( int i = 0, len = this->config.coordinator.workers.number.separated.application; i < len; i++, index++ ) {
			this->workers.push_back( CoordinatorWorker() );
			this->workers[ index ].init( WORKER_ROLE_APPLICATION, &this->eventQueue );
		}
		for ( int i = 0, len = this->config.coordinator.workers.number.separated.coordinator; i < len; i++, index++ ) {
			this->workers.push_back( CoordinatorWorker() );
			this->workers[ index ].init( WORKER_ROLE_COORDINATOR, &this->eventQueue );
		}
		for ( int i = 0, len = this->config.coordinator.workers.number.separated.master; i < len; i++, index++ ) {
			this->workers.push_back( CoordinatorWorker() );
			this->workers[ index ].init( WORKER_ROLE_MASTER, &this->eventQueue );
		}
		for ( int i = 0, len = this->config.coordinator.workers.number.separated.slave; i < len; i++, index++ ) {
			this->workers.push_back( CoordinatorWorker() );
			this->workers[ index ].init( WORKER_ROLE_SLAVE, &this->eventQueue );
		}
	}

	// Show configuration //
	if ( verbose ) {
		this->config.global.print();
		this->config.coordinator.print();
	}
	return true;
}

bool Coordinator::start() {
	/* Workers and event queues */
	if ( this->config.coordinator.workers.type == WORKER_TYPE_MIXED ) {
		this->eventQueue.mixed->start();
		for ( int i = 0, len = this->config.coordinator.workers.number.mixed; i < len; i++ ) {
			if ( this->workers[ i ].start() ) {
				this->workers[ i ].debug();
			}
		}
	} else {
		this->eventQueue.separated.application->start();
		this->eventQueue.separated.coordinator->start();
		this->eventQueue.separated.master->start();
		this->eventQueue.separated.slave->start();
		for ( int i = 0, len = this->config.coordinator.workers.number.separated.total; i < len; i++ ) {
			if ( this->workers[ i ].start() ) {
				this->workers[ i ].debug();
			}
		}
	}

	/* Sockets */
	if ( ! this->sockets.self.start() ) {
		__ERROR__( "Coordinator", "start", "Cannot start socket." );
		return false;
	} else {
		this->sockets.self.debug();
	}

	return true;
}

bool Coordinator::stop() {
	int i, len;
	/* Workers */
	len = this->workers.size();
	for ( i = len - 1; i >= 0; i-- )
		this->workers[ i ].stop();
	for ( i = len - 1; i >= 0; i-- )
		this->workers[ i ].join();

	/* Workers and event queues */
	if ( this->config.coordinator.workers.type == WORKER_TYPE_MIXED ) {
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
	for ( i = 0, len = this->sockets.masters.size(); i < len; i++ )
		this->sockets.masters[ i ].stop();
	for ( i = 0, len = this->sockets.slaves.size(); i < len; i++ )
		this->sockets.slaves[ i ].stop();

	this->free();
	return true;
}

void Coordinator::print( FILE *f ) {
	this->config.global.print( f );
	this->config.coordinator.print( f );
}
