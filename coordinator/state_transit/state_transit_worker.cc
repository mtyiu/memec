#include "state_transit_handler.hh"
#include "state_transit_worker.hh"
#include "../main/coordinator.hh"
#include "../../common/util/debug.hh"
#include "arpa/inet.h"

CoordinatorStateTransitWorker::CoordinatorStateTransitWorker() {
}

CoordinatorStateTransitWorker::~CoordinatorStateTransitWorker() {
}

bool CoordinatorStateTransitWorker::transitToDegraded( StateTransitEvent event ) { // Phase 1a --> 2
	CoordinatorStateTransitHandler *csth = CoordinatorStateTransitHandler::getInstance();

	pthread_mutex_t lock;
	pthread_mutex_init( &lock, NULL );
	char buf[ INET_ADDRSTRLEN ];
	inet_ntop( AF_INET, &event.server.sin_addr.s_addr, buf, INET_ADDRSTRLEN );
	// wait for "sync meta ack" to proceed, release lock once acquired
	__DEBUG__( CYAN, "CoordinatorStateTransitWorker", "transitToDegraded",
		"Start transition to degraded for server %s:%u.",
		buf, ntohs( event.server.sin_port )
	);

	// any cleanup to be done (request for metadata sync. and parity revert)
	// wait for metadata sync to complete
	csth->transitToDegradedEnd( event.server );

	// wait for parity revert to complete
	// obtaining ack from all clients to ensure all clients already complete reverting parity
	pthread_mutex_lock( &csth->ackSignalLock );
	if ( csth->isAllClientAcked( event.server ) == false )
		pthread_cond_wait( &csth->ackSignal[ event.server ], &csth->ackSignalLock );
	pthread_mutex_unlock( &csth->ackSignalLock );

	LOCK( &csth->serversStateLock[ event.server ] );
	// for multi-threaded environment, it is possible that other workers change
	// the state while this worker is waiting
	// just abort and late comers to take over
	if ( csth->serversState[ event.server ] != STATE_INTERMEDIATE ) {
		UNLOCK( &csth->serversStateLock[ event.server ] );
		return false;
	}

	// broadcast to client: the transition is completed
	csth->serversState[ event.server ] = STATE_DEGRADED; // Phase 2

	// wait for others to complete the transition
	if ( csth->removeFailedServer( event.server ) > 0 ) {
		UNLOCK( &csth->serversStateLock[ event.server ] );
		return true;
	}

	std::vector<struct sockaddr_in> updatedServers;
	csth->eraseUpdatedServers( &updatedServers );

	if ( csth->broadcastState( updatedServers ) < 0 ) {
		__ERROR__( "CoordinatorStateTransitWorker", "transitToDegraded",
			"Failed to broadcast state changes on server %s:%u!",
			buf, ntohs( event.server.sin_port )
		);
		UNLOCK( &csth->serversStateLock[ event.server ] );
		return false;
	}

	UNLOCK( &csth->serversStateLock[ event.server ] );

	// recover some chunks first based on popularity
	if ( Coordinator::getInstance()->config.global.recovery.popular.enabled ) {
		Coordinator::getInstance()->recoverPopularChunks( event.server );
	}

	return true;
}

bool CoordinatorStateTransitWorker::transitToNormal( StateTransitEvent event ) { // Phase 1b --> 0
	CoordinatorStateTransitHandler *csth = CoordinatorStateTransitHandler::getInstance();

	pthread_mutex_t lock;
	pthread_mutex_init( &lock, NULL );
	char buf[ INET_ADDRSTRLEN ];
	inet_ntop( AF_INET, &event.server.sin_addr.s_addr, buf, INET_ADDRSTRLEN );

	// wait for "all coordinated req. ack", release lock once acquired
	pthread_mutex_lock( &csth->ackSignalLock );
	if ( csth->isAllClientAcked( event.server ) == false )
		pthread_cond_wait( &csth->ackSignal[ event.server ], &csth->ackSignalLock );
	pthread_mutex_unlock( &csth->ackSignalLock );

	LOCK( &csth->serversStateLock[ event.server ] );
	// for multi-threaded environment, it is possible that other workers change
	// the status while this worker is waiting.
	// just abort and late comers to take over
	if ( csth->serversState[ event.server ] != STATE_COORDINATED ) {
		__ERROR__(
			"CoordinatorStateTransitWorker", "transitToNormal",
			"State changed to %d: %s:%u.",
			csth->serversState[ event.server ],
			buf, ntohs( event.server.sin_port )
		);
		UNLOCK( &csth->serversStateLock[ event.server ] );
		return false;
	}
	// any cleanup to be done
	csth->transitToNormalEnd( event.server ); // Prepare for switching to Phase 1b from Phase 0

	// ask client to use normal SET workflow
	csth->serversState[ event.server ] = STATE_NORMAL;

	// wait for others to complete the transition
	if ( csth->removeFailedServer( event.server ) > 0 ) {
		UNLOCK( &csth->serversStateLock[ event.server ] );
		return true;
	}

	std::vector<struct sockaddr_in> updatedServers;
	csth->eraseUpdatedServers( &updatedServers );

	if ( csth->broadcastState( updatedServers ) < 0 ) {
		__ERROR__( "CoordinatorStateTransitWorker", "transitToNormal",
			"Failed to broadcast state changes on server %s:%u!",
			buf, ntohs( event.server.sin_port )
		);
		UNLOCK( &csth->serversStateLock[ event.server ] );
		return false;
	}
	UNLOCK( &csth->serversStateLock[ event.server ] );
	return true;
}

void *CoordinatorStateTransitWorker::run( void* argv ) {
	CoordinatorStateTransitHandler *csth = CoordinatorStateTransitHandler::getInstance();
	CoordinatorStateTransitWorker *worker = ( CoordinatorStateTransitWorker *) argv;
	StateTransitEvent event;
	bool ret;

	while ( true ) {
		ret = csth->eventQueue->extract( event );
		if ( ! ret ) {
			if ( ! worker->getIsRunning() ) {
				break;
			}
			continue;
		}
		if ( event.start )
			worker->transitToDegraded( event );
		else
			worker->transitToNormal( event );
	}

	__DEBUG__( BLUE, "CoordinatorStateTransitWorker", "run" "Worker thread stop running." );
	worker->free();
	pthread_exit( 0 );
	return 0;
}

bool CoordinatorStateTransitWorker::start() {
	if ( ! this->isRunning ) {
		this->isRunning = true;
		if ( pthread_create( &this->tid, NULL, CoordinatorStateTransitWorker::run, ( void * ) this ) != 0 ) {
			__ERROR__( "CoordinatorStateTransitWorker", "start", "Cannot start worker thread." );
			return false;
		}
	}
	return true;
}

void CoordinatorStateTransitWorker::stop() {
	this->isRunning = false;
}

void CoordinatorStateTransitWorker::free() {
}

void CoordinatorStateTransitWorker::print( FILE *f ) {
	// TODO
}
