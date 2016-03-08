#include "remap_msg_handler.hh"
#include "remap_worker.hh"
#include "../../common/util/debug.hh"
#include "arpa/inet.h"

CoordinatorRemapWorker::CoordinatorRemapWorker() {
}

CoordinatorRemapWorker::~CoordinatorRemapWorker() {
}

bool CoordinatorRemapWorker::transitToDegraded( RemapStateEvent event ) { // Phase 1a --> 2
	CoordinatorRemapMsgHandler *crmh = CoordinatorRemapMsgHandler::getInstance();

	pthread_mutex_t lock;
	pthread_mutex_init( &lock, NULL );
	char buf[ INET_ADDRSTRLEN ];
	inet_ntop( AF_INET, &event.server.sin_addr.s_addr, buf, INET_ADDRSTRLEN );
	// wait for "sync meta ack" to proceed, release lock once acquired
	__DEBUG__( CYAN, "CoordinatorRemapWorker", "transitToDegraded",
		"Start transition to degraded for server %s:%u.",
		buf, ntohs( event.server.sin_port )
	);

	// any cleanup to be done (request for metadata sync. and parity revert)
	// wait for metadata sync to complete
	crmh->transitToDegradedEnd( event.server );

	// wait for parity revert to complete
	// obtaining ack from all clients to ensure all clients already complete reverting parity
	pthread_mutex_lock( &crmh->ackSignalLock );
	if ( crmh->isAllClientAcked( event.server ) == false )
		pthread_cond_wait( &crmh->ackSignal[ event.server ], &crmh->ackSignalLock );
	pthread_mutex_unlock( &crmh->ackSignalLock );

	LOCK( &crmh->serversStateLock[ event.server ] );
	// for multi-threaded environment, it is possible that other workers change
	// the state while this worker is waiting
	// just abort and late comers to take over
	if ( crmh->serversState[ event.server ] != REMAP_INTERMEDIATE ) {
		UNLOCK( &crmh->serversStateLock[ event.server ] );
		return false;
	}

	// broadcast to client: the transition is completed
	crmh->serversState[ event.server ] = REMAP_DEGRADED; // Phase 2
	if ( crmh->broadcastState( event.server ) < 0 ) {
		__ERROR__( "CoordinatorRemapWorker", "transitToDegraded",
			"Failed to broadcast state changes on server %s:%u!",
			buf, ntohs( event.server.sin_port )
		);
		UNLOCK( &crmh->serversStateLock[ event.server ] );
		return false;
	}

	UNLOCK( &crmh->serversStateLock[ event.server ] );
	return true;
}

bool CoordinatorRemapWorker::transitToNormal( RemapStateEvent event ) { // Phase 1b --> 0
	CoordinatorRemapMsgHandler *crmh = CoordinatorRemapMsgHandler::getInstance();

	pthread_mutex_t lock;
	pthread_mutex_init( &lock, NULL );
	char buf[ INET_ADDRSTRLEN ];
	inet_ntop( AF_INET, &event.server.sin_addr.s_addr, buf, INET_ADDRSTRLEN );

	// wait for "all coordinated req. ack", release lock once acquired
	pthread_mutex_lock( &crmh->ackSignalLock );
	if ( crmh->isAllClientAcked( event.server ) == false )
		pthread_cond_wait( &crmh->ackSignal[ event.server ], &crmh->ackSignalLock );
	pthread_mutex_unlock( &crmh->ackSignalLock );

	LOCK( &crmh->serversStateLock[ event.server ] );
	// for multi-threaded environment, it is possible that other workers change
	// the status while this worker is waiting.
	// just abort and late comers to take over
	if ( crmh->serversState[ event.server ] != REMAP_COORDINATED ) {
		__ERROR__(
			"CoordinatorRemapWorker", "transitToNormal",
			"State changed to %d: %s:%u.",
			crmh->serversState[ event.server ],
			buf, ntohs( event.server.sin_port )
		);
		UNLOCK( &crmh->serversStateLock[ event.server ] );
		return false;
	}
	// any cleanup to be done
	crmh->transitToNormalEnd( event.server ); // Prepare for switching to Phase 1b from Phase 0

	// ask client to use normal SET workflow
	crmh->serversState[ event.server ] = REMAP_NORMAL;
	if ( crmh->broadcastState( event.server ) < 0 ) {
		__ERROR__( "CoordinatorRemapWorker", "transitToNormal",
			"Failed to broadcast state changes on server %s:%u!",
			buf, ntohs( event.server.sin_port )
		);
		UNLOCK( &crmh->serversStateLock[ event.server ] );
		return false;
	}
	UNLOCK( &crmh->serversStateLock[ event.server ] );
	return true;
}

void *CoordinatorRemapWorker::run( void* argv ) {
	CoordinatorRemapMsgHandler *crmh = CoordinatorRemapMsgHandler::getInstance();
	CoordinatorRemapWorker *worker = ( CoordinatorRemapWorker *) argv;
	RemapStateEvent event;
	bool ret;

	while ( true ) {
		ret = crmh->eventQueue->extract( event );
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

	__DEBUG__( BLUE, "CoordinatorRemapWorker", "run" "Worker thread stop running." );
	worker->free();
	pthread_exit( 0 );
	return 0;
}

bool CoordinatorRemapWorker::start() {
	if ( ! this->isRunning ) {
		this->isRunning = true;
		if ( pthread_create( &this->tid, NULL, CoordinatorRemapWorker::run, ( void * ) this ) != 0 ) {
			__ERROR__( "CoordinatorRemapWorker", "start", "Cannot start worker thread." );
			return false;
		}
	}
	return true;
}

void CoordinatorRemapWorker::stop() {
	this->isRunning = false;
}

void CoordinatorRemapWorker::free() {
}

void CoordinatorRemapWorker::print( FILE *f ) {
	// TODO
}
