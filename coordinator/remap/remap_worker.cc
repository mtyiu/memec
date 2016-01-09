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
	inet_ntop( AF_INET, &event.slave.sin_addr.s_addr, buf, INET_ADDRSTRLEN );
	// wait for "sync meta ack" to proceed, release lock once acquired
	__DEBUG__( CYAN, "CoordinatorRemapWorker", "transitToDegraded",
		"Start transition to degraded for slave %s:%u.",
		buf, ntohs( event.slave.sin_port )
	);

	// any cleanup to be done (request for metadata sync. and parity revert)
	// wait for metadata sync to complete
	crmh->transitToDegradedEnd( event.slave );

	// wait for parity revert to complete
	// obtaining ack from all masters to ensure all masters already complete reverting parity
	pthread_mutex_lock( &crmh->ackSignalLock );
	if ( crmh->isAllMasterAcked( event.slave ) == false )
		pthread_cond_wait( &crmh->ackSignal[ event.slave ], &crmh->ackSignalLock );
	pthread_mutex_unlock( &crmh->ackSignalLock );

	LOCK( &crmh->slavesStateLock[ event.slave ] );
	// for multi-threaded environment, it is possible that other workers change
	// the state while this worker is waiting
	// just abort and late comers to take over
	if ( crmh->slavesState[ event.slave ] != REMAP_INTERMEDIATE ) {
		UNLOCK( &crmh->slavesStateLock[ event.slave ] );
		return false;
	}

	// broadcast to master: the transition is completed
	crmh->slavesState[ event.slave ] = REMAP_DEGRADED; // Phase 2
	if ( crmh->sendStateToMasters( event.slave ) == false ) {
		__ERROR__( "CoordinatorRemapWorker", "transitToDegraded",
			"Failed to broadcast state changes on slave %s:%u!",
			buf, ntohs( event.slave.sin_port )
		);
		UNLOCK( &crmh->slavesStateLock[ event.slave ] );
		return false;
	}

	UNLOCK( &crmh->slavesStateLock[ event.slave ] );
	return true;
}

bool CoordinatorRemapWorker::transitToNormal( RemapStateEvent event ) { // Phase 1b --> 0
	CoordinatorRemapMsgHandler *crmh = CoordinatorRemapMsgHandler::getInstance();

	pthread_mutex_t lock;
	pthread_mutex_init( &lock, NULL );
	char buf[ INET_ADDRSTRLEN ];
	inet_ntop( AF_INET, &event.slave.sin_addr.s_addr, buf, INET_ADDRSTRLEN );

	// wait for "all coordinated req. ack", release lock once acquired
	pthread_mutex_lock( &crmh->ackSignalLock );
	if ( crmh->isAllMasterAcked( event.slave ) == false )
		pthread_cond_wait( &crmh->ackSignal[ event.slave ], &crmh->ackSignalLock );
	pthread_mutex_unlock( &crmh->ackSignalLock );

	LOCK( &crmh->slavesStateLock[ event.slave ] );
	// for multi-threaded environment, it is possible that other workers change
	// the status while this worker is waiting.
	// just abort and late comers to take over
	if ( crmh->slavesState[ event.slave ] != REMAP_COORDINATED ) {
		__ERROR__(
			"CoordinatorRemapWorker", "transitToNormal",
			"State changed to %d: %s:%u.",
			crmh->slavesState[ event.slave ],
			buf, ntohs( event.slave.sin_port )
		);
		UNLOCK( &crmh->slavesStateLock[ event.slave ] );
		return false;
	}
	// any cleanup to be done
	crmh->transitToNormalEnd( event.slave ); // Prepare for switching to Phase 1b from Phase 0

	// ask master to use normal SET workflow
	crmh->slavesState[ event.slave ] = REMAP_NORMAL;
	if ( crmh->sendStateToMasters( event.slave ) == false ) {
		__ERROR__( "CoordinatorRemapWorker", "transitToNormal",
			"Failed to broadcast state changes on slave %s:%u!",
			buf, ntohs( event.slave.sin_port )
		);
		UNLOCK( &crmh->slavesStateLock[ event.slave ] );
		return false;
	}
	UNLOCK( &crmh->slavesStateLock[ event.slave ] );
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
