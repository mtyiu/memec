#include <cstring>
#include <ctype.h>
#include "coordinator.hh"
#include "../ds/map.hh"
#include "../event/coordinator_event.hh"
#include "../../common/ds/instance_id_generator.hh"
#include "../../common/ds/packet_pool.hh"
#include "../../common/ds/sockaddr_in.hh"

#define GIGA 				( 1000 * 1000 * 1000 )
#define FLOAT_THRESHOLD		( ( float ) 0.00001 )
#define A_EQUAL_B( _A_, _B_ ) \
	( _A_ - _B_ >= -1 * FLOAT_THRESHOLD && _A_ - _B_ <= FLOAT_THRESHOLD )

uint16_t Coordinator::instanceId;

Coordinator::Coordinator() {
	this->isRunning = false;
	Coordinator::instanceId = InstanceIdGenerator::getInstance()->generate( 0 );
}

void Coordinator::free() {
	this->idGenerator.free();
	this->eventQueue.free();
	delete this->stripeList;
	Map::free();
}

void Coordinator::switchPhaseForCrashedSlave( ServerSocket *serverSocket ) {
	struct sockaddr_in addr = serverSocket->getAddr();
	MasterEvent event;

	LOCK( &this->overloadedSlaves.lock );
	std::set<struct sockaddr_in>::iterator it = this->overloadedSlaves.slaveSet.find( addr );
	if ( it == this->overloadedSlaves.slaveSet.end() ) {
		std::set<struct sockaddr_in> overloadedSlaves;
		overloadedSlaves.insert( addr );
		event.switchPhase( true, overloadedSlaves, true );
		this->eventQueue.insert( event );
	}
	UNLOCK( &this->overloadedSlaves.lock );
}

void Coordinator::switchPhase( std::set<struct sockaddr_in> prevOverloadedSlaves ) {
	// skip if remap feature is disabled
	if ( ! this->config.global.remap.enabled )
		return;

	MasterEvent event;
	LOCK( &this->overloadedSlaves.lock );

	double startThreshold = this->config.global.remap.startThreshold;
	double stopThreshold = this->config.global.remap.stopThreshold;
	uint32_t totalSlaveCount = this->sockets.slaves.size();
	uint32_t curOverloadedSlaveCount = this->overloadedSlaves.slaveSet.size();
	uint32_t prevOverloadedSlaveCount = prevOverloadedSlaves.size();

	if ( curOverloadedSlaveCount > totalSlaveCount * startThreshold ) { // Phase 1 --> 2
		// __INFO__( YELLOW, "Coordinator", "switchPhase", "%lf: Overload detected (overloaded slave = %u).", this->getElapsedTime(), curOverloadedSlaveCount );

		if ( this->config.global.remap.maximum > 0 && ( curOverloadedSlaveCount > this->config.global.remap.maximum || this->remapMsgHandler->reachMaximumRemapped( this->config.global.remap.maximum ) ) ) {
			// Limit the number of remapped slaves
			UNLOCK( &this->overloadedSlaves.lock );
			return;
		}

		// need to start remapping now
		if ( prevOverloadedSlaveCount > totalSlaveCount * startThreshold ) {
			std::set<struct sockaddr_in> newOverloadedSlaves = this->overloadedSlaves.slaveSet;
			// already started remapping
			// start recently overloaded slaves for remapping
			for ( auto slave : prevOverloadedSlaves )
				newOverloadedSlaves.erase( slave );
			if ( newOverloadedSlaves.size() > 0 ) {
				event.switchPhase( true, newOverloadedSlaves );
				this->eventQueue.insert( event );
			}
			// stop non-overloaded slaves from remapping
			for ( auto slave : this->overloadedSlaves.slaveSet )
				prevOverloadedSlaves.erase( slave );
			if ( prevOverloadedSlaves.size() > 0 ) {
				event.switchPhase( false, prevOverloadedSlaves ); // Phase 4 --> 3
				this->eventQueue.insert( event );
			}
		} else {
			// start remapping phase for all in the background
			event.switchPhase( true, this->overloadedSlaves.slaveSet );
			this->eventQueue.insert( event );
		}
	} else if ( curOverloadedSlaveCount < totalSlaveCount * stopThreshold &&
		prevOverloadedSlaveCount >= totalSlaveCount * startThreshold
	) {
		// no longer need remapping after remapping has started
		// stop remapping phase for all in the background
		event.switchPhase( false, prevOverloadedSlaves ); // Phase 4 --> 3
		this->eventQueue.insert( event );
	}
	UNLOCK( &this->overloadedSlaves.lock );
}

std::set<struct sockaddr_in> Coordinator::updateOverloadedSlaveSet( ArrayMap<struct sockaddr_in, Latency> *slaveGetLatency,
		ArrayMap<struct sockaddr_in, Latency> *slaveSetLatency, std::set<struct sockaddr_in> *slaveSet ) {
	double avgSec = 0.0, avgNsec = 0.0;
	LOCK( &this->overloadedSlaves.lock );

	std::set<struct sockaddr_in> prevOverloadedSlaves = this->overloadedSlaves.slaveSet;

	// what has past is left in the past
	this->overloadedSlaves.slaveSet.clear();
	double threshold = this->config.global.remap.overloadThreshold;

	// compare each slave latency with the avg multipled by threshold
#define GET_OVERLOADED_SLAVES( _TYPE_ ) { \
	uint32_t slaveCount = slave##_TYPE_##Latency->size(); \
	avgSec = 0.0; \
	avgNsec = 0.0; \
	/* get the average of slaves */ \
	for ( uint32_t i = 0; i < slaveCount; i++ ) { \
		avgSec += ( double ) slave##_TYPE_##Latency->values[ i ]->sec / slaveCount; \
		avgNsec += ( double ) slave##_TYPE_##Latency->values[ i ]->nsec / slaveCount; \
	} \
	/* if the average is too small ( esp. there is no data ), skip overload set update */ \
	if ( avgSec > FLOAT_THRESHOLD || avgNsec > FLOAT_THRESHOLD ) { \
		/* printf( " AVG %.3lf %.3lf vs THS %.3lf %.3lf\n", avgSec, avgNsec, avgSec * threshold, avgNsec * threshold ); */ \
		for ( uint32_t i = 0; i < slaveCount; i++ ) { \
			/* if (1) sec > avgSec or (2) sec == avgSec && nsec > avgNsec */ \
			if ( ( double ) slave##_TYPE_##Latency->values[ i ]->sec > avgSec * threshold || \
					( ( A_EQUAL_B ( slave##_TYPE_##Latency->values[ i ]->sec, avgSec * threshold ) && \
						(double) slave##_TYPE_##Latency->values[ i ]->nsec >= avgNsec * threshold ) ) ) { \
				this->overloadedSlaves.slaveSet.insert( slave##_TYPE_##Latency->keys[ i ] ); \
				slaveSet->insert( slave##_TYPE_##Latency->keys[ i ] ); \
				/* printf( "Slave #%u overloaded %u %u !!!!\n", i, slave##_TYPE_##Latency->values[ i ]->sec, slave##_TYPE_##Latency->values[ i ]->nsec ); */ \
			} \
		} \
	} \
}

	GET_OVERLOADED_SLAVES( Get );
	GET_OVERLOADED_SLAVES( Set );

#undef GET_OVERLOADED_SLAVES
	UNLOCK( &this->overloadedSlaves.lock );
	return prevOverloadedSlaves;
}

void Coordinator::updateAverageSlaveLoading( ArrayMap<struct sockaddr_in, Latency> *slaveGetLatency,
		ArrayMap<struct sockaddr_in, Latency> *slaveSetLatency ) {

	LOCK( &this->slaveLoading.lock );
	ArrayMap< struct sockaddr_in, ArrayMap< struct sockaddr_in, Latency > > *latest = NULL;
	double avgSec = 0.0, avgNsec = 0.0;

	// calculate the average from existing stat from masters
#define SET_AVG_SLAVE_LATENCY( _TYPE_ ) \
	latest = &this->slaveLoading.latest##_TYPE_; \
	for ( uint32_t i = 0; i < latest->size(); i++ ) { \
		avgSec = 0.0; \
		avgNsec = 0.0; \
		uint32_t masterCount = latest->values[ i ]->size(); \
		/* calculate the average over masters with latency measurement for this slave */ \
		for ( uint32_t j = 0; j < masterCount; j++ ) { \
			avgSec += ( double ) latest->values[ i ]->values[ j ]->sec / masterCount; \
			avgNsec += ( double ) latest->values[ i ]->values[ j ]->nsec / masterCount; \
			if ( avgNsec >= GIGA ) { \
				avgNsec -= GIGA; \
				avgSec += 1; \
			} \
		} \
		/* again, if too small or no data, skip the update, i.e. use old ones */ \
		if ( avgSec > FLOAT_THRESHOLD || avgNsec > FLOAT_THRESHOLD ) { \
			int idx = 0; \
			/* directly insert latency if not exists, otherwise, replace */ \
			Latency *oldLatency = slave##_TYPE_##Latency->get( latest->keys[ i ], &idx ); \
			if ( idx == -1 ) \
				slave##_TYPE_##Latency->set( latest->keys[ i ], new Latency( avgSec, avgNsec ) ); \
			else { \
				oldLatency->set( avgSec, avgNsec ); \
			} \
		} \
	}

	SET_AVG_SLAVE_LATENCY( Get );
	SET_AVG_SLAVE_LATENCY( Set );
#undef SET_AVG_SLAVE_LATENCY

	// clean up the current stats
	// TODO release the arrayMaps??
#define CLEAN_2D_ARRAY_MAP( _DST_ ) \
	for ( uint32_t i = 0; i < _DST_.size();  i++ ) { \
		_DST_.values.clear(); \
	} \
	_DST_.clear();

	CLEAN_2D_ARRAY_MAP( this->slaveLoading.latestGet );
	CLEAN_2D_ARRAY_MAP( this->slaveLoading.latestSet );
#undef CLEAN_2D_ARRAY_MAP
	UNLOCK( &this->slaveLoading.lock );
}

void Coordinator::signalHandler( int signal ) {
	Coordinator *coordinator = Coordinator::getInstance();
	ArrayMap<int, ClientSocket> &sockets = coordinator->sockets.masters;
	ArrayMap<struct sockaddr_in, Latency> *slaveGetLatency = new ArrayMap<struct sockaddr_in, Latency>();
	ArrayMap<struct sockaddr_in, Latency> *slaveSetLatency = new ArrayMap<struct sockaddr_in, Latency>();
	std::set<struct sockaddr_in> *overloadedSlaveSet = new std::set<struct sockaddr_in>();
	switch ( signal ) {
		case SIGALRM:
			coordinator->updateAverageSlaveLoading( slaveGetLatency, slaveSetLatency );
			// start / stop remapping according to criteria ( in non-manual mode )
			if ( coordinator->config.global.remap.manual == 0 ) {
				coordinator->switchPhase( coordinator->updateOverloadedSlaveSet( slaveGetLatency, slaveSetLatency, overloadedSlaveSet ) );
				// push the stats back to masters
				// leave the free of ArrayMaps to workers after constructing the data buffer
				LOCK( &sockets.lock );
				//fprintf( stderr, "queuing events get %lu set %lu\n", slaveGetLatency->size(), slaveSetLatency->size() );
				if ( slaveGetLatency->size() > 0 || slaveSetLatency->size() > 0 ) {
					MasterEvent event;
					for ( uint32_t i = 0; i < sockets.size(); i++ ) {
						event.reqPushLoadStats(
							sockets.values[ i ],
							new ArrayMap<struct sockaddr_in, Latency>( *slaveGetLatency ),
							new ArrayMap<struct sockaddr_in, Latency>( *slaveSetLatency ),
							new std::set<struct sockaddr_in>( *overloadedSlaveSet )
						);
						coordinator->eventQueue.insert( event );
					}
				}
				UNLOCK( &sockets.lock );
			}
			break;
		default:
			Coordinator::getInstance()->stop();
			fclose( stdin );
	}
	slaveGetLatency->clear();
	slaveSetLatency->clear();
	delete slaveGetLatency;
	delete slaveSetLatency;
	delete overloadedSlaveSet;
}

bool Coordinator::init( char *path, OptionList &options, bool verbose ) {
	// Parse configuration files //
	if ( ( ! this->config.global.parse( path ) ) ||
	     ( ! this->config.coordinator.merge( this->config.global ) ) ||
	     ( ! this->config.coordinator.parse( path ) ) ||
	     ( ! this->config.coordinator.override( options ) ) ||
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

	/* Vectors and other sockets */
	Socket::init( &this->sockets.epoll );
	ClientSocket::setArrayMap( &this->sockets.masters );
	ServerSocket::setArrayMap( &this->sockets.slaves );
	this->sockets.masters.reserve( this->config.global.slaves.size() );
	this->sockets.slaves.reserve( this->config.global.slaves.size() );
	this->sockets.backupSlaves.needsDelete = false;
	for ( int i = 0, len = this->config.global.slaves.size(); i < len; i++ ) {
		ServerSocket *socket = new ServerSocket();
		int tmpfd = - ( i + 1 );
		socket->init( tmpfd, this->config.global.slaves[ i ], &this->sockets.epoll );
		this->sockets.slaves.set( tmpfd, socket );
	}
	Map::init( this->config.global.stripeList.count );
	/* Stripe list */
	this->stripeList = new StripeList<ServerSocket>(
		this->config.global.coding.params.getChunkCount(),
		this->config.global.coding.params.getDataChunkCount(),
		this->config.global.stripeList.count,
		this->sockets.slaves.values
	);
	/* Packet Pool */
	this->packetPool.init(
		this->config.coordinator.pool.packets,
		Protocol::getSuggestedBufferSize(
			this->config.global.size.key,
			this->config.global.size.chunk
		)
	);
	/* Workers, ID generator and event queues */
	if ( this->config.coordinator.workers.type == WORKER_TYPE_MIXED ) {
		this->idGenerator.init( this->config.coordinator.workers.number.mixed );
		this->eventQueue.init(
			this->config.coordinator.eventQueue.block,
			this->config.coordinator.eventQueue.size.mixed,
			this->config.coordinator.eventQueue.size.pMixed
		);
		CoordinatorWorker::init();
		this->workers.reserve( this->config.coordinator.workers.number.mixed );
		for ( int i = 0, len = this->config.coordinator.workers.number.mixed; i < len; i++ ) {
			this->workers.push_back( CoordinatorWorker() );
			this->workers[ i ].init(
				this->config.global,
				WORKER_ROLE_MIXED,
				i // worker ID
			);
		}
	} else {
		this->idGenerator.init( this->config.coordinator.workers.number.separated.total );
		this->workers.reserve( this->config.coordinator.workers.number.separated.total );
		this->eventQueue.init(
			this->config.coordinator.eventQueue.block,
			this->config.coordinator.eventQueue.size.separated.coordinator,
			this->config.coordinator.eventQueue.size.separated.master,
			this->config.coordinator.eventQueue.size.separated.slave
		);
		CoordinatorWorker::init();

		int index = 0;
#define WORKER_INIT_LOOP( _FIELD_, _CONSTANT_ ) \
		for ( int i = 0, len = this->config.coordinator.workers.number.separated._FIELD_; i < len; i++, index++ ) { \
			this->workers.push_back( CoordinatorWorker() ); \
			this->workers[ index ].init( \
				this->config.global, \
				_CONSTANT_, \
				index \
			); \
		}

		WORKER_INIT_LOOP( coordinator, WORKER_ROLE_COORDINATOR )
		WORKER_INIT_LOOP( master, WORKER_ROLE_CLIENT )
		WORKER_INIT_LOOP( slave, WORKER_ROLE_SERVER )
#undef WORKER_INIT_LOOP
	}

	/* Remapping message handler */
	if ( this->config.global.remap.enabled ) {
		char coordName[ 11 ];
		memset( coordName, 0, 11 );
		sprintf( coordName, "%s%04d", COORD_PREFIX, this->config.coordinator.coordinator.addr.id );
		remapMsgHandler = CoordinatorRemapMsgHandler::getInstance();
		remapMsgHandler->init( this->config.global.remap.spreaddAddr.addr, this->config.global.remap.spreaddAddr.port, coordName );
		// add the slave addrs to remapMsgHandler
		LOCK( &this->sockets.slaves.lock );
		for ( uint32_t i = 0; i < this->sockets.slaves.size(); i++ ) {
			remapMsgHandler->addAliveSlave( this->sockets.slaves.values[ i ]->getAddr() );
		}
		UNLOCK( &this->sockets.slaves.lock );
		//remapMsgHandler->listAliveSlaves();
	}

	/* Smoothing factor */
	Latency::smoothingFactor = this->config.global.remap.smoothingFactor;

	/* Slave Loading stats */
	LOCK_INIT( &this->slaveLoading.lock );
	uint32_t sec, msec;
	if ( this->config.coordinator.loadingStats.updateInterval > 0 ) {
		sec = this->config.coordinator.loadingStats.updateInterval / 1000;
		msec = this->config.coordinator.loadingStats.updateInterval % 1000;
	} else {
		sec = 0;
		msec = 0;
	}
	statsTimer.setInterval( sec, msec );

	/* Pending Remapping Record */
	LOCK_INIT( &this->pendingRemappingRecords.toSendLock );

	/* Log */
	LOCK_INIT( &this->log.lock );

	/* Waiting for recovery */
	LOCK_INIT( &this->waitingForRecovery.lock );
	this->waitingForRecovery.isRecovering = false;

	// Set signal handlers //
	Signal::setHandler( Coordinator::signalHandler );

	// Show configuration //
	if ( verbose )
		this->info();
	return true;
}

bool Coordinator::start() {
	/* Workers and event queues */
	this->eventQueue.start();
	if ( this->config.coordinator.workers.type == WORKER_TYPE_MIXED ) {
		for ( int i = 0, len = this->config.coordinator.workers.number.mixed; i < len; i++ ) {
			this->workers[ i ].start();
		}
	} else {
		for ( int i = 0, len = this->config.coordinator.workers.number.separated.total; i < len; i++ ) {
			this->workers[ i ].start();
		}
	}

	/* Sockets */
	if ( ! this->sockets.self.start() ) {
		__ERROR__( "Coordinator", "start", "Cannot start socket." );
		return false;
	}

	/* Remapping message handler */
	if ( this->config.global.remap.enabled && ! this->remapMsgHandler->start() ) {
		__ERROR__( "Coordinator", "start", "Cannot start remapping message handler." );
		return false;
	}

	this->startTime = start_timer();
	this->isRunning = true;

	/* Slave loading stats */
	statsTimer.start();

	return true;
}

bool Coordinator::stop() {
	if ( ! this->isRunning )
		return false;

	int i, len;

	/* Socket */
	printf( "Stopping self-socket...\n" );
	this->sockets.self.stop();

	/* Workers */
	printf( "Stopping workers...\n" );
	len = this->workers.size();
	for ( i = len - 1; i >= 0; i-- )
		this->workers[ i ].stop();

	/* Event queues */
	printf( "Stopping event queues...\n" );
	this->eventQueue.stop();

	/* Workers */
	printf( "Stopping workers...\n" );
	for ( i = len - 1; i >= 0; i-- )
		this->workers[ i ].join();

	/* Sockets */
	printf( "Stopping master sockets...\n" );
	for ( i = 0, len = this->sockets.masters.size(); i < len; i++ )
		this->sockets.masters[ i ]->stop();
	this->sockets.masters.clear();

	printf( "Stopping slave sockets...\n" );
	for ( i = 0, len = this->sockets.slaves.size(); i < len; i++ )
		this->sockets.slaves[ i ]->stop();
	this->sockets.slaves.clear();

	/* Remapping message handler */
	printf( "Stopping remapping message handler...\n" );
	if ( this->config.global.remap.enabled ) {
		this->remapMsgHandler->stop();
		this->remapMsgHandler->quit();
	}

	/* Loading stats */
	printf( "Stopping loading stats...\n" );
	statsTimer.stop();

	this->free();
	this->isRunning = false;
	printf( "\nBye.\n" );
	return true;
}

void Coordinator::syncSlaveMeta( struct sockaddr_in slave, bool *sync ) {
	SlaveEvent event;
	ServerSocket *socket = NULL;
	struct sockaddr_in addr;

	// find the corresponding socket for slave by address
	LOCK( &this->sockets.slaves.lock );
	for( uint32_t i = 0; i < this->sockets.slaves.size(); i++ ) {
		addr = this->sockets.slaves.values[ i ]->getAddr();
		if ( slave == addr ) {
			socket = this->sockets.slaves.values[ i ];
			break;
		}
	}
	UNLOCK( &this->sockets.slaves.lock );
	if ( socket == NULL ) {
		__ERROR__( "Coordinator", "syncSlaveMeta", "Cannot find slave socket\n" );
		*sync = true;
		return;
	}

	event.reqSyncMeta( socket , sync );
	this->eventQueue.insert( event );
}

void Coordinator::releaseDegradedLock() {
	uint32_t socketFromId, socketToId;
	char tmp[ 16 ];
	SlaveEvent event;

	printf( "Which socket ([0-%lu] or all)? ", this->sockets.slaves.size() - 1 );
	fflush( stdout );
	if ( ! fgets( tmp, sizeof( tmp ), stdin ) )
		return;
	if ( strncmp( tmp, "all", 3 ) == 0 ) {
		socketFromId = 0;
		socketToId = this->sockets.slaves.size();
	} else if ( sscanf( tmp, "%u", &socketFromId ) != 1 ) {
		fprintf( stderr, "Invalid socket ID.\n" );
		return;
	} else if ( socketFromId >= this->sockets.slaves.size() ) {
		fprintf( stderr, "The specified socket ID exceeds the range [0-%lu].\n", this->sockets.slaves.size() - 1 );
		return;
	} else {
		socketToId = socketFromId + 1;
	}

	for ( uint32_t socketId = socketFromId; socketId < socketToId; socketId++ ) {
		ServerSocket *socket = this->sockets.slaves.values[ socketId ];
		if ( ! socket ) {
			fprintf( stderr, "Unknown socket ID!\n" );
			return;
		}

		event.reqReleaseDegradedLock( socket, 0, 0, 0 );
		this->eventQueue.insert( event );

		// printf( "Sending release degraded locks request to: (#%u) ", socketId );
		// socket->printAddress();
		// printf( "\n" );
	}
}

void Coordinator::releaseDegradedLock( struct sockaddr_in slave, pthread_mutex_t *lock, pthread_cond_t *cond, bool *done ) {
	uint32_t index = 0;
	ServerSocket *socket;
	for ( uint32_t i = 0, len = this->sockets.slaves.size(); i < len; i++ ) {
		socket = this->sockets.slaves.values[ i ];
		if ( ! socket )
			continue;

		if ( socket->equal( slave.sin_addr.s_addr, slave.sin_port ) ) {
			index = i;
			break;
		} else {
			socket = 0;
		}
	}

	if ( ! socket ) {
		__ERROR__( "Coordinator", "releaseDegradedLock", "Cannot find socket." );
		return;
	}

	SlaveEvent event;
	event.reqReleaseDegradedLock( socket, lock, cond, done );
	this->eventQueue.insert( event );

	printf( "Sending release degraded locks request to: (#%u) ", index );
	socket->printAddress();
	printf( "\n" );
}

void Coordinator::syncRemappedData( struct sockaddr_in slaveAddr, pthread_mutex_t *lock, pthread_cond_t *cond, bool *done ) {
	CoordinatorEvent event;
	event.syncRemappedData( slaveAddr, lock, cond, done );
	this->eventQueue.insert( event );
}

double Coordinator::getElapsedTime() {
	return get_elapsed_time( this->startTime );
}

void Coordinator::info( FILE *f ) {
	this->config.global.print( f );
	this->config.coordinator.print( f );
	this->stripeList->print( f );
}

void Coordinator::debug( FILE *f ) {
	int i, len;

	fprintf( f, "Coordinator socket\n------------------\n" );
	this->sockets.self.print( f );

	fprintf( f, "\nMaster sockets\n--------------\n" );
	for ( i = 0, len = this->sockets.masters.size(); i < len; i++ ) {
		fprintf( f, "%d. ", i + 1 );
		this->sockets.masters[ i ]->print( f );
	}
	if ( len == 0 ) fprintf( f, "(None)\n" );

	fprintf( f, "\nSlave sockets\n-------------\n" );
	for ( i = 0, len = this->sockets.slaves.size(); i < len; i++ ) {
		fprintf( f, "%d. ", i + 1 );
		this->sockets.slaves[ i ]->print( f );
	}
	if ( len == 0 ) fprintf( f, "(None)\n" );

	fprintf( f, "\nBackup slave sockets\n-------------\n" );
	for ( i = 0, len = this->sockets.backupSlaves.size(); i < len; i++ ) {
		fprintf( f, "%d. ", i + 1 );
		this->sockets.backupSlaves[ i ]->print( f );
	}
	if ( len == 0 ) fprintf( f, "(None)\n" );

	fprintf( f, "\nCoordinator event queue\n-----------------------\n" );
	this->eventQueue.print( f );

	fprintf( f, "\nWorkers\n-------\n" );
	for ( i = 0, len = this->workers.size(); i < len; i++ ) {
		fprintf( f, "%d. ", i + 1 );
		this->workers[ i ].print( f );
	}

	fprintf( f, "\nOther threads\n--------------\n" );
	this->sockets.self.printThread();

	if ( this->config.global.remap.enabled ) {
		fprintf( f, "\nRemapping handler event queue\n------------------\n" );
		this->remapMsgHandler->eventQueue->print();
	}

	fprintf( f, "\n" );
}

void Coordinator::interactive() {
	char buf[ 4096 ];
	char *command;
	bool valid;
	int i, len;

	this->help();
	while( this->isRunning ) {
		valid = false;
		printf( "> " );
		fflush( stdout );
		if ( ! fgets( buf, sizeof( buf ), stdin ) ) {
			printf( "\n" );
			break;
		}

		// Trim
		len = strnlen( buf, sizeof( buf ) );
		for ( i = len - 1; i >= 0; i-- ) {
			if ( isspace( buf[ i ] ) )
				buf[ i ] = '\0';
			else
				break;
		}

		command = buf;
		while( isspace( command[ 0 ] ) ) {
			command++;
		}
		if ( strlen( command ) == 0 )
			continue;

		if ( strcmp( command, "help" ) == 0 ) {
			valid = true;
			this->help();
		} else if ( strcmp( command, "exit" ) == 0 ) {
			break;
		} else if ( strcmp( command, "info" ) == 0 ) {
			valid = true;
			this->info();
		} else if ( strcmp( command, "debug" ) == 0 ) {
			valid = true;
			this->debug();
		} else if ( strcmp( command, "id" ) == 0 ) {
			valid = true;
			this->printInstanceId();
		} else if ( strcmp( command, "hash" ) == 0 ) {
			valid = true;
			this->hash();
		} else if ( strcmp( command, "lookup" ) == 0 ) {
			valid = true;
			this->lookup();
		} else if ( strcmp( command, "stripe" ) == 0 ) {
			valid = true;
			this->stripe();
		} else if ( strcmp( command, "dump" ) == 0 ) {
			valid = true;
			this->dump();
		} else if ( strcmp( command, "remapping" ) == 0 ) {
			valid = true;
			this->printRemapping();
		} else if ( strcmp( command, "pending" ) == 0 ) {
			valid = true;
			this->printPending();
		} else if ( strcmp( command, "time" ) == 0 ) {
			valid = true;
			this->time();
		} else if ( strcmp( command, "seal" ) == 0 ) {
			valid = true;
			this->seal();
		} else if ( strcmp( command, "metadata" ) == 0 ) {
			valid = true;
			this->metadata();
		} else if ( strcmp( command, "flush" ) == 0 ) {
			valid = true;
			this->flush();
		} else if ( strcmp( command, "log" ) == 0 ) {
			valid = true;
			this->printLog();
		} else if ( strcmp( command, "release" ) == 0 ) {
			valid = true;
			this->releaseDegradedLock();
		} else if ( strcmp( command, "remapMigrate" ) == 0 ) {
			for ( uint32_t i = 0; i < this->sockets.slaves.size(); i++ ){
				this->syncRemappedData(
					this->sockets.slaves[ i ]->getAddr(),
					0, 0, 0
				);
			}
			valid = true;
		} else if ( strcmp( command, "overload" ) == 0 ) {
			this->setSlave( true );
			valid = true;
		} else if ( strcmp( command, "underload" ) == 0 ) {
			this->setSlave( false );
			valid = true;
		} else if ( strcmp( command, "manual" ) == 0 ) {
			this->switchToManualOverload();
			valid = true;
		} else if ( strcmp( command, "auto" ) == 0 ) {
			this->switchToAutoOverload();
			valid = true;
		} else {
			valid = false;
		}

		if ( ! valid ) {
			fprintf( stderr, "Invalid command!\n" );
		}
	}
}

void Coordinator::dump() {
	FILE *f = stdout;
	size_t numKeys = 0;
	for ( size_t i = 0, len = this->sockets.slaves.size(); i < len; i++ ) {
		fprintf( f, "##### Slave #%lu: ", i + 1 );
		this->sockets.slaves[ i ]->printAddress( f );
		fprintf( f, " #####\n" );

		numKeys += this->sockets.slaves[ i ]->map.dump();
	}
	fprintf( f, "Total number of key-value pairs = %lu.\n", numKeys );

	Map::dumpDegradedLocks();
}

void Coordinator::metadata() {
	FILE *f = fopen( "coordinator.meta", "w+" );
	if ( ! f ) {
		__ERROR__( "Slave", "metadata", "Cannot write to the file \"coordinator.meta\"." );
	}

	printf( "Writing log to coordinator.log..." );
	fflush( stdout );
	for ( size_t i = 0, len = this->sockets.slaves.size(); i < len; i++ ) {
		this->sockets.slaves[ i ]->map.persist( f );
	}
	fclose( f );
	printf( "Done.\n" );
}

void Coordinator::printRemapping( FILE *f ) {
	fprintf( f, "\nRemapping Records\n" );
	fprintf( f, "----------------------------------------\n" );
	this->remappingRecords.print( f );
	if ( this->config.global.remap.enabled ) {
		fprintf( f, "\nList of Tracking Slaves\n" );
		fprintf( f, "----------------------------------------\n" );
		this->remapMsgHandler->listAliveSlaves();
	}
}

void Coordinator::printPending( FILE *f ) {
	fprintf( f, "\nPending\n" );
	fprintf( f, "----------------------------------------\n" );
	fprintf( f, "\nList of Sync Metadata Requests\n" );
	fprintf( f, "----------------------------------------\n" );
	this->pending.printSyncMetaRequests( f );
	fprintf( f, "\nList of Remapping Record Counters \n" );
	fprintf( f, "----------------------------------------\n" );
	this->pending.printSyncRemappingRecords( f );
}

void Coordinator::printInstanceId( FILE *f ) {
	fprintf( f, "Instance ID = %u\n", Coordinator::instanceId );
}

void Coordinator::help() {
	fprintf(
		stdout,
		"Supported commands:\n"
		"- help: Show this help message\n"
		"- info: Show configuration\n"
		"- debug: Show debug messages\n"
		"- id: Print instance ID\n"
		"- time: Show elapsed time\n"
		"- hash: Show the stripe list hashed by an input key\n"
		"- lookup: Search for the metadata of an input key\n"
		"- stripe: Query the seal status of a stripe\n"
		"- seal: Force all slaves to seal all its chunks\n"
		"- flush: Force all slaves to flush all its chunks\n"
		"- log: Write the log to coordinator.log\n"
		"- release: Release degraded locks at the specified socket\n"
		"- metadata: Write metadata to disk\n"
		"- remapping: Show remapping info\n"
		"- remapRecordSync: Force all remapping records to masters\n"
		"- remapMigrate: Force all remapped kv to migrate\n"
		"- overload: Force a slave to overload ( normal > degraded )\n"
		"- underload: Force a slave to underload ( degraded > normal) \n"
		"- manual: Switch to overload slaves manually\n"
		"- auto: Switch to detect overload slaves using loading statistics\n"
		"- exit: Terminate this client\n"
	);
	fflush( stdout );
}

void Coordinator::time() {
	fprintf( stdout, "Elapsed time (s): %12.6lf\n", this->getElapsedTime() );
	fflush( stdout );
}

void Coordinator::hash() {
	char key[ 256 ];
	uint8_t keySize;
	uint32_t listId, dataChunkId, dataChunkCount, parityChunkCount;

	printf( "Input key: " );
	fflush( stdout );
	if ( ! fgets( key, sizeof( key ), stdin ) ) {
		fprintf( stderr, "Invalid input!\n" );
		return;
	}
	keySize = ( uint8_t ) strnlen( key, sizeof( key ) ) - 1;

	dataChunkCount = this->config.global.coding.params.getDataChunkCount();
	parityChunkCount = this->config.global.coding.params.getParityChunkCount();

	ServerSocket **dataServerSockets = new ServerSocket *[ dataChunkCount ];
	ServerSocket **parityServerSockets = new ServerSocket *[ parityChunkCount ];

	listId = this->stripeList->get( key, keySize, dataServerSockets, parityServerSockets, &dataChunkId, true );

	printf( "\n--- Hashed to List #%u ---\n", listId );
	for ( uint32_t i = 0; i < dataChunkCount; i++ ) {
		printf( "[ %u]: ", i );
		dataServerSockets[ i ]->printAddress();
		if ( i == dataChunkId )
			printf( " ***" );
		printf( "\n" );
	}
	for ( uint32_t i = 0; i < parityChunkCount; i++ ) {
		printf( "[p%u]: ", ( i + dataChunkCount ) );
		parityServerSockets[ i ]->printAddress();
		printf( " ***\n" );
	}

	delete[] dataServerSockets;
	delete[] parityServerSockets;
}

void Coordinator::lookup() {
	char key[ 256 ];
	uint8_t keySize;

	printf( "Input key: " );
	fflush( stdout );
	if ( ! fgets( key, sizeof( key ), stdin ) ) {
		fprintf( stderr, "Invalid input!\n" );
		return;
	}
	keySize = ( uint8_t ) strnlen( key, sizeof( key ) ) - 1;

	Metadata metadata;
	ServerSocket *dataServerSocket;
	this->stripeList->get( key, keySize, &dataServerSocket );

	if ( dataServerSocket->map.findMetadataByKey( key, keySize, metadata ) ) {
		printf( "Metadata: (%u, %u, %u); Is sealed? %s\n", metadata.listId, metadata.stripeId, metadata.chunkId, dataServerSocket->map.isSealed( metadata ) ? "yes" : "no" );

		Key k;
		RemappingRecord remappingRecord;
		k.set( keySize, key );
		if ( this->remappingRecords.find( k, &remappingRecord ) ) {
			printf( "Remapping record found: " );
			for ( uint32_t i = 0; i < remappingRecord.remappedCount; i++ ) {
				printf(
					"%s(%u, %u) |-> (%u, %u)%s",
					i == 0 ? "" : "; ",
					remappingRecord.original[ i * 2     ],
					remappingRecord.original[ i * 2 + 1 ],
					remappingRecord.remapped[ i * 2     ],
					remappingRecord.remapped[ i * 2 + 1 ],
					i == remappingRecord.remappedCount - 1 ? "\n" : ""
				);
			}
		}

		DegradedLock degradedLock;
		if ( dataServerSocket->map.findDegradedLock( metadata.listId, metadata.stripeId, degradedLock ) ) {
			printf( "Degraded lock found: " );
			for ( uint32_t i = 0; i < degradedLock.reconstructedCount; i++ ) {
				printf(
					"%s(%u, %u) |-> (%u, %u) (ongoing: %u)%s",
					i == 0 ? "" : "; ",
					degradedLock.original[ i * 2     ],
					degradedLock.original[ i * 2 + 1 ],
					degradedLock.reconstructed[ i * 2     ],
					degradedLock.reconstructed[ i * 2 + 1 ],
					degradedLock.ongoingAtChunk,
					i == degradedLock.reconstructedCount - 1 ? "\n" : ""
				);
			}
		}
	} else {
		printf( "Key not found.\n" );
	}
}

void Coordinator::stripe() {
	ServerSocket *s;
	uint32_t chunkCount, dataChunkCount;
	Metadata metadata;

	printf( "Input list ID & stripe ID: " );
	fflush( stdout );

	if ( fscanf( stdin, "%u %u", &metadata.listId, &metadata.stripeId ) != 2 ) {
		fprintf( stderr, "Invalid input!\n" );
		return;
	}

	chunkCount = this->config.global.coding.params.getChunkCount();
	dataChunkCount = this->config.global.coding.params.getDataChunkCount();
	for ( uint32_t i = 0; i < chunkCount; i++ ) {
		s = this->stripeList->get( metadata.listId, i );
		metadata.chunkId = i;
		printf(
			"\t(%u, %u, %s%u): %s\n",
			metadata.listId, metadata.stripeId,
			i >= dataChunkCount ? "p" : "", i,
			s->map.isSealed( metadata ) ? "sealed" : "not sealed"
		);
	}
}

void Coordinator::appendLog( Log log ) {
	log.setTimestamp( this->getElapsedTime() );
	LOCK( &this->log.lock );
	this->log.items.push_back( log );
	UNLOCK( &this->log.lock );
}

void Coordinator::printLog() {
	FILE *f = fopen( "coordinator.log", "w" );
	if ( ! f ) {
		fprintf( stderr, "Cannot write to coordinator.log.\n" );
		return;
	}

	LOCK( &this->log.lock );
	for ( size_t i = 0, len = this->log.items.size(); i < len; i++ ) {
		this->log.items[ i ].print( f );
	}
	UNLOCK( &this->log.lock );

	fclose( f );
}

void Coordinator::seal() {
	SlaveEvent event;
	size_t count = 0;
	for ( size_t i = 0, len = this->sockets.slaves.size(); i < len; i++ ) {
		if ( this->sockets.slaves[ i ]->ready() ) {
			event.reqSealChunks( this->sockets.slaves[ i ] );
			this->eventQueue.insert( event );
			count++;
		}
	}
	printf( "Sending seal requests to %lu slaves...\n", count );
}

void Coordinator::flush() {
	SlaveEvent event;
	size_t count = 0;
	for ( size_t i = 0, len = this->sockets.slaves.size(); i < len; i++ ) {
		if ( this->sockets.slaves[ i ]->ready() ) {
			event.reqFlushChunks( this->sockets.slaves[ i ] );
			this->eventQueue.insert( event );
			count++;
		}
	}
	printf( "Sending flush requests to %lu slaves...\n", count );
}

void Coordinator::setSlave( bool overloaded ) {
	int socket, i, len;
	MasterEvent event;

	printf( "\nSlave sockets\n-------------\n" );
	for ( i = 0, len = this->sockets.slaves.size(); i < len; i++ ) {
		printf( "%d. ", i + 1 );
		this->sockets.slaves[ i ]->print( stdout );
	}
	if ( len == 0 ) printf( "(None)\n" );

	printf( "Which slave to %s (socket fd, enter 0 to exit) ? ", overloaded ? "overload" : "underload" );
	fflush( stdout );
	std::set<struct sockaddr_in> slaves;
	ServerSocket *s = 0;
	while ( scanf( "%u", &socket) == 1 ) {
		s = this->sockets.slaves.get( socket );
		if ( ! s ) break;

		slaves.insert( s->getAddr() );
		printf( "Added slave " );
		s->printAddress();
		printf( "\nWhich slave to %s (socket fd, enter 0 to exit) ? ", overloaded ? "overload" : "underload" );
		fflush( stdout );
	}
	if ( this->config.global.remap.manual == 0 )
		printf( "\nWARNING: Not in manual state for setting overloaded slaves.\n" );
	event.switchPhase( overloaded, slaves, false /* isCrushed */, true /* isforced */ );
	this->eventQueue.insert( event );
}

void Coordinator::switchToManualOverload() {
	this->config.global.remap.manual = true;
}

void Coordinator::switchToAutoOverload() {
	this->config.global.remap.manual = false;
}
