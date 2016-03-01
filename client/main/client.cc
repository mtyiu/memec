#include <cstring>
#include <ctype.h>
#include "client.hh"
#include "../remap/basic_remap_scheme.hh"

uint16_t Master::instanceId;

Master::Master() {
	this->isRunning = false;
	/* Set debug flag */
	this->debugFlags.isDegraded = false;

	Master::instanceId = 0;
}

void Master::updateSlavesCurrentLoading() {
	int index = -1;
	Latency *tempLatency = NULL;

	LOCK( &this->slaveLoading.lock );

#define UPDATE_LATENCY( _SRC_, _DST_, _SRC_VAL_TYPE_, _DST_OP_, _CHECK_EXIST_, _MIRROR_DST_ ) \
	for ( uint32_t i = 0; i < _SRC_.size(); i++ ) { \
		struct sockaddr_in &slaveAddr = _SRC_.keys[ i ]; \
		_SRC_VAL_TYPE_ *srcLatency = _SRC_.values[ i ]; \
		Latency *dstLatency = NULL; \
		if ( _CHECK_EXIST_ ) { \
			dstLatency = _DST_.get( slaveAddr, &index ); \
		} else { \
			index = -1; \
		} \
		if ( index == -1 ) { \
			tempLatency = new Latency(); \
			tempLatency->set( *srcLatency ); \
			_DST_.set( slaveAddr, tempLatency ); \
			if ( _MIRROR_DST_ ) \
				_MIRROR_DST_->set( slaveAddr, new Latency( tempLatency ) ); \
		} else { \
			dstLatency->_DST_OP_( *srcLatency ); \
			if ( _MIRROR_DST_ ) \
				_MIRROR_DST_->values[ index ]->set( *dstLatency ); \
		} \
	}

	ArrayMap< struct sockaddr_in, std::set< Latency > > &pastGet = this->slaveLoading.past.get;
	ArrayMap< struct sockaddr_in, Latency > &currentGet = this->slaveLoading.current.get;
	ArrayMap< struct sockaddr_in, std::set< Latency > > &pastSet = this->slaveLoading.past.set;
	ArrayMap< struct sockaddr_in, Latency > &currentSet = this->slaveLoading.current.set;
	ArrayMap< struct sockaddr_in, Latency > *tmp = NULL;
	//fprintf( stderr, "past %lu %lu current %lu %lu\n", pastGet.size(), pastSet.size(), currentGet.size(), currentSet.size() );

	// reset the current stats before update
	currentGet.clear();
	currentSet.clear();

	// GET
	UPDATE_LATENCY( pastGet, currentGet, std::set<Latency>, set, false, tmp );

	// SET
	UPDATE_LATENCY( pastSet, currentSet, std::set<Latency>, set, false, tmp );

	// reset past stats
	pastGet.clear();
	pastSet.clear();

	UNLOCK( &this->slaveLoading.lock );
}

void Master::updateSlavesCumulativeLoading () {
	int index = -1;
	Latency *tempLatency = NULL;

	LOCK( &this->slaveLoading.lock );

	ArrayMap< struct sockaddr_in, Latency > &currentGet = this->slaveLoading.current.get;
	ArrayMap< struct sockaddr_in, Latency > &cumulativeGet = this->slaveLoading.cumulative.get;
	ArrayMap< struct sockaddr_in, Latency > *cumulativeMirrorGet = &this->slaveLoading.cumulativeMirror.get;
	ArrayMap< struct sockaddr_in, Latency > &currentSet = this->slaveLoading.current.set;
	ArrayMap< struct sockaddr_in, Latency > &cumulativeSet = this->slaveLoading.cumulative.set;
	ArrayMap< struct sockaddr_in, Latency > *cumulativeMirrorSet = &this->slaveLoading.cumulativeMirror.set;
	//fprintf( stderr, "cumulative %lu %lu current %lu %lu\n", cumulativeGet.size(), cumulativeSet.size(), currentGet.size(), currentSet.size() );

	// GET
	UPDATE_LATENCY( currentGet, cumulativeGet, Latency, aggregate, true, cumulativeMirrorGet );

	// SET
	UPDATE_LATENCY( currentSet, cumulativeSet, Latency, aggregate, true, cumulativeMirrorSet );

#undef UPDATE_LATENCY

	UNLOCK( &this->slaveLoading.lock );
}

void Master::mergeSlaveCumulativeLoading ( ArrayMap< struct sockaddr_in, Latency > *getLatency, ArrayMap< struct sockaddr_in, Latency> *setLatency ) {

	LOCK( &this->slaveLoading.lock );

	int index = -1;
	// check if the slave addr already exists in currentMap
	// if not, update the cumulativeMap directly
	// otherwise, ignore it
#define MERGE_AND_UPDATE_LATENCY( _SRC_, _MERGE_DST_, _CHECK_EXIST_, _MIRROR_DST_ ) \
	for ( uint32_t i = 0; i < _SRC_->size(); i++ ) { \
		_CHECK_EXIST_.get( _SRC_->keys[ i ], &index ); \
		if ( index != -1 ) \
			continue; \
		_MERGE_DST_.get( _SRC_->keys[ i ], &index ); \
		if ( index == -1 ) { \
			_MERGE_DST_.set( _SRC_->keys[ i ], _SRC_->values[ i ] ); \
			_MIRROR_DST_.set( _SRC_->keys[ i ], new Latency( _SRC_->values[ i ] ) ); \
		} else { \
			_MERGE_DST_.values[ index ]->aggregate( _SRC_->values[ i ] ); \
			_MIRROR_DST_.values[ index ]->set( _MERGE_DST_.values[ index ] ); \
			delete _SRC_->values[ i ]; \
		} \
	}
	MERGE_AND_UPDATE_LATENCY( getLatency, this->slaveLoading.cumulative.get, this->slaveLoading.current.get, this->slaveLoading.cumulativeMirror.get );
	MERGE_AND_UPDATE_LATENCY( setLatency, this->slaveLoading.cumulative.set, this->slaveLoading.current.set, this->slaveLoading.cumulativeMirror.set );

#undef MERGE_AND_UPDATE_LATENCY

	UNLOCK( &this->slaveLoading.lock );
}

void Master::free() {
	this->idGenerator.free();
	this->eventQueue.free();
	delete this->stripeList;
}

void Master::signalHandler( int signal ) {
	//Signal::setHandler();
	Master *master = Master::getInstance();
	ArrayMap<int, CoordinatorSocket> &sockets = master->sockets.coordinators;
	switch ( signal ) {
		case SIGALRM:
			// update the loading stats
			master->updateSlavesCurrentLoading();
			master->updateSlavesCumulativeLoading();

			// ask workers to send the loading stats to coordinators
			LOCK( &master->slaveLoading.lock );
			CoordinatorEvent event;
			LOCK( &sockets.lock );
			for ( uint32_t i = 0; i < sockets.size(); i++ ) {
				event.reqSendLoadStats(
					sockets.values[ i ],
					&master->slaveLoading.cumulative.get,
					&master->slaveLoading.cumulative.set
				);
				master->eventQueue.insert( event );
			}
			UNLOCK( &sockets.lock );
			UNLOCK( &master->slaveLoading.lock );

			// set next update alarm
			//alarm ( master->config.master.loadingStats.updateInterval );
			break;
		default:
			master->stop();
			fclose( stdin );
			break;
	}
}

bool Master::init( char *path, OptionList &options, bool verbose ) {
	// Parse configuration files //
	if ( ( ! this->config.global.parse( path ) ) ||
	     ( ! this->config.master.merge( this->config.global ) ) ||
	     ( ! this->config.master.parse( path ) ) ||
	     ( ! this->config.master.override( options ) ) ) {
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
	/* Vectors and other sockets */
	Socket::init( &this->sockets.epoll );
	ApplicationSocket::setArrayMap( &this->sockets.applications );
	CoordinatorSocket::setArrayMap( &this->sockets.coordinators );
	SlaveSocket::setArrayMap( &this->sockets.slaves );
	// this->sockets.applications.reserve( 20000 );
	this->sockets.coordinators.reserve( this->config.global.coordinators.size() );
	for ( int i = 0, len = this->config.global.coordinators.size(); i < len; i++ ) {
		CoordinatorSocket *socket = new CoordinatorSocket();
		int fd;

		socket->init( this->config.global.coordinators[ i ], &this->sockets.epoll );
		fd = socket->getSocket();
		this->sockets.coordinators.set( fd, socket );
	}
	this->sockets.slaves.reserve( this->config.global.slaves.size() );
	for ( int i = 0, len = this->config.global.slaves.size(); i < len; i++ ) {
		SlaveSocket *socket = new SlaveSocket();
		int fd;

		socket->init( this->config.global.slaves[ i ], &this->sockets.epoll );
		fd = socket->getSocket();
		this->sockets.slaves.set( fd, socket );
	}
	/* Stripe list */
	this->stripeList = new StripeList<SlaveSocket>(
		this->config.global.coding.params.getChunkCount(),
		this->config.global.coding.params.getDataChunkCount(),
		this->config.global.stripeList.count,
		this->sockets.slaves.values
	);
	/* Workers, ID generator, packet pool and event queues */
	if ( this->config.master.workers.type == WORKER_TYPE_MIXED ) {
		this->idGenerator.init( this->config.master.workers.number.mixed );
		this->packetPool.init(
			this->config.master.pool.packets,
			Protocol::getSuggestedBufferSize(
				this->config.global.size.key,
				this->config.global.size.chunk
			)
		);
		this->eventQueue.init(
			this->config.master.eventQueue.block,
			this->config.master.eventQueue.size.mixed,
			this->config.master.eventQueue.size.pMixed
		);
		this->workers.reserve( this->config.master.workers.number.mixed );
		MasterWorker::init();
		for ( int i = 0, len = this->config.master.workers.number.mixed; i < len; i++ ) {
			this->workers.push_back( MasterWorker() );
			this->workers[ i ].init(
				this->config.global,
				WORKER_ROLE_MIXED,
				i // worker ID
			);
		}
	} else {
		this->idGenerator.init( this->config.master.workers.number.separated.total );
		this->packetPool.init(
			this->config.master.pool.packets,
			Protocol::getSuggestedBufferSize(
				this->config.global.size.key,
				this->config.global.size.chunk
			)
		);
		this->workers.reserve( this->config.master.workers.number.separated.total );
		this->eventQueue.init(
			this->config.master.eventQueue.block,
			this->config.master.eventQueue.size.separated.application,
			this->config.master.eventQueue.size.separated.coordinator,
			this->config.master.eventQueue.size.separated.master,
			this->config.master.eventQueue.size.separated.slave
		);

		int index = 0;
#define WORKER_INIT_LOOP( _FIELD_, _CONSTANT_ ) \
		for ( int i = 0, len = this->config.master.workers.number.separated._FIELD_; i < len; i++, index++ ) { \
			this->workers.push_back( MasterWorker() ); \
			this->workers[ index ].init( \
				this->config.global, \
				_CONSTANT_, \
				index \
			); \
		}

		MasterWorker::init();
		WORKER_INIT_LOOP( application, WORKER_ROLE_APPLICATION )
		WORKER_INIT_LOOP( coordinator, WORKER_ROLE_COORDINATOR )
		WORKER_INIT_LOOP( master, WORKER_ROLE_CLIENT )
		WORKER_INIT_LOOP( slave, WORKER_ROLE_SERVER )
#undef WORKER_INIT_LOOP
	}

	/* Remapping message handler; Remapping scheme */
	if ( this->config.global.remap.enabled ) {
		char masterName[ 11 ];
		memset( masterName, 0, 11 );
		sprintf( masterName, "%s%04d", CLIENT_PREFIX, this->config.master.master.addr.id );
		remapMsgHandler.init( this->config.global.remap.spreaddAddr.addr, this->config.global.remap.spreaddAddr.port, masterName );
		BasicRemappingScheme::slaveLoading = &this->slaveLoading;
		BasicRemappingScheme::overloadedSlave = &this->overloadedSlave;
		BasicRemappingScheme::stripeList = this->stripeList;
		BasicRemappingScheme::remapMsgHandler = &this->remapMsgHandler;
		// add the slave addrs to remapMsgHandler
		LOCK( &this->sockets.slaves.lock );
		for ( uint32_t i = 0; i < this->sockets.slaves.size(); i++ ) {
			remapMsgHandler.addAliveSlave( this->sockets.slaves.values[ i ]->getAddr() );
		}
		UNLOCK( &this->sockets.slaves.lock );
		//remapMsgHandler.listAliveSlaves();
	}

	/* Smoothing factor */
	Latency::smoothingFactor = this->config.global.remap.smoothingFactor;

	/* Loading statistics update */
	uint32_t sec, msec;
	if ( this->config.master.loadingStats.updateInterval > 0 ) {
		LOCK_INIT ( &this->slaveLoading.lock );
		this->slaveLoading.past.get.clear();
		this->slaveLoading.past.set.clear();
		this->slaveLoading.current.get.clear();
		this->slaveLoading.current.set.clear();
		this->slaveLoading.cumulative.get.clear();
		this->slaveLoading.cumulative.set.clear();
		sec = this->config.master.loadingStats.updateInterval / 1000;
		msec = this->config.master.loadingStats.updateInterval % 1000;
	} else {
		sec = 0;
		msec = 0;
	}
	this->statsTimer.setInterval( sec, msec );

	// Socket mapping lock //
	LOCK_INIT( &this->sockets.slavesIdToSocketLock );

	// Set signal handlers //
	Signal::setHandler( Master::signalHandler );

	// Show configuration //
	if ( verbose )
		this->info();
	return true;
}

bool Master::start() {
	bool ret = true;
	/* Workers and event queues */
	this->eventQueue.start();
	if ( this->config.master.workers.type == WORKER_TYPE_MIXED ) {
		for ( int i = 0, len = this->config.master.workers.number.mixed; i < len; i++ ) {
			this->workers[ i ].start();
		}
	} else {
		for ( int i = 0, len = this->config.master.workers.number.separated.total; i < len; i++ ) {
			this->workers[ i ].start();
		}
	}

	/* Socket */
	// Connect to coordinators
	for ( int i = 0, len = this->config.global.coordinators.size(); i < len; i++ ) {
		if ( ! this->sockets.coordinators[ i ]->start() )
			ret = false;
	}
	// Connect to slaves
	for ( int i = 0, len = this->config.global.slaves.size(); i < len; i++ ) {
		this->sockets.slaves[ i ]->timestamp.current.setVal( 0 );
		this->sockets.slaves[ i ]->timestamp.lastAck.setVal( 0 );
		if ( ! this->sockets.slaves[ i ]->start() )
			ret = false;
	}
	// Start listening
	if ( ! this->sockets.self.start() ) {
		__ERROR__( "Master", "start", "Cannot start socket." );
		ret = false;
	}

	// Wait until the instance ID is available from coordinator
	while( ret && ! this->sockets.coordinators[ 0 ]->registered ) {
		usleep(5);
	}

	// Register to slaves
	for ( int i = 0, len = this->config.global.slaves.size(); i < len; i++ ) {
		this->sockets.slaves[ i ]->registerMaster();
	}

	/* Remapping message handler */
	if ( this->config.global.remap.enabled && ! this->remapMsgHandler.start() ) {
		__ERROR__( "Master", "start", "Cannot start remapping message handler." );
		ret = false;
	}

	this->startTime = start_timer();
	this->isRunning = true;

	/* Loading statistics update */
	//fprintf( stderr, "Update loading stats every %d seconds\n", this->config.master.loadingStats.updateInterval );
	//alarm ( this->config.master.loadingStats.updateInterval );
	this->statsTimer.start();

	return ret;
}

bool Master::stop() {
	if ( ! this->isRunning )
		return false;

	int i, len;

	/* Sockets */
	printf( "Stopping self-sockets...\n" );
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
	printf( "Stopping sockets...\n" );
	for ( i = 0, len = this->sockets.applications.size(); i < len; i++ )
		this->sockets.applications[ i ]->stop();
	this->sockets.applications.clear();
	for ( i = 0, len = this->sockets.coordinators.size(); i < len; i++ )
		this->sockets.coordinators[ i ]->stop();
	this->sockets.coordinators.clear();
	for ( i = 0, len = this->sockets.slaves.size(); i < len; i++ )
		this->sockets.slaves[ i ]->stop();
	this->sockets.slaves.clear();

	 /* Remapping message handler */
	if ( this->config.global.remap.enabled ) {
		this->remapMsgHandler.stop();
		this->remapMsgHandler.quit();
	}

	/* Loading statistics update */
	//alarm ( 0 );
	statsTimer.stop();

	this->free();
	this->isRunning = false;
	printf( "\nBye.\n" );
	return true;
}

double Master::getElapsedTime() {
	return get_elapsed_time( this->startTime );
}

void Master::info( FILE *f ) {
	this->config.global.print( f );
	this->config.master.print( f );
	this->stripeList->print( f );
}

void Master::debug( FILE *f ) {
	int i, len;

	fprintf( f, "Master socket\n-------------\n" );
	this->sockets.self.print( f );

	fprintf( f, "\nApplication sockets\n-------------------\n" );
	for ( i = 0, len = this->sockets.applications.size(); i < len; i++ ) {
		fprintf( f, "%d. ", i + 1 );
		this->sockets.applications[ i ]->print( f );
	}
	if ( len == 0 ) fprintf( f, "(None)\n" );

	fprintf( f, "\nCoordinator sockets\n-------------------\n" );
	for ( i = 0, len = this->sockets.coordinators.size(); i < len; i++ ) {
		fprintf( f, "%d. ", i + 1 );
		this->sockets.coordinators[ i ]->print( f );
	}
	if ( len == 0 ) fprintf( f, "(None)\n" );

	fprintf( f, "\nSlave sockets\n-------------\n" );
	for ( i = 0, len = this->sockets.slaves.size(); i < len; i++ ) {
		fprintf( f, "%d. ", i + 1 );
		this->sockets.slaves[ i ]->print( f );
	}
	if ( len == 0 ) fprintf( f, "(None)\n" );

	fprintf( f, "\nMaster event queue\n------------------\n" );
	this->eventQueue.print( f );

	fprintf( f, "\nPacket pool\n-----------\n" );
	this->packetPool.print( f );

	fprintf( f, "\nWorkers\n-------\n" );
	for ( i = 0, len = this->workers.size(); i < len; i++ ) {
		fprintf( f, "%d. ", i + 1 );
		this->workers[ i ].print( f );
	}

	fprintf( f, "\nOther threads\n--------------\n" );
	this->sockets.self.printThread();

	fprintf( f, "\n" );
}

void Master::interactive() {
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
		} else if ( strcmp( command, "pending" ) == 0 ) {
			valid = true;
			this->printPending();
		} else if ( strcmp( command, "remapping" ) == 0 ) {
			valid = true;
			this->printRemapping();
		} else if ( strcmp( command, "backup" ) == 0 ) {
			valid = true;
			this->printBackup();
		} else if ( strcmp( command, "metadata" ) == 0 ) {
			valid = true;
			this->syncMetadata();
		} else if ( strncmp( command, "set", 3 ) == 0 ) {
			valid = this->setDebugFlag( command );
		} else if ( strcmp( command, "time" ) == 0 ) {
			valid = true;
			this->time();
		} else if ( strcmp( command, "ackparity" ) == 0 ) {
			pthread_cond_t condition;
			pthread_mutex_t lock;
			uint32_t counter = 0;

			pthread_cond_init( &condition, 0 );
			pthread_mutex_init( &lock, 0 );
			this->ackParityDelta( stdout, 0, &condition, &lock, &counter, true );
			// wait for all ack, skip waiting if there is nothing to revert
			LOCK( &lock );
			if ( counter > 0 ) {
				printf( "waiting for %u acknowledgements with counter at %p\n", counter, &counter );
				UNLOCK( &lock );
				pthread_cond_wait( &condition, &lock );
			}
			valid = true;
		} else if ( strcmp( command, "revertparity" ) == 0 ) {
			pthread_cond_t condition;
			pthread_mutex_t lock;
			uint32_t counter = 0;

			pthread_cond_init( &condition, 0 );
			pthread_mutex_init( &lock, 0 );

			this->revertDelta( stdout, 0, &condition, &lock, &counter, true );
			// wait for all ack, skip waiting if there is nothing to revert
			LOCK( &lock );
			if ( counter > 0 ) {
				printf( "waiting for %u acknowledgements with counter at %p\n", counter, &counter );
				UNLOCK( &lock );
				pthread_cond_wait( &condition, &lock );
			}
			valid = true;
		} else if ( strcmp( command, "replay" ) == 0 ) {
			// FOR REPLAY TESTING ONLY
			for ( int i = 0, len = this->sockets.slaves.size(); i < len; i ++ ) {
				printf("Prepare replay for slave id = %hu fd = %u\n", this->sockets.slaves[ i ]->instanceId, this->sockets.slaves[ i ]->getSocket() );
				MasterWorker::replayRequestPrepare( this->sockets.slaves[ i ] );
			}
			for ( int i = 0, len = this->sockets.slaves.size(); i < len; i ++ ) {
				printf("Replay for slave id = %hu fd = %u\n", this->sockets.slaves[ i ]->instanceId, this->sockets.slaves[ i ]->getSocket() );
				MasterWorker::replayRequest( this->sockets.slaves[ i ] );
			}
			valid = true;
		} else {
			valid = false;
		}

		if ( ! valid ) {
			fprintf( stderr, "Invalid command!\n" );
		}
	}
}

bool Master::setDebugFlag( char *input ) {
	bool ret = true;
	char *token = 0, *key = 0, *value = 0;
	int numOfTokens;

	for ( numOfTokens = 0; numOfTokens < 3; numOfTokens++ ) {
		token = strtok( ( token == NULL ? input : NULL ), " " );
		if ( token ) {
			if ( strlen( token ) == 0 )
				continue;
		} else {
			return false;
		}

		switch( numOfTokens ) {
			case 1:
				key = token;
				break;
			case 2:
				value = token;
				break;
			default:
				break;
		}
	}

	if ( strcmp( key, "degraded" ) == 0 ) {
		this->debugFlags.isDegraded = ( strcmp( value, "true" ) == 0 );
	} else {
		ret = false;
	}
	return ret;
}

bool Master::isDegraded( SlaveSocket *socket ) {
	return (
		( this->debugFlags.isDegraded )
		||
		(
			this->remapMsgHandler.useCoordinatedFlow( socket->getAddr() ) &&
			! this->config.master.degraded.disabled
		)
	);
}

void Master::printInstanceId( FILE *f ) {
	fprintf( f, "Instance ID = %u\n", Master::instanceId );
}

void Master::printPending( FILE *f ) {
	size_t i;
	std::unordered_multimap<PendingIdentifier, Key>::iterator it;
	std::unordered_multimap<PendingIdentifier, KeyValue>::iterator keyValueIt;
	std::unordered_multimap<PendingIdentifier, KeyValueUpdate>::iterator keyValueUpdateIt;

	LOCK( &this->pending.applications.setLock );
	fprintf(
		f,
		"Pending requests for applications\n"
		"---------------------------------\n"
		"[SET] Pending: %lu\n",
		this->pending.applications.set.size()
	);
	i = 1;
	for (
		keyValueIt = this->pending.applications.set.begin();
		keyValueIt != this->pending.applications.set.end();
		keyValueIt++, i++
	) {
		const PendingIdentifier &pid = keyValueIt->first;
		KeyValue &keyValue = keyValueIt->second;
		Key key = keyValue.key();
		fprintf(
			f, "%lu. ID: (%u, %u); Key: %.*s (size = %u); Timestamp: %u source: ",
			i, keyValueIt->first.instanceId, keyValueIt->first.requestId,
			key.size, key.data, key.size,
			keyValueIt->first.timestamp
		);
		if ( pid.ptr )
			( ( Socket * ) pid.ptr )->printAddress( f );
		else
			fprintf( f, "[N/A]\n" );

		for ( uint8_t i = 0; i < key.size; i++ ) {
			if ( ! isprint( key.data[ i ] ) ) {
				fprintf( f, " %u", i );
			}
		}

		fprintf( f, "\n" );
	}
	UNLOCK( &this->pending.applications.setLock );

	LOCK( &this->pending.applications.getLock );
	fprintf(
		f,
		"\n[GET] Pending: %lu\n",
		this->pending.applications.get.size()
	);
	i = 1;
	for (
		it = this->pending.applications.get.begin();
		it != this->pending.applications.get.end();
		it++, i++
	) {
		const PendingIdentifier &pid = it->first;
		const Key &key = it->second;
		fprintf(
			f, "%lu. ID: (%u, %u); Key: %.*s (size = %u); source: ",
			i, it->first.instanceId, it->first.requestId,
			key.size, key.data, key.size
		);
		if ( pid.ptr )
			( ( Socket * ) pid.ptr )->printAddress( f );
		else
			fprintf( f, "(nil)\n" );
		fprintf( f, "\n" );
	}
	UNLOCK( &this->pending.applications.getLock );

	LOCK( &this->pending.applications.updateLock );
	fprintf(
		f,
		"\n[UPDATE] Pending: %lu\n",
		this->pending.applications.update.size()
	);
	i = 1;
	for (
		keyValueUpdateIt = this->pending.applications.update.begin();
		keyValueUpdateIt != this->pending.applications.update.end();
		keyValueUpdateIt++, i++
	) {
		const PendingIdentifier &pid = keyValueUpdateIt->first;
		const KeyValueUpdate &keyValueUpdate = keyValueUpdateIt->second;
		fprintf(
			f, "%lu. ID: (%u, %u); Key: %.*s (size = %u, offset = %u, length = %u); source: ",
			i, keyValueUpdateIt->first.instanceId, keyValueUpdateIt->first.requestId,
			keyValueUpdate.size, keyValueUpdate.data, keyValueUpdate.size,
			keyValueUpdate.offset, keyValueUpdate.length
		);
		if ( pid.ptr )
			( ( Socket * ) pid.ptr )->printAddress( f );
		else
			fprintf( f, "(nil)\n" );
		fprintf( f, "\n" );
	}
	UNLOCK( &this->pending.applications.updateLock );

	LOCK( &this->pending.applications.delLock );
	fprintf(
		f,
		"\n[DELETE] Pending: %lu\n",
		this->pending.applications.del.size()
	);
	i = 1;
	for (
		it = this->pending.applications.del.begin();
		it != this->pending.applications.del.end();
		it++, i++
	) {
		const PendingIdentifier &pid = it->first;
		const Key &key = it->second;
		fprintf(
			f, "%lu. ID: (%u, %u); Key: %.*s (size = %u); source: ",
			i, it->first.instanceId, it->first.requestId,
			key.size, key.data, key.size
		);
		if ( pid.ptr )
			( ( Socket * ) pid.ptr )->printAddress( f );
		else
			fprintf( f, "(nil)\n" );
		fprintf( f, "\n" );
	}
	UNLOCK( &this->pending.applications.delLock );

	LOCK( &this->pending.slaves.setLock );
	fprintf(
		f,
		"\n\nPending requests for slaves\n"
		"---------------------------\n"
		"[SET] Pending: %lu\n",
		this->pending.slaves.set.size()
	);

	i = 1;
	for (
		it = this->pending.slaves.set.begin();
		it != this->pending.slaves.set.end();
		it++, i++
	) {
		const PendingIdentifier &pid = it->first;
		const Key &key = it->second;
		fprintf(
			f, "%lu. ID: (%u, %u), parent ID: (%u, %u); Key: %.*s (size = %u); target: ",
			i, it->first.instanceId, it->first.requestId,
			it->first.parentInstanceId, it->first.parentRequestId,
			key.size, key.data, key.size
		);
		if ( pid.ptr )
			( ( Socket * ) pid.ptr )->printAddress( f );
		else
			fprintf( f, "[N/A]" );
		fprintf( f, "\n" );
	}
	UNLOCK( &this->pending.slaves.setLock );

	LOCK( &this->pending.slaves.getLock );
	fprintf(
		f,
		"\n[GET] Pending: %lu\n",
		this->pending.slaves.get.size()
	);
	i = 1;
	for (
		it = this->pending.slaves.get.begin();
		it != this->pending.slaves.get.end();
		it++, i++
	) {
		const PendingIdentifier &pid = it->first;
		const Key &key = it->second;
		fprintf(
			f, "%lu. ID: (%u, %u), parent ID: (%u, %u); Key: %.*s (size = %u); target: ",
			i, it->first.instanceId, it->first.requestId,
			it->first.parentInstanceId, it->first.parentRequestId,
			key.size, key.data, key.size
		);
		( ( Socket * ) pid.ptr )->printAddress( f );
		fprintf( f, "\n" );
	}
	UNLOCK( &this->pending.slaves.getLock );

	LOCK( &this->pending.slaves.updateLock );
	fprintf(
		f,
		"\n[UPDATE] Pending: %lu\n",
		this->pending.slaves.update.size()
	);
	i = 1;
	for (
		keyValueUpdateIt = this->pending.slaves.update.begin();
		keyValueUpdateIt != this->pending.slaves.update.end();
		keyValueUpdateIt++, i++
	) {
		const PendingIdentifier &pid = keyValueUpdateIt->first;
		const KeyValueUpdate &keyValueUpdate = keyValueUpdateIt->second;
		fprintf(
			f, "%lu. ID: (%u, %u), parent ID: (%u, %u); Key: %.*s (size = %u, offset = %u, length = %u); target: ",
			i, keyValueUpdateIt->first.instanceId, keyValueUpdateIt->first.requestId,
			keyValueUpdateIt->first.parentInstanceId, keyValueUpdateIt->first.parentRequestId,
			keyValueUpdate.size, keyValueUpdate.data, keyValueUpdate.size,
			keyValueUpdate.offset, keyValueUpdate.length
		);
		if ( pid.ptr )
			( ( Socket * ) pid.ptr )->printAddress( f );
		else
			fprintf( f, "(nil)\n" );
		fprintf( f, "\n" );
	}
	UNLOCK( &this->pending.slaves.updateLock );

	LOCK( &this->pending.slaves.delLock );
	fprintf(
		f,
		"\n[DELETE] Pending: %lu\n",
		this->pending.slaves.del.size()
	);
	i = 1;
	for (
		it = this->pending.slaves.del.begin();
		it != this->pending.slaves.del.end();
		it++, i++
	) {
		const PendingIdentifier &pid = it->first;
		const Key &key = it->second;
		fprintf(
			f, "%lu. ID: (%u, %u), parent ID: (%u, %u); Key: %.*s (size = %u); target: ",
			i, it->first.instanceId, it->first.requestId,
			it->first.parentInstanceId, it->first.parentRequestId,
			key.size, key.data, key.size
		);
		( ( Socket * ) pid.ptr )->printAddress( f );
		fprintf( f, "\n" );
	}
	UNLOCK( &this->pending.slaves.delLock );

	fprintf(
		f,
		"\n[ACK] Parity backup: (remove)%10lu  (revert)%10lu\n",
		this->pending.ack.remove.size(),
		this->pending.ack.revert.size()
	);
}

void Master::printRemapping( FILE *f ) {
	fprintf(
		f,
		"\nList of Tracking Slaves\n"
		"------------------------\n"
	);
	this->remapMsgHandler.listAliveSlaves();

	fprintf(
		f,
		"\nSlaves Transit. Info\n"
		"------------------------\n"
	);
	char buf[ 16 ];
	for ( auto &info : this->remapMsgHandler.stateTransitInfo ) {
		Socket::ntoh_ip( info.first.sin_addr.s_addr, buf, 16 );
		fprintf(
			f,
			"\t%s:%hu Revert: %7u   Pending Normal: %7u\n",
			buf, ntohs( info.first.sin_port ),
			info.second.getParityRevertCounterVal(),
			info.second.getPendingRequestCount()
		);
	}
}

void Master::printBackup( FILE *f ) {
	SlaveSocket *s;
	LOCK( &this->sockets.slaves.lock );
	std::vector<SlaveSocket *> &sockets = this->sockets.slaves.values;
	for ( size_t i = 0, size = sockets.size(); i < size; i++ ) {
		s = sockets[ i ];
		s->printAddress();
		printf( ":\n" );
		s->backup.print( f );
		printf( "\n" );
		fprintf( f,
			"Timestamps: Current: %10u  Last Ack: %10u; (Pending #) Update: %10lu  Delete: %10lu\n",
			s->timestamp.current.getVal(),
			s->timestamp.lastAck.getVal(),
			s->timestamp.pendingAck._update.size(),
			s->timestamp.pendingAck._del.size()
		);
	}
	UNLOCK( &this->sockets.slaves.lock );
}

void Master::syncMetadata() {
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
		SlaveSocket *socket = this->sockets.slaves.values[ socketId ];
		if ( ! socket ) {
			fprintf( stderr, "Unknown socket ID!\n" );
			return;
		}

		event.syncMetadata( socket );
		this->eventQueue.insert( event );

		printf( "Synchronize metadata backup for the slave: (#%u) ", socketId );
		socket->printAddress();
		printf( "\n" );
	}
}

void Master::help() {
	fprintf(
		stdout,
		"Supported commands:\n"
		"- help: Show this help message\n"
		"- info: Show configuration\n"
		"- debug: Show debug messages\n"
		"- id: Print instance ID\n"
		"- pending: Show all pending requests\n"
		"- remapping: Show remapping info\n"
		"- backup: Show backup info\n"
		"- metadata: Synchronize metadata backup for a particular slave\n"
		"- set: Set debug flag (degraded = [true|false])\n"
		"- time: Show elapsed time\n"
		"- ackparity: Acknowledge parity delta\n"
		"- revertparity: Revert unacknowledged parity delta\n"
		"- exit: Terminate this client\n"
	);
	fflush( stdout );
}

void Master::time() {
	fprintf( stdout, "Elapsed time: %12.6lf s\n", this->getElapsedTime() );
	fflush( stdout );
}

#define DISPATCH_EVENT_TO_OTHER_SLAVES( _METHOD_NAME_, _S_, _COND_, _LOCK_, _COUNTER_ ) {  \
	std::vector<SlaveEvent> events; \
	for ( int j = 0, len = this->sockets.slaves.size(); j < len; j++ ) { \
		SlaveEvent event; \
		SlaveSocket *p = this->sockets.slaves[ j ]; \
		struct sockaddr_in saddr = p->getAddr(); \
		/* skip myself, and any node declared to be failed */ \
		if ( p == _S_ || this->remapMsgHandler.useCoordinatedFlow( saddr ) ) continue; \
		if ( _LOCK_ ) LOCK( _LOCK_ ); \
		if ( _COUNTER_ ) *_COUNTER_ += 1; \
		if ( _LOCK_ ) UNLOCK( _LOCK_ ); \
		if ( strcmp( #_METHOD_NAME_, "ackParityDelta" ) == 0 ) {\
			event.ackParityDelta( p, timestamps, _S_->instanceId, _COND_, _LOCK_, _COUNTER_ ); \
		} else if ( strcmp( #_METHOD_NAME_, "revertDelta" ) == 0 ) { \
			event.revertDelta( p, timestamps, requests, _S_->instanceId, _COND_, _LOCK_, _COUNTER_ ); \
		} \
		/* avoid counter going 0 (mistaken as finished) before all requests are sent */ \
		events.push_back( event ); \
	} \
	for ( uint32_t j = 0; j < events.size(); j++ ) { \
		this->eventQueue.insert( events[ j ] ); \
	} \
}

void Master::ackParityDelta( FILE *f, SlaveSocket *target, pthread_cond_t *condition, LOCK_T *lock, uint32_t *counter, bool force ) {
	uint32_t from, to, update, del;
	std::vector<uint32_t> timestamps;
	std::vector<Key> requests;

	for( int i = 0, len = this->sockets.slaves.size(); i < len; i++ ) {
		SlaveSocket *s = this->sockets.slaves[ i ];

		if ( target && target != s )
			continue;

		LOCK( &target->ackParityDeltaBackupLock );
		from = s->timestamp.lastAck.getVal();
		to = s->timestamp.current.getVal();
		del = update = to;

		LOCK( &s->timestamp.pendingAck.updateLock );
		if ( ! s->timestamp.pendingAck._update.empty() )
			update = s->timestamp.pendingAck._update.begin()->getVal() - 1;
		UNLOCK( &s->timestamp.pendingAck.updateLock );

		LOCK( &s->timestamp.pendingAck.delLock );
		if ( ! s->timestamp.pendingAck._del.empty() )
			del = s->timestamp.pendingAck._del.begin()->getVal() - 1;
		UNLOCK( &s->timestamp.pendingAck.delLock );

		/* find the small acked timestamp */
		to = update < to ? ( del < update ? del : update ) : to ;

		/* check the threshold is reached */
		if ( ! force && Timestamp( to ) - Timestamp( from ) < this->config.master.backup.ackBatchSize ) {
			UNLOCK( &target->ackParityDeltaBackupLock );
			continue;
		}

		if ( f ) {
			s->printAddress();
			fprintf( stderr, " ack parity delta for timestamps from %u to %u\n", from, to );
		}

		timestamps.clear();
		timestamps.push_back( from );
		timestamps.push_back( to );
		DISPATCH_EVENT_TO_OTHER_SLAVES( ackParityDelta, s, condition, lock, counter );

		s->timestamp.lastAck.setVal( to );
		UNLOCK( &target->ackParityDeltaBackupLock );
	}
}

bool Master::revertDelta( FILE *f, SlaveSocket *target, pthread_cond_t *condition, LOCK_T *lock, uint32_t *counter, bool force ) {
	std::vector<uint32_t> timestamps;
	std::vector<Key> requests;
	std::set<uint32_t> timestampSet;

	KeyValue keyValue, keyValueDup;
	Key key;
	char *valueStr;
	uint32_t valueSize;

	PendingIdentifier pid;

	// process one target slave at a time
	if ( ! target )
		return false;

	// ack parity delta as well, but do not wait for it to complete
	//this->ackParityDelta( f, target, 0, 0, 0, force );

	// UPDATE / DELETE
	LOCK( &target->timestamp.pendingAck.updateLock );
	for ( auto &ts : target->timestamp.pendingAck._update )
		timestampSet.insert( ts.getVal() );
	UNLOCK( &target->timestamp.pendingAck.updateLock );

	LOCK( &target->timestamp.pendingAck.delLock );
	for ( auto &ts : target->timestamp.pendingAck._del )
		timestampSet.insert( ts.getVal() );
	UNLOCK( &target->timestamp.pendingAck.delLock );

	// SET
	std::unordered_multimap<PendingIdentifier, Key>::iterator it, saveIt;
	uint32_t listIndex, chunkIndex;
	SlaveSocket *dataSlave = 0;
	LOCK ( &this->pending.slaves.setLock );
	for( it = this->pending.slaves.set.begin(), saveIt = it; it != this->pending.slaves.set.end(); it = saveIt ) {
		saveIt++;
		// skip if not pending for failed slave
		if ( it->first.ptr != target )
			continue;
		listIndex = this->stripeList->get( it->second.data, it->second.size, 0, 0, &chunkIndex );
		dataSlave = this->stripeList->get( listIndex, chunkIndex );
		// skip revert if failed slave is not the data slave, but see if the request can be immediately replied
		if ( dataSlave != target ) {
			if (
				this->pending.count( PT_SLAVE_SET, it->first.instanceId, it->first.requestId, false, false ) == 1 &&
				this->pending.eraseKeyValue( PT_APPLICATION_SET, it->first.parentInstanceId, it->first.parentRequestId, 0, &pid, &keyValue, true, true, true, it->second.data )
			) {

				keyValue.deserialize( key.data, key.size, valueStr, valueSize );
				keyValueDup.dup( key.data, key.size, valueStr, valueSize, 0 );

				// duplicate the key for reply
				if ( pid.ptr ) {
					ApplicationEvent applicationEvent;
					applicationEvent.resSet( ( ApplicationSocket * ) pid.ptr, pid.instanceId, pid.requestId, keyValueDup, true, true );
					this->eventQueue.insert( applicationEvent );
				}
			}
			this->pending.slaves.set.erase( it );
			continue;
		}
		// skip if no backup available to do the revert
		if ( ! this->pending.findKeyValue( PT_APPLICATION_SET, it->first.parentInstanceId, it->first.parentRequestId, 0, &keyValue, true, it->second.data ) )
			continue;
		// temporarily duplicate the key for revert
		key = keyValue.key();
		key.dup( key.size, key.data );
		requests.push_back( key );
	}
	UNLOCK ( &this->pending.slaves.setLock );

	// force revert all parity for testing purpose
	//for ( uint32_t i = target->timestamp.lastAck.getVal() ; i < target->timestamp.current.getVal(); i++ ) {
	//	timestampSet.insert( i );
	//}

	//if ( timestampSet.size() == 0 && requests.size() == 0 )
	//	return false;

	if ( f ) {
		target->printAddress();
		fprintf( f, " revert parity delta for timestamps from %u to %u with count = %lu\n", *timestampSet.begin(), target->timestamp.current.getVal(), timestampSet.size() );
	}
	fprintf( stdout, " revert parity delta for timestamps from %u to %u with count = %lu\n", *timestampSet.begin(), target->timestamp.current.getVal(), timestampSet.size() );

	timestamps.insert( timestamps.begin(), timestampSet.begin(), timestampSet.end() ); // ordered timestamps

	DISPATCH_EVENT_TO_OTHER_SLAVES( revertDelta, target, condition, lock, counter );

	return true;
}