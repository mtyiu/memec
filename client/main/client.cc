#include <cstring>
#include <ctype.h>
#include "client.hh"
#include "../remap/basic_remap_scheme.hh" 
uint16_t Client::instanceId;

Client::Client() {
	this->isRunning = false;
	/* Set debug flag */
	this->debugFlags.isDegraded = false;

	Client::instanceId = 0;
}

void Client::updateServersCurrentLoading() {
	int index = -1;
	Latency *tempLatency = NULL;

	LOCK( &this->serverLoading.lock );

#define UPDATE_LATENCY( _SRC_, _DST_, _SRC_VAL_TYPE_, _DST_OP_, _CHECK_EXIST_, _MIRROR_DST_ ) \
	for ( uint32_t i = 0; i < _SRC_.size(); i++ ) { \
		struct sockaddr_in &serverAddr = _SRC_.keys[ i ]; \
		_SRC_VAL_TYPE_ *srcLatency = _SRC_.values[ i ]; \
		Latency *dstLatency = NULL; \
		if ( _CHECK_EXIST_ ) { \
			dstLatency = _DST_.get( serverAddr, &index ); \
		} else { \
			index = -1; \
		} \
		if ( index == -1 ) { \
			tempLatency = new Latency(); \
			tempLatency->set( *srcLatency ); \
			_DST_.set( serverAddr, tempLatency ); \
			if ( _MIRROR_DST_ ) \
				_MIRROR_DST_->set( serverAddr, new Latency( tempLatency ) ); \
		} else { \
			dstLatency->_DST_OP_( *srcLatency ); \
			if ( _MIRROR_DST_ ) \
				_MIRROR_DST_->values[ index ]->set( *dstLatency ); \
		} \
	}

	ArrayMap< struct sockaddr_in, std::set< Latency > > &pastGet = this->serverLoading.past.get;
	ArrayMap< struct sockaddr_in, Latency > &currentGet = this->serverLoading.current.get;
	ArrayMap< struct sockaddr_in, std::set< Latency > > &pastSet = this->serverLoading.past.set;
	ArrayMap< struct sockaddr_in, Latency > &currentSet = this->serverLoading.current.set;
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

	UNLOCK( &this->serverLoading.lock );
}

void Client::updateServersCumulativeLoading () {
	int index = -1;
	Latency *tempLatency = NULL;

	LOCK( &this->serverLoading.lock );

	ArrayMap< struct sockaddr_in, Latency > &currentGet = this->serverLoading.current.get;
	ArrayMap< struct sockaddr_in, Latency > &cumulativeGet = this->serverLoading.cumulative.get;
	ArrayMap< struct sockaddr_in, Latency > *cumulativeMirrorGet = &this->serverLoading.cumulativeMirror.get;
	ArrayMap< struct sockaddr_in, Latency > &currentSet = this->serverLoading.current.set;
	ArrayMap< struct sockaddr_in, Latency > &cumulativeSet = this->serverLoading.cumulative.set;
	ArrayMap< struct sockaddr_in, Latency > *cumulativeMirrorSet = &this->serverLoading.cumulativeMirror.set;
	//fprintf( stderr, "cumulative %lu %lu current %lu %lu\n", cumulativeGet.size(), cumulativeSet.size(), currentGet.size(), currentSet.size() );

	// GET
	UPDATE_LATENCY( currentGet, cumulativeGet, Latency, aggregate, true, cumulativeMirrorGet );

	// SET
	UPDATE_LATENCY( currentSet, cumulativeSet, Latency, aggregate, true, cumulativeMirrorSet );

#undef UPDATE_LATENCY

	UNLOCK( &this->serverLoading.lock );
}

void Client::mergeServerCumulativeLoading ( ArrayMap< struct sockaddr_in, Latency > *getLatency, ArrayMap< struct sockaddr_in, Latency> *setLatency ) {

	LOCK( &this->serverLoading.lock );

	int index = -1;
	// check if the server addr already exists in currentMap
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
	MERGE_AND_UPDATE_LATENCY( getLatency, this->serverLoading.cumulative.get, this->serverLoading.current.get, this->serverLoading.cumulativeMirror.get );
	MERGE_AND_UPDATE_LATENCY( setLatency, this->serverLoading.cumulative.set, this->serverLoading.current.set, this->serverLoading.cumulativeMirror.set );

#undef MERGE_AND_UPDATE_LATENCY

	UNLOCK( &this->serverLoading.lock );
}

void Client::free() {
	this->idGenerator.free();
	this->eventQueue.free();
	delete this->stripeList;
}

void Client::signalHandler( int signal ) {
	//Signal::setHandler();
	Client *client = Client::getInstance();
	ArrayMap<int, CoordinatorSocket> &sockets = client->sockets.coordinators;
	switch ( signal ) {
		case SIGALRM:
			// update the loading stats
			client->updateServersCurrentLoading();
			client->updateServersCumulativeLoading();

			// ask workers to send the loading stats to coordinators
			LOCK( &client->serverLoading.lock );
			CoordinatorEvent event;
			LOCK( &sockets.lock );
			for ( uint32_t i = 0; i < sockets.size(); i++ ) {
				event.reqSendLoadStats(
					sockets.values[ i ],
					&client->serverLoading.cumulative.get,
					&client->serverLoading.cumulative.set
				);
				client->eventQueue.insert( event );
			}
			UNLOCK( &sockets.lock );
			UNLOCK( &client->serverLoading.lock );

			// set next update alarm
			//alarm ( client->config.global.timeout.load );
			break;
		default:
			client->stop();
			fclose( stdin );
			break;
	}
}

bool Client::init( char *path, OptionList &globalOptions, OptionList &clientOptions, bool verbose ) {
	// Parse configuration files //
	if ( ( ! this->config.global.parse( path ) ) ||
	     ( ! this->config.global.override( globalOptions ) ) ||
	     ( ! this->config.global.validate() ) ||
	     ( ! this->config.client.parse( path ) ) ||
	     ( ! this->config.client.override( clientOptions ) ) ||
	     ( ! this->config.client.validate() ) ) {
		return false;
	}

	// Initialize modules //
	/* Socket */
	if ( ! this->sockets.epoll.init(
			this->config.global.epoll.maxEvents,
			this->config.global.epoll.timeout
		) || ! this->sockets.self.init(
			this->config.client.client.addr.type,
			this->config.client.client.addr.addr,
			this->config.client.client.addr.port,
			&this->sockets.epoll
		) ) {
		__ERROR__( "Client", "init", "Cannot initialize socket." );
		return false;
	}
	/* Vectors and other sockets */
	Socket::init( &this->sockets.epoll );
	ApplicationSocket::setArrayMap( &this->sockets.applications );
	CoordinatorSocket::setArrayMap( &this->sockets.coordinators );
	ServerSocket::setArrayMap( &this->sockets.servers );
	// this->sockets.applications.reserve( 20000 );
	this->sockets.coordinators.reserve( this->config.global.coordinators.size() );
	for ( int i = 0, len = this->config.global.coordinators.size(); i < len; i++ ) {
		CoordinatorSocket *socket = new CoordinatorSocket();
		int fd;

		socket->init( this->config.global.coordinators[ i ], &this->sockets.epoll );
		fd = socket->getSocket();
		this->sockets.coordinators.set( fd, socket );
	}
	this->sockets.servers.reserve( this->config.global.servers.size() );
	for ( int i = 0, len = this->config.global.servers.size(); i < len; i++ ) {
		ServerSocket *socket = new ServerSocket();
		int fd;

		socket->init( this->config.global.servers[ i ], &this->sockets.epoll );
		fd = socket->getSocket();
		this->sockets.servers.set( fd, socket );
	}
	/* Stripe list */
	this->stripeList = new StripeList<ServerSocket>(
		this->config.global.coding.params.getChunkCount(),
		this->config.global.coding.params.getDataChunkCount(),
		this->config.global.stripeLists.count,
		this->sockets.servers.values
	);
	/* Workers, ID generator, packet pool and event queues */
	this->idGenerator.init( this->config.global.workers.count );
	this->packetPool.init(
		this->config.global.pool.packets,
		Protocol::getSuggestedBufferSize(
			this->config.global.size.key,
			this->config.global.size.chunk
		)
	);
	this->eventQueue.init(
		this->config.global.eventQueue.block,
		this->config.global.eventQueue.size,
		this->config.global.eventQueue.prioritized
	);
	this->workers.reserve( this->config.global.workers.count );
	ClientWorker::init();
	for ( int i = 0, len = this->config.global.workers.count; i < len; i++ ) {
		this->workers.push_back( ClientWorker() );
		this->workers[ i ].init(
			this->config.global,
			i // worker ID
		);
	}

	/* Remapping message handler; Remapping scheme */
	if ( ! this->config.global.states.disabled ) {
		char clientName[ 11 ];
		memset( clientName, 0, 11 );
		sprintf( clientName, "%s%04d", CLIENT_PREFIX, this->config.client.client.addr.id );
		stateTransitHandler.init( this->config.global.states.spreaddAddr.addr, this->config.global.states.spreaddAddr.port, clientName );
		BasicRemappingScheme::serverLoading = &this->serverLoading;
		BasicRemappingScheme::overloadedServer = &this->overloadedServer;
		BasicRemappingScheme::stripeList = this->stripeList;
		BasicRemappingScheme::stateTransitHandler = &this->stateTransitHandler;
		// add the server addrs to stateTransitHandler
		LOCK( &this->sockets.servers.lock );
		for ( uint32_t i = 0; i < this->sockets.servers.size(); i++ ) {
			stateTransitHandler.addAliveServer( this->sockets.servers.values[ i ]->getAddr() );
		}
		UNLOCK( &this->sockets.servers.lock );
		//stateTransitHandler.listAliveServers();
	}

	/* Smoothing factor */
	Latency::smoothingFactor = this->config.global.states.smoothingFactor;

	/* Loading statistics update */
	uint32_t sec, msec;
	if ( this->config.global.timeout.load > 0 ) {
		LOCK_INIT ( &this->serverLoading.lock );
		this->serverLoading.past.get.clear();
		this->serverLoading.past.set.clear();
		this->serverLoading.current.get.clear();
		this->serverLoading.current.set.clear();
		this->serverLoading.cumulative.get.clear();
		this->serverLoading.cumulative.set.clear();
		sec = this->config.global.timeout.load / 1000;
		msec = this->config.global.timeout.load % 1000;
	} else {
		sec = 0;
		msec = 0;
	}
	this->statsTimer.setInterval( sec, msec );

	// Socket mapping lock //
	LOCK_INIT( &this->sockets.serversIdToSocketLock );

	// Set signal handlers //
	Signal::setHandler( Client::signalHandler );

	// Show configuration //
	if ( verbose )
		this->info();
	return true;
}

bool Client::start() {
	bool ret = true;
	/* Workers and event queues */
	this->eventQueue.start();
	for ( int i = 0, len = this->config.global.workers.count; i < len; i++ ) {
		this->workers[ i ].start();
	}

	/* Socket */
	// Connect to coordinators
	for ( int i = 0, len = this->config.global.coordinators.size(); i < len; i++ ) {
		if ( ! this->sockets.coordinators[ i ]->start() )
			ret = false;
	}
	// Connect to servers
	for ( int i = 0, len = this->config.global.servers.size(); i < len; i++ ) {
		this->sockets.servers[ i ]->timestamp.current.setVal( 0 );
		this->sockets.servers[ i ]->timestamp.lastAck.setVal( 0 );
		if ( ! this->sockets.servers[ i ]->start() )
			ret = false;
	}
	// Start listening
	if ( ! this->sockets.self.start() ) {
		__ERROR__( "Client", "start", "Cannot start socket." );
		ret = false;
	}

	// Wait until the instance ID is available from coordinator
	while( ret && ! this->sockets.coordinators[ 0 ]->registered ) {
		usleep(5);
	}

	// Register to servers
	for ( int i = 0, len = this->config.global.servers.size(); i < len; i++ ) {
		this->sockets.servers[ i ]->registerClient();
	}

	/* Remapping message handler */
	if ( ! this->config.global.states.disabled && ! this->stateTransitHandler.start() ) {
		__ERROR__( "Client", "start", "Cannot start remapping message handler." );
		ret = false;
	}

	this->startTime = start_timer();
	this->isRunning = true;

	/* Loading statistics update */
	//fprintf( stderr, "Update loading stats every %d seconds\n", this->config.global.timeout.load );
	//alarm ( this->config.global.timeout.load );
	this->statsTimer.start();

	return ret;
}

bool Client::stop() {
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
	for ( i = 0, len = this->sockets.servers.size(); i < len; i++ )
		this->sockets.servers[ i ]->stop();
	this->sockets.servers.clear();

	 /* Remapping message handler */
	if ( ! this->config.global.states.disabled ) {
		this->stateTransitHandler.stop();
		this->stateTransitHandler.quit();
	}

	/* Loading statistics update */
	//alarm ( 0 );
	statsTimer.stop();

	this->free();
	this->isRunning = false;
	printf( "\nBye.\n" );
	return true;
}

double Client::getElapsedTime() {
	return get_elapsed_time( this->startTime );
}

void Client::info( FILE *f ) {
	this->config.global.print( f );
	this->config.client.print( f );
	this->stripeList->print( f );
}

void Client::debug( FILE *f ) {
	int i, len;

	fprintf( f, "Client socket\n-------------\n" );
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

	fprintf( f, "\nServer sockets\n-------------\n" );
	for ( i = 0, len = this->sockets.servers.size(); i < len; i++ ) {
		fprintf( f, "%d. ", i + 1 );
		this->sockets.servers[ i ]->print( f );
	}
	if ( len == 0 ) fprintf( f, "(None)\n" );

	fprintf( f, "\nClient event queue\n------------------\n" );
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

void Client::interactive() {
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
			for ( int i = 0, len = this->sockets.servers.size(); i < len; i ++ ) {
				printf("Prepare replay for server id = %hu fd = %u\n", this->sockets.servers[ i ]->instanceId, this->sockets.servers[ i ]->getSocket() );
				ClientWorker::replayRequestPrepare( this->sockets.servers[ i ] );
			}
			for ( int i = 0, len = this->sockets.servers.size(); i < len; i ++ ) {
				printf("Replay for server id = %hu fd = %u\n", this->sockets.servers[ i ]->instanceId, this->sockets.servers[ i ]->getSocket() );
				ClientWorker::replayRequest( this->sockets.servers[ i ] );
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

bool Client::setDebugFlag( char *input ) {
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

bool Client::isDegraded( ServerSocket *socket ) {
	return (
		( this->debugFlags.isDegraded )
		||
		(
			this->stateTransitHandler.useCoordinatedFlow( socket->getAddr(), true, true ) &&
			! this->config.client.degraded.disabled
		)
	);
}

void Client::printInstanceId( FILE *f ) {
	fprintf( f, "Instance ID = %u\n", Client::instanceId );
}

void Client::printPending( FILE *f ) {
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

	LOCK( &this->pending.servers.setLock );
	fprintf(
		f,
		"\n\nPending requests for servers\n"
		"---------------------------\n"
		"[SET] Pending: %lu\n",
		this->pending.servers.set.size()
	);

	i = 1;
	for (
		it = this->pending.servers.set.begin();
		it != this->pending.servers.set.end();
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
	UNLOCK( &this->pending.servers.setLock );

	LOCK( &this->pending.servers.getLock );
	fprintf(
		f,
		"\n[GET] Pending: %lu\n",
		this->pending.servers.get.size()
	);
	i = 1;
	for (
		it = this->pending.servers.get.begin();
		it != this->pending.servers.get.end();
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
	UNLOCK( &this->pending.servers.getLock );

	LOCK( &this->pending.servers.updateLock );
	fprintf(
		f,
		"\n[UPDATE] Pending: %lu\n",
		this->pending.servers.update.size()
	);
	i = 1;
	for (
		keyValueUpdateIt = this->pending.servers.update.begin();
		keyValueUpdateIt != this->pending.servers.update.end();
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
	UNLOCK( &this->pending.servers.updateLock );

	LOCK( &this->pending.servers.delLock );
	fprintf(
		f,
		"\n[DELETE] Pending: %lu\n",
		this->pending.servers.del.size()
	);
	i = 1;
	for (
		it = this->pending.servers.del.begin();
		it != this->pending.servers.del.end();
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
	UNLOCK( &this->pending.servers.delLock );

	fprintf(
		f,
		"\n[ACK] Parity backup: (remove)%10lu  (revert)%10lu\n",
		this->pending.ack.remove.size(),
		this->pending.ack.revert.size()
	);
}

void Client::printRemapping( FILE *f ) {
	fprintf(
		f,
		"\nList of Tracking Servers\n"
		"------------------------\n"
	);
	this->stateTransitHandler.listAliveServers();

	fprintf(
		f,
		"\nServers Transit. Info\n"
		"------------------------\n"
	);
	char buf[ 16 ];
	for ( auto &info : this->stateTransitHandler.stateTransitInfo ) {
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

void Client::printBackup( FILE *f ) {
	ServerSocket *s;
	LOCK( &this->sockets.servers.lock );
	std::vector<ServerSocket *> &sockets = this->sockets.servers.values;
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
	UNLOCK( &this->sockets.servers.lock );
}

void Client::syncMetadata() {
	uint32_t socketFromId, socketToId;
	char tmp[ 16 ];
	ServerEvent event;

	printf( "Which socket ([0-%lu] or all)? ", this->sockets.servers.size() - 1 );
	fflush( stdout );
	if ( ! fgets( tmp, sizeof( tmp ), stdin ) )
		return;
	if ( strncmp( tmp, "all", 3 ) == 0 ) {
		socketFromId = 0;
		socketToId = this->sockets.servers.size();
	} else if ( sscanf( tmp, "%u", &socketFromId ) != 1 ) {
		fprintf( stderr, "Invalid socket ID.\n" );
		return;
	} else if ( socketFromId >= this->sockets.servers.size() ) {
		fprintf( stderr, "The specified socket ID exceeds the range [0-%lu].\n", this->sockets.servers.size() - 1 );
		return;
	} else {
		socketToId = socketFromId + 1;
	}

	for ( uint32_t socketId = socketFromId; socketId < socketToId; socketId++ ) {
		ServerSocket *socket = this->sockets.servers.values[ socketId ];
		if ( ! socket ) {
			fprintf( stderr, "Unknown socket ID!\n" );
			return;
		}

		event.syncMetadata( socket );
		this->eventQueue.insert( event );

		printf( "Synchronize metadata backup for the server: (#%u) ", socketId );
		socket->printAddress();
		printf( "\n" );
	}
}

void Client::help() {
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
		"- metadata: Synchronize metadata backup for a particular server\n"
		"- set: Set debug flag (degraded = [true|false])\n"
		"- time: Show elapsed time\n"
		"- ackparity: Acknowledge parity delta\n"
		"- revertparity: Revert unacknowledged parity delta\n"
		"- exit: Terminate this client\n"
	);
	fflush( stdout );
}

void Client::time() {
	fprintf( stdout, "Elapsed time: %12.6lf s\n", this->getElapsedTime() );
	fflush( stdout );
}

#define DISPATCH_EVENT_TO_OTHER_SERVERS( _METHOD_NAME_, _S_, _COND_, _LOCK_, _COUNTER_ ) {  \
	std::vector<ServerEvent> events; \
	for ( int j = 0, len = this->sockets.servers.size(); j < len; j++ ) { \
		ServerEvent event; \
		ServerSocket *p = this->sockets.servers[ j ]; \
		struct sockaddr_in saddr = p->getAddr(); \
		/* skip myself, and any node declared to be failed */ \
		if ( p == _S_ || this->stateTransitHandler.useCoordinatedFlow( saddr ) ) continue; \
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

void Client::ackParityDelta( FILE *f, ServerSocket *target, pthread_cond_t *condition, LOCK_T *lock, uint32_t *counter, bool force ) {
	uint32_t from, to, update, del;
	std::vector<uint32_t> timestamps;
	std::vector<Key> requests;

	for( int i = 0, len = this->sockets.servers.size(); i < len; i++ ) {
		ServerSocket *s = this->sockets.servers[ i ];

		if ( target && target != s )
			continue;

		LOCK( &target->ackParityDeltaBackupLock );
		from = s->timestamp.lastAck.getVal();
		to = s->timestamp.current.getVal();
		del = update = to;

		LOCK( &s->timestamp.pendingAck.updateLock );
		if ( ! s->timestamp.pendingAck._update.empty() )
			update = s->timestamp.pendingAck._update.begin()->first.getVal() - 1;
		UNLOCK( &s->timestamp.pendingAck.updateLock );

		LOCK( &s->timestamp.pendingAck.delLock );
		if ( ! s->timestamp.pendingAck._del.empty() )
			del = s->timestamp.pendingAck._del.begin()->first.getVal() - 1;
		UNLOCK( &s->timestamp.pendingAck.delLock );

		/* find the small acked timestamp */
		to = update < to ? ( del < update ? del : update ) : to ;

		/* check the threshold is reached */
		if ( ! force && Timestamp( to ) - Timestamp( from ) < this->config.client.backup.ackBatchSize ) {
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
		DISPATCH_EVENT_TO_OTHER_SERVERS( ackParityDelta, s, condition, lock, counter );

		s->timestamp.lastAck.setVal( to );
		UNLOCK( &target->ackParityDeltaBackupLock );
	}
}

bool Client::revertDelta( FILE *f, ServerSocket *target, pthread_cond_t *condition, LOCK_T *lock, uint32_t *counter, bool force ) {
	std::vector<uint32_t> timestamps;
	std::vector<Key> requests;
	std::set<uint32_t> timestampSet;

	KeyValue keyValue, keyValueDup;
	Key key;
	char *valueStr;
	uint32_t valueSize;

	PendingIdentifier pid;

	// process one target server at a time
	if ( ! target )
		return false;

	// ack parity delta as well, but do not wait for it to complete
	this->ackParityDelta( f, target, 0, 0, 0, force );

	// UPDATE / DELETE
	LOCK( &target->timestamp.pendingAck.updateLock );
	for ( auto &rec : target->timestamp.pendingAck._update ) {
		// skip reverting requests that are pending
		if ( this->stateTransitHandler.stateTransitInfo[ target->getAddr() ].isPendingRequest( rec.second ) ) {
			__INFO__( YELLOW, "Client", "revertDelta", "SKIP revert pending request id = %u", rec.second );
			continue;
		}
		timestampSet.insert( rec.first.getVal() );
	}
	UNLOCK( &target->timestamp.pendingAck.updateLock );

	LOCK( &target->timestamp.pendingAck.delLock );
	for ( auto &rec : target->timestamp.pendingAck._del ) {
		// skip reverting requests that are pending
		if ( this->stateTransitHandler.stateTransitInfo[ target->getAddr() ].isPendingRequest( rec.second ) ) {
			continue;
		}
		timestampSet.insert( rec.first.getVal() );
	}
	UNLOCK( &target->timestamp.pendingAck.delLock );

	// SET
	std::unordered_multimap<PendingIdentifier, Key>::iterator it, saveIt;
	uint32_t listIndex, chunkIndex;
	std::unordered_set<uint32_t> outdatedRequestIds;
	ServerSocket *dataServer = 0;

	LOCK ( &this->pending.servers.setLock );
	for( it = this->pending.servers.set.begin(), saveIt = it; it != this->pending.servers.set.end(); it = saveIt ) {
		saveIt++;
		// skip if not pending for failed server
		listIndex = this->stripeList->get( it->second.data, it->second.size, 0, 0, &chunkIndex );
		dataServer = this->stripeList->get( listIndex, chunkIndex );
		if ( it->first.ptr != target && dataServer != target )
			continue;
		// skip revert if failed server is not the data server, but see if the request can be immediately replied
		if ( dataServer != target ) {
			if (
				this->pending.count( PT_SERVER_SET, it->first.instanceId, it->first.requestId, false, false ) == 1 &&
				this->pending.eraseKeyValue( PT_APPLICATION_SET, it->first.parentInstanceId, it->first.parentRequestId, 0, &pid, &keyValue, true, true, true, it->second.data )
			) {

				keyValue.deserialize( key.data, key.size, valueStr, valueSize );
				keyValueDup.dup( key.data, key.size, valueStr, valueSize, 0 );

				// duplicate the key for reply
				if ( pid.ptr ) {
					ApplicationEvent applicationEvent;
					applicationEvent.resSet( ( ApplicationSocket * ) pid.ptr, pid.instanceId, pid.requestId, keyValueDup, true, true );
					this->eventQueue.insert( applicationEvent );
					__DEBUG__( CYAN, "Client", "revertDelta", "Complete request id=%u without waiting failed server id=%u", pid.requestId, target->instanceId );
					// mark the request as no need to wait further (already replied)
					outdatedRequestIds.insert( pid.requestId );
				}
			}
			this->pending.servers.set.erase( it );
			continue;
		} else if ( it->first.ptr != target ) {
			// data server failed, but this pending is for parity, directly revert and no need to wait anymore
			//this->pending.servers.set.erase( it );
			//continue;
		}
		// skip if no backup available to do the revert
		if ( ! this->pending.findKeyValue( PT_APPLICATION_SET, it->first.parentInstanceId, it->first.parentRequestId, 0, &keyValue, true, it->second.data ) )
			continue;
		// temporarily duplicate the key for revert
		key = keyValue.key();
		key.dup( key.size, key.data );
		requests.push_back( key );
	}
	UNLOCK ( &this->pending.servers.setLock );

	for ( uint32_t requestId : outdatedRequestIds ) {
		this->stateTransitHandler.stateTransitInfo.at( target->getAddr() ).removePendingRequest( requestId, false );
	}
	if ( ! outdatedRequestIds.empty() ) { 
		this->stateTransitHandler.stateTransitInfo.at( target->getAddr() ).setCompleted();
	}


	// force revert all parity for testing purpose
	//for ( uint32_t i = target->timestamp.lastAck.getVal() ; i < target->timestamp.current.getVal(); i++ ) {
	//	timestampSet.insert( i );
	//}

	//if ( timestampSet.size() == 0 && requests.size() == 0 )
	//	return false;

	if ( f ) {
		target->printAddress();
		fprintf( f, "Revert parity delta for timestamps from %u to %u with count = %lu\n", *timestampSet.begin(), target->timestamp.current.getVal(), timestampSet.size() );
	}
	//fprintf( stdout, "Revert parity delta for timestamps from %u to %u with count = %lu\n", *timestampSet.begin(), target->timestamp.current.getVal(), timestampSet.size() );

	timestamps.insert( timestamps.begin(), timestampSet.begin(), timestampSet.end() ); // ordered timestamps

	DISPATCH_EVENT_TO_OTHER_SERVERS( revertDelta, target, condition, lock, counter );

	return true;
}
