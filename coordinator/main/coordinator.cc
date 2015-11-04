#include <cstring>
#include <ctype.h>
#include "coordinator.hh"
#include "../ds/map.hh"
#include "../../common/ds/sockaddr_in.hh"

#define GIGA 				( 1000 * 1000 * 1000 )
#define FLOAT_THRESHOLD		( ( float ) 0.00001 )
#define A_EQUAL_B( _A_, _B_ ) \
	( _A_ - _B_ >= -1 * FLOAT_THRESHOLD && _A_ - _B_ <= FLOAT_THRESHOLD )

Coordinator::Coordinator() {
	this->isRunning = false;
}

void Coordinator::free() {
	this->idGenerator.free();
	this->eventQueue.free();
	delete this->stripeList;
	Map::free();
}

bool Coordinator::switchPhase() {
	// skip if remap feature is disabled
	bool switched = false;
	if ( ! this->config.global.remap.enabled )
		return switched;

	MasterEvent event;
	LOCK( &this->overloadedSlaves.lock );
	if ( this->remapMsgHandler->isRemapStopped() &&
			this->overloadedSlaves.slaveSet.size() > this->sockets.slaves.size() * this->config.global.remap.startThreshold ) {
		// start remapping phase in the background
		event.switchPhase( true );
	} else if ( this->remapMsgHandler->isRemapStarted() &&
			this->overloadedSlaves.slaveSet.size() < this->sockets.slaves.size() * this->config.global.remap.stopThreshold ) {
		// stop remapping phase in the background
		event.switchPhase( false );
	} else {
		goto quit;
	}
	this->eventQueue.insert( event );
quit:
	UNLOCK( &this->overloadedSlaves.lock );
	return switched;
}

void Coordinator::updateOverloadedSlaveSet( ArrayMap<struct sockaddr_in, Latency> *slaveGetLatency,
		ArrayMap<struct sockaddr_in, Latency> *slaveSetLatency, std::set<struct sockaddr_in> *slaveSet ) {
	double avgSec = 0.0, avgNsec = 0.0;
	LOCK( &this->overloadedSlaves.lock );

	// what has past is left in the past
	this->overloadedSlaves.slaveSet.clear();
	double threshold = this->config.global.remap.overloadThreshold;

	// compare each slave latency with the avg multipled by threshold
#define GET_OVERLOADED_SLAVES( _TYPE_ ) { \
	uint32_t slaveCount = slave##_TYPE_##Latency->size(); \
	for ( uint32_t i = 0; i < slaveCount; i++ ) { \
		avgSec += ( double ) slave##_TYPE_##Latency->values[ i ]->sec / slaveCount; \
		avgNsec += ( double ) slave##_TYPE_##Latency->values[ i ]->nsec / slaveCount; \
	} \
	for ( uint32_t i = 0; i < slaveCount; i++ ) { \
		if ( ( double ) slave##_TYPE_##Latency->values[ i ]->sec > avgSec * threshold || \
				( ( A_EQUAL_B ( slave##_TYPE_##Latency->values[ i ]->sec, avgSec * threshold ) && \
					(double) slave##_TYPE_##Latency->values[ i ]->nsec >= avgNsec * threshold ) ) ) {\
			this->overloadedSlaves.slaveSet.insert( slave##_TYPE_##Latency->keys[ i ] ); \
			slaveSet->insert( slave##_TYPE_##Latency->keys[ i ] ); \
			/* printf( "Slave #%u overloaded!!!!\n", i ); */ \
		} \
	} \
}

	GET_OVERLOADED_SLAVES( Get );
	GET_OVERLOADED_SLAVES( Set );
#undef GET_OVERLOADED_SLAVES
	UNLOCK( &this->overloadedSlaves.lock );
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
		for ( uint32_t j = 0; j < masterCount; j++ ) { \
			avgSec += ( double ) latest->values[ i ]->values[ j ]->sec / masterCount; \
			avgNsec += ( double ) latest->values[ i ]->values[ j ]->nsec / masterCount; \
			if ( avgNsec >= GIGA ) { \
				avgNsec -= GIGA; \
				avgSec += 1; \
			} \
		} \
		slave##_TYPE_##Latency->set( latest->keys[ i ], new Latency( avgSec, avgNsec ) ); \
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
	//this->slaveLoading.latestGet.clear();
	//this->slaveLoading.latestSet.clear();
	UNLOCK( &this->slaveLoading.lock );
}

void Coordinator::signalHandler( int signal ) {
	Coordinator *coordinator = Coordinator::getInstance();
	ArrayMap<int, MasterSocket> &sockets = coordinator->sockets.masters;
	ArrayMap<struct sockaddr_in, Latency> *slaveGetLatency = new ArrayMap<struct sockaddr_in, Latency>();
	ArrayMap<struct sockaddr_in, Latency> *slaveSetLatency = new ArrayMap<struct sockaddr_in, Latency>();
	std::set<struct sockaddr_in> *overloadedSlaveSet = new std::set<struct sockaddr_in>();
	switch ( signal ) {
		case SIGALRM:
			coordinator->updateAverageSlaveLoading( slaveGetLatency, slaveSetLatency );
			coordinator->updateOverloadedSlaveSet( slaveGetLatency, slaveSetLatency, overloadedSlaveSet );
			coordinator->switchPhase();
			// TODO start / stop remapping according to criteria
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
			// set timer for next push
			//alarm( coordinator->config.coordinator.loadingStats.updateInterval );
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
	MasterSocket::setArrayMap( &this->sockets.masters );
	SlaveSocket::setArrayMap( &this->sockets.slaves );
	this->sockets.masters.reserve( this->config.global.slaves.size() );
	this->sockets.slaves.reserve( this->config.global.slaves.size() );
	Map::init( this->config.global.stripeList.count );
	/* Stripe list */
	this->addr.reserve( this->config.global.slaves.size() );
	for ( uint32_t i = 0, size = this->config.global.slaves.size(); i < size; i++ ) {
		this->addr.push_back( &this->config.global.slaves[ i ] );
	}
	this->stripeList = new StripeList<ServerAddr>(
		this->config.global.coding.params.getChunkCount(),
		this->config.global.coding.params.getDataChunkCount(),
		this->config.global.stripeList.count,
		this->addr
	);
	/* Workers, ID generator and event queues */
	if ( this->config.coordinator.workers.type == WORKER_TYPE_MIXED ) {
		this->idGenerator.init( this->config.coordinator.workers.number.mixed );
		this->eventQueue.init(
			this->config.coordinator.eventQueue.block,
			this->config.coordinator.eventQueue.size.mixed
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
		WORKER_INIT_LOOP( master, WORKER_ROLE_MASTER )
		WORKER_INIT_LOOP( slave, WORKER_ROLE_SLAVE )
#undef WORKER_INIT_LOOP
	}

	/* Remapping message handler */
	if ( this->config.global.remap.enabled ) {
		char coordName[ 11 ];
		memset( coordName, 0, 11 );
		sprintf( coordName, "%s%04d", COORD_PREFIX, this->config.coordinator.coordinator.addr.id );
		remapMsgHandler->init( this->config.global.remap.spreaddAddr.addr, this->config.global.remap.spreaddAddr.port, coordName );
		// TODO : add the slave addrs to remapMsgHandler
	}

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
	//alarm( this->config.coordinator.loadingStats.updateInterval );
	statsTimer.start();

	return true;
}

bool Coordinator::stop() {
	if ( ! this->isRunning )
		return false;

	int i, len;

	/* Sockets */
	this->sockets.self.stop();

	/* Workers */
	len = this->workers.size();
	for ( i = len - 1; i >= 0; i-- )
		this->workers[ i ].stop();

	/* Event queues */
	this->eventQueue.stop();

	/* Workers */
	for ( i = len - 1; i >= 0; i-- )
		this->workers[ i ].join();

	/* Sockets */
	for ( i = 0, len = this->sockets.masters.size(); i < len; i++ )
		this->sockets.masters[ i ]->stop();
	this->sockets.masters.clear();

	for ( i = 0, len = this->sockets.slaves.size(); i < len; i++ )
		this->sockets.slaves[ i ]->stop();
	this->sockets.slaves.clear();

	/* Remapping message handler */
	if ( this->config.global.remap.enabled ) {
		this->remapMsgHandler->stop();
		this->remapMsgHandler->quit();
	}

	/* Loading stats */
	statsTimer.stop();

	this->free();
	this->isRunning = false;
	printf( "\nBye.\n" );
	return true;
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

	fprintf( f, "\nCoordinator event queue\n-----------------------\n" );
	this->eventQueue.print( f );

	fprintf( f, "\nWorkers\n-------\n" );
	for ( i = 0, len = this->workers.size(); i < len; i++ ) {
		fprintf( f, "%d. ", i + 1 );
		this->workers[ i ].print( f );
	}

	fprintf( f, "\nOther threads\n--------------\n" );
	this->sockets.self.printThread();

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
		} else if ( strcmp( command, "dump" ) == 0 ) {
			valid = true;
			this->dump();
		} else if ( strcmp( command, "remapping" ) == 0 ) {
			valid = true;
			this->printRemapping();
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
	for ( size_t i = 0, len = this->sockets.slaves.size(); i < len; i++ ) {
		fprintf( f, "Slave #%lu: ", i + 1 );
		this->sockets.slaves[ i ]->printAddress( f );
		fprintf( f, "\n----------------------------------------\n" );

		this->sockets.slaves[ i ]->map.dump();
	}
}

void Coordinator::metadata() {
	FILE *f = fopen( "coordinator.meta", "w+" );
	if ( ! f ) {
		__ERROR__( "Slave", "metadata", "Cannot write to the file \"coordinator.meta\"." );
	}

	for ( size_t i = 0, len = this->sockets.slaves.size(); i < len; i++ ) {
		this->sockets.slaves[ i ]->map.persist( f );
	}

	fclose( f );
}

void Coordinator::printRemapping( FILE *f ) {
	fprintf( f, "\nRemapping Records\n" );
	fprintf( f, "----------------------------------------\n" );
	this->remappingRecords.print( f );
}

void Coordinator::help() {
	fprintf(
		stdout,
		"Supported commands:\n"
		"- help: Show this help message\n"
		"- info: Show configuration\n"
		"- debug: Show debug messages\n"
		"- time: Show elapsed time\n"
		"- seal: Force all slaves to seal all its chunks\n"
		"- flush: Force all slaves to flush all its chunks\n"
		"- metadata: Write metadata to disk\n"
		"- exit: Terminate this client\n"
	);
	fflush( stdout );
}

void Coordinator::time() {
	fprintf( stdout, "Elapsed time: %12.6lf s\n", this->getElapsedTime() );
	fflush( stdout );
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
