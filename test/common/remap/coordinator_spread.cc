#include <cmath>						// NAN
#include <cstdio>
#include <cstring>
#include <unistd.h>
#include <vector>
#include "../../../common/ds/sockaddr_in.hh"
#include "../../../common/remap/remap_group.hh"
#include "../../../common/util/time.hh"
#include "simple_remap_msg_handler.hh"

#define ROUNDS	( 1000 * 50 )			// 50k messages
#define NUM_SERVERS_IN_MSG ( 2 ) 		// double failure

void usage( char *prog ) {
	printf( "Usage: %s [spread daemon addr in \"port@ip\"] [num of client]\n", prog );
}

int main( int argc, char **argv ) {
	char userbuf[ 32 ];			// name of the coordinator in spread
	int target = 0;				// no. of clients to benchmark
	if ( argc < 3 ) {
		usage( argv[ 0 ] );
		return -1;
	}
	target = atoi( argv[ 2 ] );

	// init the coordinator-like message handler
	SimpleRemapMsgHandler ch;
	sprintf( userbuf, "%s%d", COORD_PREFIX, 1 );
	ch.init( argv[ 1 ], userbuf );
	ch.join( SERVER_GROUP );
	if ( ch.start() == false ) {
		fprintf( stderr, ">> Failed to start! <<\n" );
		return -1;
	}

	// init the dummy variables
	std::vector<struct sockaddr_in> servers;
	struct sockaddr_in server;
	for ( int i = 0; i < NUM_SERVERS_IN_MSG; i++ ) {
		servers.push_back( server );
	}

	char targetGroups[ 2 ][ MAX_GROUP_NAME ];
	strcpy( targetGroups[ 0 ], CLIENT_GROUP );
	strcpy( targetGroups[ 1 ], SERVER_GROUP );

	// wait for all clients to join
	printf( "Wait for clients...\n" );
	while( ch.clients + ch.servers < target )
		usleep(500);

	int numGroups = ( ch.clients > 0 && ch.servers > 0 )? 2 : 1;

	printf( "All clients are ready...\n" );
	// start of benchmark
	int i = 0;
	struct timespec startTime = start_timer();
	for ( i = 0; i < ROUNDS; i++ ) {
		if ( ch.sendStatePub( servers, numGroups , targetGroups ) == false ) {
			fprintf( stderr, ">> Failed to send states! <<\n" );
			break;
		}
	}
	double duration = get_elapsed_time( startTime );
	printf(
		"%d boardcast to %d groups and %d receivers;"
		" duration = %.10lf;"
		" average = %.10lf\n",
		i, numGroups , target,
		duration, i > 0 ? duration / i : NAN
	);
	// end of benchmark

	ch.stop();
	return 0;
}
