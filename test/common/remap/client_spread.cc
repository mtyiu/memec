#include <unistd.h>
#include "../../../common/remap/remap_group.hh"
#include "../../../common/remap/remap_state.hh"
#include "../../../common/remap/remap_msg_handler.hh"
#include "simple_remap_msg_handler.hh"

#define MAIN_THREAD_WAIT_TIME	(180)

void usage( char *prog ) {
	printf( "Usage: %s [spread daemon addr in \"port@ip\"] [id] [is_server]\n", prog );
}

int main( int argc, char **argv ) {
	int id = 0;
	bool isServer = 0;
	if ( argc < 3 ) {
		usage( argv[ 0 ] );
		return -1;
	}
	id = atoi( argv[ 2 ] );
	if ( argc >= 4 ) {
		isServer = atoi( argv[ 3 ] );
	}
	// decide the role of this spread client
	char *group = isServer? ( char* ) SLAVE_GROUP : ( char* ) MASTER_GROUP;
	char *prefix = isServer? ( char* ) SLAVE_PREFIX : ( char* ) MASTER_PREFIX;
	char userbuf[ 32 ];

	// connect to spread daemon
	SimpleRemapMsgHandler ch( group );
	sprintf( userbuf, "%s%d", prefix , id );
	ch.init( argv[ 1 ], userbuf );
	if ( ch.start() == false ) {
		fprintf( stderr, "Failed to start\n" );
		return -1;
	}
	
	// wait sufficient time until the benchmark finish, and exit
	while ( true ) {
		sleep( MAIN_THREAD_WAIT_TIME );
	}

	return 0;
}
