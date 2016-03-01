#include <vector>
#include <arpa/inet.h>
#include <unistd.h>
#include "../../../common/remap/remap_state.hh"
#include "../../../client/remap/remap_msg_handler.hh"

#define TIME_OUT 1

#define MASTER_REMAP_UNIT_TEST
#include "common.hh"
#undef MASTER_REMAP_UNIT_TEST

int main ( int argc, char **argv ) {

	if ( argc < 2 ) {
		fprintf( stderr, "Usage: %s name_identified\n", argv[0] );
		return 1;
	}

	MasterRemapMsgHandler* mh = new MasterRemapMsgHandler();
	char namebuf[64];

	fprintf( stderr, "START testing master remapping message handler\n");
	// init. the hanlder with an address and a user name
	struct in_addr addr;
	inet_pton( AF_INET, "127.0.0.1", &addr );
	sprintf( namebuf, "%s%s", CLIENT_PREFIX, argv[1] );

	mh->init( addr.s_addr, htons( 4803 ), namebuf );
	// start listening to incomming messages from coordinator (via spread daemon)
	if ( ! mh->start() ) {
		fprintf( stderr, "!! Cannot start reading message with message handler !!\n" );
	} else {
		fprintf( stderr, ".. Add random servers\n" );
		std::vector<struct sockaddr_in> servers = addSlaves( mh );
		// simulate the flow of start/end of remapping phase
		for ( int i = 0; i < ROUNDS; i++ ) {
			fprintf( stderr, ".. Waiting start of remapping phase\n" );
			while ( meetStatus( mh, servers, REMAP_INTERMEDIATE ) == false &&
				meetStatus( mh, servers, REMAP_WAIT_DEGRADED ) == false &&
				meetStatus( mh, servers, REMAP_WAIT_NORMAL ) == false
			)
				sleep( TIME_OUT );
			fprintf( stderr, ".. Waiting end of remapping phase\n" );
			mh->ackTransit();
			while ( meetStatus( mh, servers, REMAP_NORMAL ) == false )
				sleep( TIME_OUT );
			fprintf( stderr, "... Stop listening to incomming messages\n" );
		}
		mh->stop();
	}
	// stop listening and disconnect from spread daemon
	mh->quit();
	delete mh;

	fprintf( stderr, "END testing master remapping message handler\n");

	return 0;
}
