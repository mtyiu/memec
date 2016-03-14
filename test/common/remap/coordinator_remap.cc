#include <vector>
#include <arpa/inet.h>
#include <unistd.h>
#include "../../../common/remap/remap_state.hh"
#include "../../../coordinator/remap/remap_msg_handler.hh"

#define TIME_OUT 2
#define JOIN_TIME_OUT 4

#define COORDINATOR_REMAP_UNIT_TEST
#include "common.hh"
#undef COORDINATOR_REMAP_UNIT_TEST

int main () {
	CoordinatorRemapMsgHandler *ch = CoordinatorRemapMsgHandler::getInstance();

	fprintf( stderr, "START testing coordinator remapping message handler\n" );

	struct in_addr addr;
	inet_pton( AF_INET, "127.0.0.1", &addr );

	ch->init( addr.s_addr, htons( 4803 ), COORD_PREFIX );

	if ( ! ch->start() ) {
		fprintf( stderr, "!! Cannot start reading message with message handler !!\n" );
	} else {
		fprintf( stderr, ".. wait for clients to join in %d seconds\n", JOIN_TIME_OUT );
		sleep( JOIN_TIME_OUT );
		fprintf( stderr, " .. Add random servers\n" );
		std::vector<struct sockaddr_in> servers = addServers( ch );
		for ( int i = 0; i < ROUNDS; i++ ) {
			sleep( TIME_OUT );
			fprintf( stderr, ".. Start remapping phase\n" );
			startRemap( ch, servers );
			while( meetStatus( ch, servers, REMAP_DEGRADED) == false )
				sleep( TIME_OUT );
			fprintf( stderr, ".. Stop remapping phase\n" );
			for ( int i = 0; i < REMAP_COUNT; i++ ) {
				stopRemap( ch, servers );
				sleep( TIME_OUT );
			}
			while( meetStatus( ch, servers, REMAP_NORMAL ) == false )
				sleep( TIME_OUT );
		}
		ch->stop();
	}
	ch->quit();

	fprintf( stderr, "END testing coordinator remapping message handler\n" );

	return 0;
}
