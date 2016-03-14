#include <climits>
#include <cstdlib>
#include <arpa/inet.h>
#include "../../../common/remap/remap_msg_handler.hh"
#include "../../../coordinator/remap/remap_msg_handler.hh"
#include "../../../client/remap/remap_msg_handler.hh"

#define SERVER_COUNT		8
#define REMAP_COUNT		2
#define SEED			7654321
#define ROUNDS 2

// random a set of servers for testing
std::vector<struct sockaddr_in> addServers( RemapMsgHandler *handler ) {
	srand( SEED );
	std::vector<struct sockaddr_in> servers;
	struct sockaddr_in server;
	for ( int i = 0; i < SERVER_COUNT; i++ ) {
		server.sin_addr.s_addr = rand() % UINT_MAX;
		server.sin_port = rand() % USHRT_MAX;
		handler->addAliveServer( server );
		servers.push_back( server );
		fprintf( stderr, "\t\t Servers %u:%hu added\n", servers[ i ].sin_addr.s_addr, servers[ i ].sin_port );
	}
	return servers;
}

#ifdef COORDINATOR_REMAP_UNIT_TEST

static int count = 1;

// randomly select a set of servers to start remapping
void startRemap( CoordinatorRemapMsgHandler *handler, const std::vector<struct sockaddr_in> &servers ) {
	srand( SEED / SERVER_COUNT );
	std::vector<struct sockaddr_in> serversToRemap;
	for ( int i = 0; i < REMAP_COUNT; i++ ) {
		int idx = rand() % servers.size();
		serversToRemap.push_back( servers[ idx ] );
		fprintf( stderr, "\t\t Start Remap Servers%d %u:%hu\n", idx, servers[ idx ].sin_addr.s_addr, servers[ idx ].sin_port );
	}
	handler->transitToDegraded( &serversToRemap );
}

// randomly select a set of servers to stop remapping
void stopRemap( CoordinatorRemapMsgHandler *handler, const std::vector<struct sockaddr_in> &servers ) {
	srand( SEED / SERVER_COUNT );
	std::vector<struct sockaddr_in> serversToStop;
	for ( int i = 0; i < count; i++ ) {
		int idx = rand() % servers.size();
		serversToStop.push_back( servers[ idx ] );
	}
	handler->transitToNormal( &serversToStop );
	count++;
}

#endif

// check for status change completion
bool meetStatus( RemapMsgHandler *handler, const std::vector<struct sockaddr_in> &servers, RemapState target ) {
	bool ret = true;
	RemapState current;
	srand( SEED / SERVER_COUNT );
	for ( int i = 0; i < REMAP_COUNT; i ++ ) {
		int idx = rand() % servers.size();
		current = handler->getState( servers[ idx ] );
		fprintf( stderr, "\t\t Server %u:%hu status=%d\n", servers[ idx ].sin_addr.s_addr, servers[ idx ].sin_port, current );
		ret = ( ret && current == target );
	}
	return ret;
}
