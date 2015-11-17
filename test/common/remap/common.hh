#include <climits>
#include <cstdlib>
#include <arpa/inet.h>
#include "../../../common/remap/remap_msg_handler.hh"
#include "../../../coordinator/remap/remap_msg_handler.hh"
#include "../../../master/remap/remap_msg_handler.hh"

#define SLAVE_COUNT		8
#define REMAP_COUNT		2
#define SEED			7654321
#define ROUNDS 2

static int count = 1;

// random a set of slaves for testing
std::vector<struct sockaddr_in> addSlaves( RemapMsgHandler *handler ) {
	srand( SEED );
	std::vector<struct sockaddr_in> slaves;
	struct sockaddr_in slave;
	for ( int i = 0; i < SLAVE_COUNT; i++ ) {
		slave.sin_addr.s_addr = rand() % UINT_MAX;
		slave.sin_port = rand() % USHRT_MAX;
		handler->addAliveSlave( slave );
		slaves.push_back( slave );
		fprintf( stderr, "\t\t Slaves %u:%hu added\n", slaves[ i ].sin_addr.s_addr, slaves[ i ].sin_port );
	}
	return slaves;
}

// randomly select a set of slaves to start remapping
void startRemap( CoordinatorRemapMsgHandler *handler, const std::vector<struct sockaddr_in> &slaves ) {
	srand( SEED / SLAVE_COUNT );
	std::vector<struct sockaddr_in> slavesToRemap;
	for ( int i = 0; i < REMAP_COUNT; i++ ) {
		int idx = rand() % slaves.size();
		slavesToRemap.push_back( slaves[ idx ] );
		fprintf( stderr, "\t\t Start Remap Slaves%d %u:%hu\n", idx, slaves[ idx ].sin_addr.s_addr, slaves[ idx ].sin_port );
	}
	handler->startRemap( &slavesToRemap );
}

// randomly select a set of slaves to stop remapping
void stopRemap( CoordinatorRemapMsgHandler *handler, const std::vector<struct sockaddr_in> &slaves ) {
	srand( SEED / SLAVE_COUNT );
	std::vector<struct sockaddr_in> slavesToStop;
	for ( int i = 0; i < count; i++ ) {
		int idx = rand() % slaves.size();
		slavesToStop.push_back( slaves[ idx ] );
	}
	handler->stopRemap( &slavesToStop );
	count++;
}

// check for status change completion
bool meetStatus( RemapMsgHandler *handler, const std::vector<struct sockaddr_in> &slaves, RemapStatus target ) {
	bool ret = true;
	RemapStatus current;
	srand( SEED / SLAVE_COUNT );
	for ( int i = 0; i < REMAP_COUNT; i ++ ) {
		int idx = rand() % slaves.size();
		current = handler->getStatus( slaves[ idx ] );
		fprintf( stderr, "\t\t Slave %u:%hu status=%d\n", slaves[ idx ].sin_addr.s_addr, slaves[ idx ].sin_port, current );
		ret = ( ret && current == target );
	}
	return ret;
}
