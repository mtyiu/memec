#ifndef __COORDINATOR_MAIN_COORDINATOR_HH__
#define __COORDINATOR_MAIN_COORDINATOR_HH__

#include <cstdio>
#include <pthread.h>
#include <set>
#include "../config/coordinator_config.hh"
#include "../event/event_queue.hh"
#include "../remap/remap_msg_handler.hh"
#include "../socket/coordinator_socket.hh"
#include "../socket/master_socket.hh"
#include "../socket/slave_socket.hh"
#include "../worker/worker.hh"
#include "../../common/config/global_config.hh"
#include "../../common/ds/array_map.hh"
#include "../../common/ds/id_generator.hh"
#include "../../common/lock/lock.hh"
#include "../../common/socket/epoll.hh"
#include "../../common/signal/signal.hh"
#include "../../common/util/option.hh"
#include "../../common/util/time.hh"

// Implement the singleton pattern
class Coordinator {
private:
	bool isRunning;
	struct timespec startTime;
	std::vector<CoordinatorWorker> workers;

	Coordinator();
	// Do not implement
	Coordinator( Coordinator const& );
	void operator=( Coordinator const& );

	void free();

	// Helper functions to determine slave loading
	void updateAverageSlaveLoading( ArrayMap<struct sockaddr_in, Latency> *slaveGetLatency,
			ArrayMap<struct sockaddr_in, Latency> *slaveSetLatency );
	void updateOverloadedSlaveSet( ArrayMap<struct sockaddr_in, Latency> *slaveGetLatency,
			ArrayMap<struct sockaddr_in, Latency> *slaveSetLatency,
			std::set<struct sockaddr_in> *slaveSet );
	bool switchPhase();

	// Commands
	void help();

public:
	struct {
		GlobalConfig global;
		CoordinatorConfig coordinator;
	} config;
	struct {
		CoordinatorSocket self;
		EPoll epoll;
		ArrayMap<int, MasterSocket> masters;
		ArrayMap<int, SlaveSocket> slaves;
	} sockets;
	IDGenerator idGenerator;
	CoordinatorEventQueue eventQueue;

	CoordinatorRemapMsgHandler remapMsgHandler;

	struct {
		// ( slaveAddr, ( mastserAddr, Latency ) )
		ArrayMap< struct sockaddr_in, ArrayMap< struct sockaddr_in, Latency > > latestGet;
		ArrayMap< struct sockaddr_in, ArrayMap< struct sockaddr_in, Latency > > latestSet;
		pthread_mutex_t lock;
	} slaveLoading;
	struct {
		std::set< struct sockaddr_in > slaveSet;
		pthread_mutex_t lock;
	} overloadedSlaves;
	Timer statsTimer;

	static Coordinator *getInstance() {
		static Coordinator coordinator;
		return &coordinator;
	}

	static void signalHandler( int signal );

	bool init( char *path, OptionList &options, bool verbose );
	bool start();
	bool stop();
	void info( FILE *f = stdout );
	void debug( FILE *f = stdout );
	void dump();
	void time();
	void seal();
	void flush();
	double getElapsedTime();
	void interactive();
};

#endif
