#ifndef __COORDINATOR_MAIN_COORDINATOR_HH__
#define __COORDINATOR_MAIN_COORDINATOR_HH__

#include <cstdio>
#include <pthread.h>
#include "../config/coordinator_config.hh"
#include "../event/event_queue.hh"
#include "../remap/remap_msg_handler.hh"
#include "../socket/coordinator_socket.hh"
#include "../socket/master_socket.hh"
#include "../socket/slave_socket.hh"
#include "../worker/worker.hh"
#include "../../common/config/global_config.hh"
#include "../../common/ds/array_map.hh"
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

	CoordinatorRemapMsgHandler remapMsgHandler;

	Coordinator();
	// Do not implement
	Coordinator( Coordinator const& );
	void operator=( Coordinator const& );

	void free();
	void updateAverageSlaveLoading( ArrayMap<ServerAddr, Latency> *slaveGetLatency, 
			ArrayMap<ServerAddr, Latency> *slaveSetLatency );
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
	CoordinatorEventQueue eventQueue;
	struct {
		// ( slaveAddr, ( mastserAddr, Latency ) )
		ArrayMap< ServerAddr, ArrayMap< ServerAddr, Latency > > latestGet;
		ArrayMap< ServerAddr, ArrayMap< ServerAddr, Latency > > latestSet;
		pthread_mutex_t loadingLock;
	} slaveLoading;
	
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
	double getElapsedTime();
	void interactive();
};

#endif
