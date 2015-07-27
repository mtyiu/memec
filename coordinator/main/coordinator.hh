#ifndef __COORDINATOR_MAIN_COORDINATOR_HH__
#define __COORDINATOR_MAIN_COORDINATOR_HH__

#include <cstdio>
#include "../config/coordinator_config.hh"
#include "../event/event_queue.hh"
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

	Coordinator();
	// Do not implement
	Coordinator( Coordinator const& );
	void operator=( Coordinator const& );

	void free();
	// Commands
	void help();
	void time();

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
	double getElapsedTime();
	void interactive();
};

#endif
