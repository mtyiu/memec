#ifndef __COORDINATOR_MAIN_COORDINATOR_HH__
#define __COORDINATOR_MAIN_COORDINATOR_HH__

#include <vector>
#include <cstdio>
#include "../config/coordinator_config.hh"
#include "../event/event_queue.hh"
#include "../socket/coordinator_socket.hh"
#include "../socket/master_socket.hh"
#include "../socket/slave_socket.hh"
#include "../worker/worker.hh"
#include "../../common/config/global_config.hh"
#include "../../common/socket/epoll.hh"

// Implement the singleton pattern
class Coordinator {
private:
	std::vector<CoordinatorWorker> workers;

	Coordinator();
	// Do not implement
	Coordinator( Coordinator const& );
	void operator=( Coordinator const& );

	void free();

public:
	struct {
		GlobalConfig global;
		CoordinatorConfig coordinator;
	} config;
	struct {
		CoordinatorSocket self;
		EPoll epoll;
		std::vector<MasterSocket> masters;
		std::vector<SlaveSocket> slaves;
	} sockets;
	CoordinatorEventQueue eventQueue;
	
	static Coordinator *getInstance() {
		static Coordinator coordinator;
		return &coordinator;
	}

	bool init( char *path, bool verbose );
	bool start();
	bool stop();
	void print( FILE *f = stdout );
};

#endif
