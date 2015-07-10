#ifndef __SLAVE_MAIN_SLAVE_HH__
#define __SLAVE_MAIN_SLAVE_HH__

#include <vector>
#include <cstdio>
#include "../config/slave_config.hh"
#include "../event/slave_event_queue.hh"
#include "../socket/coordinator_socket.hh"
#include "../socket/master_socket.hh"
#include "../socket/slave_socket.hh"
#include "../worker/worker.hh"
#include "../../common/config/global_config.hh"
#include "../../common/socket/epoll.hh"
#include "../../common/signal/signal.hh"
#include "../../common/util/time.hh"

// Implement the singleton pattern
class Slave {
private:
	bool isRunning;
	struct timespec startTime;
	std::vector<SlaveWorker> workers;

	Slave();
	// Do not implement
	Slave( Slave const& );
	void operator=( Slave const& );

	void free();
	// Commands
	void help();
	void info();
	void time();

public:
	struct {
		GlobalConfig global;
		SlaveConfig slave;
	} config;
	struct {
		SlaveSocket self;
		EPoll epoll;
		std::vector<CoordinatorSocket> coordinators;
		std::vector<MasterSocket> masters;
		std::vector<SlaveSocket> slaves;
	} sockets;
	SlaveEventQueue eventQueue;
	
	static Slave *getInstance() {
		static Slave slave;
		return &slave;
	}

	static void signalHandler( int signal );

	bool init( char *path, bool verbose );
	bool start();
	bool stop();
	void print( FILE *f = stdout );
	void debug( FILE *f = stdout );
	double getElapsedTime();
	void interactive();
};

#endif
