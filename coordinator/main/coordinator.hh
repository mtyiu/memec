#ifndef __COORDINATOR_MAIN_COORDINATOR_HH__
#define __COORDINATOR_MAIN_COORDINATOR_HH__

#include <vector>
#include <cstdio>
#include "../config/coordinator_config.hh"
#include "../event/mixed_event.hh"
#include "../event/application_event.hh"
#include "../event/coordinator_event.hh"
#include "../event/master_event.hh"
#include "../event/slave_event.hh"
#include "../socket/coordinator_socket.hh"
#include "../socket/master_socket.hh"
#include "../socket/slave_socket.hh"
#include "../../common/config/global_config.hh"
#include "../../common/event/event_queue.hh"
#include "../../common/socket/epoll.hh"

// Implement the singleton pattern
class Coordinator {
private:
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
	union {
		EventQueue<MixedEvent> *mixed;
		struct {
			EventQueue<ApplicationEvent> *application;
			EventQueue<CoordinatorEvent> *coordinator;
			EventQueue<MasterEvent> *master;
			EventQueue<SlaveEvent> *slave;
		} separated;
	} eventQueue;
	
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
