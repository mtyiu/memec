#ifndef __COORDINATOR_MAIN_COORDINATOR_HH__
#define __COORDINATOR_MAIN_COORDINATOR_HH__

#include <vector>
#include <cstdio>
#include "../config/coordinator_config.hh"
#include "../socket/coordinator_socket.hh"
#include "../socket/master_socket.hh"
#include "../socket/slave_socket.hh"
#include "../../common/config/global_config.hh"

// Implement the singleton pattern
class Coordinator {
private:
	struct {
		CoordinatorSocket self;
		std::vector<MasterSocket> masters;
		std::vector<SlaveSocket> slaves;
	} sockets;

	Coordinator();
	// Do not implement
	Coordinator( Coordinator const& );
	void operator=( Coordinator const& );

public:
	struct {
		GlobalConfig global;
		CoordinatorConfig coordinator;
	} config;
	
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
