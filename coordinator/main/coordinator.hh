#ifndef __COORDINATOR_HH__
#define __COORDINATOR_HH__

#include <cstdio>
#include "../config/coordinator_config.hh"
#include "../socket/coordinator_socket.hh"
#include "../../common/config/global_config.hh"

// Implement the singleton pattern
class Coordinator {
private:
	CoordinatorSocket socket;

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
