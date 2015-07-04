#ifndef __MASTER_HH__
#define __MASTER_HH__

#include <cstdio>
#include "../config/master_config.hh"
// #include "../socket/coordinator_socket.hh"
#include "../socket/master_socket.hh"
#include "../../common/config/global_config.hh"

// Implement the singleton pattern
class Master {
private:
	MasterSocket socket;

	Master();
	// Do not implement
	Master( Master const& );
	void operator=( Master const& );

public:
	struct {
		GlobalConfig global;
		MasterConfig master;
	} config;
	
	static Master *getInstance() {
		static Master master;
		return &master;
	}

	bool init( char *path, bool verbose );
	bool start();
	bool stop();
	void print( FILE *f = stdout );
};

#endif
