#ifndef __SLAVE_MAIN_SLAVE_HH__
#define __SLAVE_MAIN_SLAVE_HH__

#include <vector>
#include <cstdio>
#include "../config/slave_config.hh"
#include "../socket/coordinator_socket.hh"
#include "../socket/master_socket.hh"
#include "../socket/slave_socket.hh"
#include "../../common/config/global_config.hh"

// Implement the singleton pattern
class Slave {
private:
	struct {
		SlaveSocket self;
		std::vector<CoordinatorSocket> coordinators;
		std::vector<MasterSocket> masters;
		std::vector<SlaveSocket> slaves;
	} sockets;

	Slave();
	// Do not implement
	Slave( Slave const& );
	void operator=( Slave const& );

public:
	struct {
		GlobalConfig global;
		SlaveConfig slave;
	} config;
	
	static Slave *getInstance() {
		static Slave slave;
		return &slave;
	}

	bool init( char *path, bool verbose );
	bool start();
	bool stop();
	void print( FILE *f = stdout );
};

#endif
