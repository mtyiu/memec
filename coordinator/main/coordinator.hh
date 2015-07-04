#ifndef __COORDINATOR_HH__
#define __COORDINATOR_HH__

#include "../config/coordinator_config.hh"
#include "../../common/config/global_config.hh"

class Coordinator {
private:
	struct {
		GlobalConfig global;
	} config;

	Coordinator();
	// Do not implement
	Coordinator( Coordinator const& );
	void operator=( Coordinator const& );

public:
	static Coordinator *getInstance() {
		static Coordinator coordinator;
		return &coordinator;
	}

	bool init( char *path, bool verbose );
	bool start();
	bool stop();
};

#endif
