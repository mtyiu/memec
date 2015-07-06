#ifndef __COORDINATOR_CONFIG_COORDINATOR_CONFIG_HH__
#define __COORDINATOR_CONFIG_COORDINATOR_CONFIG_HH__

#include <vector>
#include <stdint.h>
#include "../../common/config/server_addr.hh"
#include "../../common/config/config.hh"
#include "../../common/config/global_config.hh"

class CoordinatorConfig : public Config {
public:
	uint32_t epollMaxEvents;
	int32_t epollTimeout;
	ServerAddr addr;

	bool merge( GlobalConfig &globalConfig );
	bool parse( const char *path );
	bool set( const char *section, const char *name, const char *value );
	bool validate();
	bool validate( std::vector<ServerAddr> coordinators );
	void print( FILE *f = stdout );
};

#endif
