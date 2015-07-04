#ifndef __COORDINATOR_CONFIG_HH__
#define __COORDINATOR_CONFIG_HH__

#include <vector>
#include "../../common/config/server_addr.hh"
#include "../../common/config/config.hh"

class CoordinatorConfig : public Config {
public:
	ServerAddr addr;

	bool parse( const char *path );
	bool set( const char *section, const char *name, const char *value );
	bool validate();
	bool validate( std::vector<ServerAddr> coordinators );
	void print( FILE *f = stdout );
};

#endif
