#ifndef __COORDINATOR_CONFIG_COORDINATOR_CONFIG_HH__
#define __COORDINATOR_CONFIG_COORDINATOR_CONFIG_HH__

#include <vector>
#include <stdint.h>
#include "../../common/config/server_addr.hh"
#include "../../common/config/config.hh"
#include "../../common/config/global_config.hh"
#include "../../common/util/option.hh"
#include "../../common/worker/worker_type.hh"

class CoordinatorConfig : public Config {
public:
	struct {
		ServerAddr addr;
	} coordinator;
	struct {
		bool isManual;
		uint16_t maximum;
		struct {
			float start;
			float stop;
			float overload;
		} threshold;
	} states;

	CoordinatorConfig();
	bool parse( const char *path );
	bool override( OptionList &options );
	bool set( const char *section, const char *name, const char *value );
	bool validate();
	bool validate( std::vector<ServerAddr> coordinators );
	void print( FILE *f = stdout );
};

#endif
