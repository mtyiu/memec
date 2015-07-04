#ifndef __COORDINATOR_CONFIG_HH__
#define __COORDINATOR_CONFIG_HH__

#include "../../common/config/config.hh"

class CoordinatorConfig : public Config {
public:
	bool set( const char *section, const char *name, const char *value );
	bool validate();
};

#endif
