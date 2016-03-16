#ifndef __CLIENT_CONFIG_CLIENT_CONFIG_HH__
#define __CLIENT_CONFIG_CLIENT_CONFIG_HH__

#include <vector>
#include <stdint.h>
#include "../../common/config/server_addr.hh"
#include "../../common/config/config.hh"
#include "../../common/config/global_config.hh"
#include "../../common/worker/worker_type.hh"

class ClientConfig : public Config {
public:
	struct {
		ServerAddr addr;
	} client;
	struct {
		bool disabled;
	} degraded;
	struct {
		uint32_t ackTimeout;
	} states;
	struct {
		uint32_t ackBatchSize;
	} backup;

	ClientConfig();
	bool parse( const char *path );
	bool set( const char *section, const char *name, const char *value );
	bool validate();
	void print( FILE *f = stdout );
};

#endif
