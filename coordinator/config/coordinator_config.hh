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
		uint32_t maxEvents;
		int32_t timeout;
	} epoll;
	struct {
		WorkerType type;
		struct {
			uint8_t mixed;
			struct {
				uint16_t total;
				uint8_t coordinator;
				uint8_t master;
				uint8_t slave;
			} separated;
		} number;
	} workers;
	struct {
		bool block;
		struct {
			uint32_t mixed;
			uint32_t pMixed;
			struct {
				uint32_t coordinator;
				uint32_t master;
				uint32_t slave;
			} separated;
		} size;
	} eventQueue;
	struct {
		uint32_t updateInterval;
	} loadingStats;
	struct {
		uint32_t packets;
	} pool;
	struct {
		uint32_t worker;
		uint32_t queue;
	} remap;

	CoordinatorConfig();
	bool merge( GlobalConfig &globalConfig );
	bool parse( const char *path );
	bool override( OptionList &options );
	bool set( const char *section, const char *name, const char *value );
	bool validate();
	bool validate( std::vector<ServerAddr> coordinators );
	void print( FILE *f = stdout );
};

#endif
