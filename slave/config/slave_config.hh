#ifndef __SLAVE_CONFIG_COORDINATOR_CONFIG_HH__
#define __SLAVE_CONFIG_COORDINATOR_CONFIG_HH__

#include <vector>
#include <stdint.h>
#include "../../common/config/server_addr.hh"
#include "../../common/config/config.hh"
#include "../../common/config/global_config.hh"
#include "../../common/event/event_queue_type.hh"

class SlaveConfig : public Config {
public:
	struct {
		ServerAddr addr;
		uint8_t workers;
	} slave;
	struct {
		uint32_t maxEvents;
		int32_t timeout;
	} epoll;
	struct {
		EventQueueType type;
		bool block;
		struct {
			uint32_t mixed;
			struct {
				uint32_t application;
				uint32_t coordinator;
				uint32_t master;
				uint32_t slave;
			} separated;
		} size;
	} eventQueue;

	bool merge( GlobalConfig &globalConfig );
	bool parse( const char *path );
	bool set( const char *section, const char *name, const char *value );
	bool validate();
	bool validate( std::vector<ServerAddr> slaves );
	void print( FILE *f = stdout );
};

#endif
