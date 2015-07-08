#ifndef __MASTER_CONFIG_MASTER_CONFIG_HH__
#define __MASTER_CONFIG_MASTER_CONFIG_HH__

#include <vector>
#include <stdint.h>
#include "../../common/config/server_addr.hh"
#include "../../common/config/config.hh"
#include "../../common/config/global_config.hh"
#include "../../common/event/event_queue_type.hh"

class MasterConfig : public Config {
public:
	struct {
		ServerAddr addr;
		uint8_t workers;
	} master;
	struct {
		uint32_t maxEvents;
		int32_t timeout;
	} epoll;
	struct {
		EventQueueType type;
		bool block;
		struct {
			uint32_t mixed;
			uint32_t application;
			uint32_t coordinator;
			uint32_t master;
			uint32_t slave;
		} size;
	} eventQueue;

	bool merge( GlobalConfig &globalConfig );
	bool parse( const char *path );
	bool set( const char *section, const char *name, const char *value );
	bool validate();
	void print( FILE *f = stdout );
};

#endif
