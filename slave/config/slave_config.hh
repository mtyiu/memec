#ifndef __SLAVE_CONFIG_SLAVE_CONFIG_HH__
#define __SLAVE_CONFIG_SLAVE_CONFIG_HH__

#include <vector>
#include <stdint.h>
#include "../storage/storage_type.hh"
#include "../../common/config/server_addr.hh"
#include "../../common/config/config.hh"
#include "../../common/config/global_config.hh"
#include "../../common/util/option.hh"
#include "../../common/worker/worker_type.hh"

class SlaveConfig : public Config {
public:
	struct {
		ServerAddr addr;
	} slave;
	struct {
		uint32_t timeout;
	} slavePeers;
	struct {
		uint32_t maxEvents;
		int32_t timeout;
	} epoll;
	struct {
		uint64_t chunks;
	} pool;
	struct {
		WorkerType type;
		struct {
			uint8_t mixed;
			struct {
				uint16_t total;
				uint8_t coding;
				uint8_t coordinator;
				uint8_t io;
				uint8_t master;
				uint8_t slave;
				uint8_t slavePeer;
			} separated;
		} number;
	} workers;
	struct {
		bool block;
		struct {
			uint32_t mixed;
			uint32_t pMixed;
			struct {
				uint32_t coding;
				uint32_t coordinator;
				uint32_t io;
				uint32_t master;
				uint32_t slave;
				uint32_t slavePeer;
			} separated;
		} size;
	} eventQueue;
	struct {
		bool disabled;
	} seal;
	struct {
		StorageType type;
		char path[ STORAGE_PATH_MAX ];
	} storage;

	SlaveConfig();
	bool merge( GlobalConfig &globalConfig );
	bool parse( const char *path );
	bool override( OptionList &options );
	bool set( const char *section, const char *name, const char *value );
	bool validate();
	int validate( std::vector<ServerAddr> slaves );
	int validate( std::vector<ServerAddr> slaves, ServerAddr &addr );
	void print( FILE *f = stdout );
};

#endif
