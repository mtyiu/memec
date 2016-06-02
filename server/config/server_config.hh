#ifndef __SERVER_CONFIG_SERVER_CONFIG_HH__
#define __SERVER_CONFIG_SERVER_CONFIG_HH__

#include <vector>
#include <stdint.h>
#include "../storage/storage_type.hh"
#include "../../common/config/server_addr.hh"
#include "../../common/config/config.hh"
#include "../../common/config/global_config.hh"

class ServerConfig : public Config {
public:
	struct {
		ServerAddr addr;
	} server;
	struct {
		uint64_t chunks;
	} pool;
	struct {
		uint32_t chunksPerList;
	} buffer;
	struct {
		bool disabled;
	} seal;
	struct {
		StorageType type;
		char path[ STORAGE_PATH_MAX ];
	} storage;

	ServerConfig();
	bool parse( const char *path );
	bool set( const char *section, const char *name, const char *value );
	bool validate();
	int validate( std::vector<ServerAddr> servers );
	int validate( std::vector<ServerAddr> servers, ServerAddr &addr );
	void print( FILE *f = stdout );
};

#endif
