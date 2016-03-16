#ifndef __APPLICATION_CONFIG_APPLICATION_CONFIG_HH__
#define __APPLICATION_CONFIG_APPLICATION_CONFIG_HH__

#include <vector>
#include <stdint.h>
#include "../../common/config/server_addr.hh"
#include "../../common/config/config.hh"

class ApplicationConfig : public Config {
public:
	struct {
		uint32_t key;
		uint32_t chunk;
	} size;
	struct {
		uint32_t maxEvents;
		int32_t timeout;
	} epoll;
	struct {
		uint16_t count;
	} workers;
	struct {
		bool block;
		uint32_t size;
	} eventQueue;
	std::vector<ServerAddr> clients;

	ApplicationConfig();
	bool parse( const char *path );
	bool set( const char *section, const char *name, const char *value );
	bool validate();
	void print( FILE *f = stdout );
};

#endif
