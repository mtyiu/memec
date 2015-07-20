#ifndef __APPLICATION_CONFIG_APPLICATION_CONFIG_HH__
#define __APPLICATION_CONFIG_APPLICATION_CONFIG_HH__

#include <vector>
#include <stdint.h>
#include "../../common/config/server_addr.hh"
#include "../../common/config/config.hh"
#include "../../common/worker/worker_type.hh"

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
		WorkerType type;
		struct {
			uint8_t mixed;
			struct {
				uint16_t total;
				uint8_t application;
				uint8_t master;
			} separated;
		} number;
	} workers;
	struct {
		bool block;
		struct {
			uint32_t mixed;
			struct {
				uint32_t application;
				uint32_t master;
			} separated;
		} size;
	} eventQueue;
	std::vector<ServerAddr> masters;

	bool parse( const char *path );
	bool set( const char *section, const char *name, const char *value );
	bool validate();
	void print( FILE *f = stdout );
};

#endif
