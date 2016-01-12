#ifndef __COMMON_CONFIG_GLOBAL_CONFIG_HH__
#define __COMMON_CONFIG_GLOBAL_CONFIG_HH__

#include <vector>
#include <cstdio>
#include <stdint.h>
#include "config.hh"
#include "server_addr.hh"
#include "../coding/coding_scheme.hh"
#include "../coding/coding_params.hh"

class GlobalConfig : public Config {
public:
	struct {
		uint32_t key;
		uint32_t chunk;
	} size;
	struct {
		uint32_t count;
	} stripeList;
	struct {
		uint32_t maxEvents;
		int32_t timeout;
	} epoll;
	struct {
		uint32_t timeout;
	} sync;
	std::vector<ServerAddr> coordinators;
	std::vector<ServerAddr> slaves;
	struct {
		bool enabled;
		ServerAddr spreaddAddr;
		float startThreshold;
		float stopThreshold;
		float overloadThreshold;
		float smoothingFactor;
		uint32_t maximum;
		bool manual; // manual overload instead of using load stats 
	} remap;
	struct {
		uint16_t chunksPerList;
	} buffer;
	struct {
		CodingScheme scheme;
		CodingParams params;
	} coding;

	GlobalConfig();
	bool parse( const char *path );
	bool set( const char *section, const char *name, const char *value );
	bool validate();
	void print( FILE *f = stdout );
};

#endif
