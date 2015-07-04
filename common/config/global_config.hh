#ifndef __GLOBAL_CONFIG_HH__
#define __GLOBAL_CONFIG_HH__

#include <vector>
#include <cstdio>
#include <stdint.h>
#include "config.hh"
#include "server_addr.hh"
#include "../coding/coding_scheme.hh"
#include "../coding/coding_params.hh"

class GlobalConfig : public Config {
public:
	uint32_t keySize;
	uint32_t chunkSize;
	uint32_t epollMaxEvents;
	int32_t epollTimeout;
	CodingScheme codingScheme;
	CodingParams codingParams;
	std::vector<ServerAddr> coordinators;
	std::vector<ServerAddr> slaves;
	
	bool parse( const char *path );
	bool set( const char *section, const char *name, const char *value );
	bool validate();
	void print( FILE *f = stdout );
};

#endif
