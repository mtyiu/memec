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
	} stripeLists;
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
		uint32_t prioritized;
	} eventQueue;
	struct {
		uint32_t packets;
	} pool;
	struct {
		uint32_t metadata;
		uint32_t load;
	} timeout;
	std::vector<ServerAddr> coordinators;
	std::vector<ServerAddr> servers;
	struct {
		CodingScheme scheme;
		CodingParams params;
	} coding;
	struct {
		bool disabled;
		ServerAddr spreaddAddr;
		uint16_t workers;
		uint32_t queue;
		float smoothingFactor;
	} states;
	struct {
		bool disabled;
	} backup;

	GlobalConfig();
	bool parse( const char *path );
	bool set( const char *section, const char *name, const char *value );
	bool validate();
	void print( FILE *f = stdout );
};

#endif
