#ifndef __CLIENT_PROTOCOL_PROTOCOL_HH__
#define __CLIENT_PROTOCOL_PROTOCOL_HH__

#include "../../common/config/server_addr.hh"
#include "../../common/ds/array_map.hh"
#include "../../common/ds/bitmask_array.hh"
#include "../../common/ds/latency.hh"
#include "../../common/ds/sockaddr_in.hh"
#include "../../common/protocol/protocol.hh"

class ClientProtocol : public Protocol {
public:
	ClientProtocol() : Protocol( ROLE_CLIENT ) {}

	char *reqPushLoadStats(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		ArrayMap<struct sockaddr_in, Latency> *serverGetLatency,
		ArrayMap<struct sockaddr_in, Latency> *serverSetLatency
	);

	bool parseLoadingStats(
		const LoadStatsHeader& loadStatsHeader,
		ArrayMap<struct sockaddr_in, Latency> &serverGetLatency,
		ArrayMap<struct sockaddr_in, Latency> &serverSetLatency,
		std::set<struct sockaddr_in> &overloadedServerSet,
		char* buffer, uint32_t size
	);

	char *reqSet(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		char *key, uint8_t keySize,
		char *value, uint32_t valueSize,
		char *buf = 0
	);

	char *reqGet(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		char *key, uint8_t keySize
	);

	char *reqUpdate(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		char *key, uint8_t keySize,
		char *valueUpdate, uint32_t valueUpdateOffset, uint32_t valueUpdateSize,
		uint32_t timestamp = 0,
		bool checkGetChunk = false
	);

	char *reqDelete(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		char *key, uint8_t keySize,
		uint32_t timestamp = 0,
		bool checkGetChunk = false
	);
};

#endif
