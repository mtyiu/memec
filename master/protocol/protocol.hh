#ifndef __MASTER_PROTOCOL_PROTOCOL_HH__
#define __MASTER_PROTOCOL_PROTOCOL_HH__

#include "../../common/config/server_addr.hh"
#include "../../common/ds/array_map.hh"
#include "../../common/ds/bitmask_array.hh"
#include "../../common/ds/latency.hh"
#include "../../common/ds/sockaddr_in.hh"
#include "../../common/protocol/protocol.hh"

class MasterProtocol : public Protocol {
public:
	MasterProtocol() : Protocol( ROLE_MASTER ) {}

	// ---------- register_protocol.cc ----------
	char *reqRegisterCoordinator( size_t &size, uint32_t id, uint32_t addr, uint16_t port );
	char *reqRegisterSlave( size_t &size, uint32_t id, uint32_t addr, uint16_t port );
	char *resRegisterApplication( size_t &size, uint32_t id, bool success );

	// ---------- load_protocol.cc ----------
	char *reqPushLoadStats(
		size_t &size, uint32_t id,
		ArrayMap<struct sockaddr_in, Latency> *slaveGetLatency,
		ArrayMap<struct sockaddr_in, Latency> *slaveSetLatency
	);
	bool parseLoadingStats(
		const LoadStatsHeader& loadStatsHeader,
		ArrayMap<struct sockaddr_in, Latency> &slaveGetLatency,
		ArrayMap<struct sockaddr_in, Latency> &slaveSetLatency,
		std::set<struct sockaddr_in> &overloadedSlaveSet,
		char* buffer, uint32_t size
	);

	// ---------- normal_protocol.cc ----------
	char *reqSet(
		size_t &size, uint32_t id,
		char *key, uint8_t keySize,
		char *value, uint32_t valueSize,
		char *buf = 0
	);
	char *reqGet(
		size_t &size, uint32_t id,
		char *key, uint8_t keySize
	);
	char *reqUpdate(
		size_t &size, uint32_t id,
		char *key, uint8_t keySize,
		char *valueUpdate, uint32_t valueUpdateOffset, uint32_t valueUpdateSize
	);
	char *reqDelete(
		size_t &size, uint32_t id,
		char *key, uint8_t keySize
	);

	char *resSet(
		size_t &size, uint32_t id, bool success,
		uint8_t keySize, char *key
	);
	char *resGet(
		size_t &size, uint32_t id, bool success,
		uint8_t keySize, char *key,
		uint32_t valueSize = 0, char *value = 0
	);
	char *resUpdate(
		size_t &size, uint32_t id, bool success,
		uint8_t keySize, char *key,
		uint32_t valueUpdateOffset, uint32_t valueUpdateSize
	);
	char *resDelete(
		size_t &size, uint32_t id, bool success,
		uint8_t keySize, char *key
	);

	// ---------- remap_protocol.cc ----------
	char *reqRemappingSetLock(
		size_t &size, uint32_t id,
		uint32_t listId, std::vector<uint32_t> chunkId,
		uint32_t reqRemapState,
		char *key, uint8_t keySize,
		uint32_t sockfd = UINT_MAX
	);
	char *reqRemappingSet(
		size_t &size, uint32_t id,
		uint32_t listId, uint32_t chunkId,
		char *key, uint8_t keySize,
		char *value, uint32_t valueSize,
		char *buf = 0,
		uint32_t sockfd = UINT_MAX,
		bool isParity = false,
		struct sockaddr_in *target = 0
	);
	char *resSyncRemappingRecords( size_t &size, uint32_t id );

	// ---------- degraded_protocol.cc ----------
	char *reqDegradedLock(
		size_t &size, uint32_t id,
		uint32_t listId,
		uint32_t srcDataChunkId, uint32_t dstDataChunkId,
		uint32_t srcParityChunkId, uint32_t dstParityChunkId,
		char *key, uint8_t keySize
	);
	char *reqDegradedGet(
		size_t &size, uint32_t id,
		uint32_t listId, uint32_t stripeId,
		uint32_t srcDataChunkId, uint32_t dstDataChunkId,
		uint32_t srcParityChunkId, uint32_t dstParityChunkId,
		bool isSealed, char *key, uint8_t keySize
	);
	char *reqDegradedUpdate(
		size_t &size, uint32_t id,
		uint32_t listId, uint32_t stripeId,
		uint32_t srcDataChunkId, uint32_t dstDataChunkId,
		uint32_t srcParityChunkId, uint32_t dstParityChunkId,
		bool isSealed, char *key, uint8_t keySize,
		char *valueUpdate, uint32_t valueUpdateOffset, uint32_t valueUpdateSize
	);
	char *reqDegradedDelete(
		size_t &size, uint32_t id,
		uint32_t listId, uint32_t stripeId,
		uint32_t srcDataChunkId, uint32_t dstDataChunkId,
		uint32_t srcParityChunkId, uint32_t dstParityChunkId,
		bool isSealed, char *key, uint8_t keySize
	);
};

#endif
