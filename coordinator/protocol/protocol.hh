#ifndef __COORDINATOR_PROTOCOL_PROTOCOL_HH__
#define __COORDINATOR_PROTOCOL_PROTOCOL_HH__

#include <unordered_map>
#include "../socket/server_socket.hh"
#include "../../common/ds/latency.hh"
#include "../../common/protocol/protocol.hh"
#include "../../common/config/server_addr.hh"
#include "../../common/stripe_list/stripe_list.hh"

class CoordinatorProtocol : public Protocol {
public:
	CoordinatorProtocol() : Protocol( ROLE_COORDINATOR ) {}

	char *reqPushLoadStats(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		ArrayMap< struct sockaddr_in, Latency > *serverGetLatency,
		ArrayMap< struct sockaddr_in, Latency > *serverSetLatency,
		std::set< struct sockaddr_in > *overloadedServerSet
	);

	bool parseLoadingStats(
		const LoadStatsHeader& loadStatsHeader,
		ArrayMap< struct sockaddr_in, Latency >& serverGetLatency,
		ArrayMap< struct sockaddr_in, Latency >& serverSetLatency,
		char* buffer, uint32_t size
	);

	char *announceServerReconstructed(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		ServerSocket *srcSocket, ServerSocket *dstSocket,
		bool toServer
	);

	char *addNewServer(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		char *name, uint8_t nameLen,
		ServerSocket *socket, bool toServer
	);

	char *updateStripeList(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		StripeList<ServerSocket> *stripeList, bool isMigrating, bool toServer
	);
};

#endif
