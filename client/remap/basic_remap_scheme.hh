#ifndef __CLIENT_REMAP_BASIC_REMAP_SCHEME_HH__
#define __CLIENT_REMAP_BASIC_REMAP_SCHEME_HH__

#include "remap_msg_handler.hh"
#include "../socket/server_socket.hh"
#include "../ds/stats.hh"
#include "../../common/lock/lock.hh"
#include "../../common/stripe_list/stripe_list.hh"

class BasicRemappingScheme {
public:
	static void redirect(
		uint32_t *original, uint32_t *remapped, uint32_t numEntries, uint32_t &remappedCount,
		uint32_t dataChunkCount, uint32_t parityChunkCount,
		ServerSocket **dataServerSockets, ServerSocket **parityServerSockets,
		bool isGet
	);
	static bool isOverloaded( ServerSocket *socket );

	static ServerLoading *serverLoading;
	static OverloadedServer *overloadedServer;
	static StripeList<ServerSocket> *stripeList;
	static ClientRemapMsgHandler *remapMsgHandler;
	static Latency increment;
};

#endif
