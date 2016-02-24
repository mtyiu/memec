#ifndef __MASTER_REMAP_BASIC_REMAP_SCHEME_HH__
#define __MASTER_REMAP_BASIC_REMAP_SCHEME_HH__

#include "remap_msg_handler.hh"
#include "../socket/slave_socket.hh"
#include "../ds/stats.hh"
#include "../../common/lock/lock.hh"
#include "../../common/stripe_list/stripe_list.hh"

class BasicRemappingScheme {
public:
	static void redirect(
		uint32_t *original, uint32_t *remapped, uint32_t numEntries, uint32_t &remappedCount,
		uint32_t dataChunkCount, uint32_t parityChunkCount,
		SlaveSocket **dataSlaveSockets, SlaveSocket **paritySlaveSockets,
		bool isGet
	);
	static bool isOverloaded( SlaveSocket *socket );

	static SlaveLoading *slaveLoading;
	static OverloadedSlave *overloadedSlave;
	static StripeList<SlaveSocket> *stripeList;
	static MasterRemapMsgHandler *remapMsgHandler;
	static Latency increment;
};

#endif
