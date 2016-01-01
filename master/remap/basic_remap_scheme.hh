#ifndef __MASTER_REMAP_BASIC_REMAP_SCHEME_HH__
#define __MASTER_REMAP_BASIC_REMAP_SCHEME_HH__

#include "remap_msg_handler.hh"
#include "../socket/slave_socket.hh"
#include "../ds/stats.hh"
#include "../../common/lock/lock.hh"
#include "../../common/stripe_list/stripe_list.hh"

class BasicRemappingScheme {
public:
	static void getRemapTarget(
		uint32_t *&original, uint32_t *&remapped, uint32_t &remappedCount,
		uint32_t dataChunkCount, uint32_t parityChunkCount,
		SlaveSocket **dataSlaveSockets, SlaveSocket **paritySlaveSockets
	);
	static void getDegradedOpTarget( uint32_t listId, uint32_t originalChunkId, uint32_t &newChunkId, uint32_t dataCount, uint32_t parityCount, SlaveSocket **data, SlaveSocket **parity );
	static bool isOverloaded( SlaveSocket *socket );

	static SlaveLoading *slaveLoading;
	static OverloadedSlave *overloadedSlave;
	static StripeList<SlaveSocket> *stripeList;
	static MasterRemapMsgHandler *remapMsgHandler;
	static Latency increment;

	static pthread_mutex_t lock;
	static uint32_t remapped;
	static uint32_t lockonly;

};

#endif
