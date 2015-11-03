#ifndef __MASTER_REMAP_BASIC_REMAP_SCHEME_HH__
#define __MASTER_REMAP_BASIC_REMAP_SCHEME_HH__

#include "remap_msg_handler.hh"
#include "../socket/slave_socket.hh"
#include "../ds/stats.hh"
#include "../../common/lock/lock.hh"
#include "../../common/stripe_list/stripe_list.hh"

class BasicRemappingScheme {
public:
	static void getRemapTarget( uint32_t originalListId, uint32_t originalChunkId, uint32_t &remappedListId, uint32_t &remappedChunkId, uint32_t dataCount, uint32_t parityCount, SlaveSocket **data, SlaveSocket **parity );
	static void getDegradedOpTarget( uint32_t listId, uint32_t originalChunkId, uint32_t &newChunkId, uint32_t dataCount, uint32_t parityCount, SlaveSocket **data, SlaveSocket **parity );
	static bool isOverloaded( SlaveSocket *socket );

	static SlaveLoading *slaveLoading;
	static OverloadedSlave *overloadedSlave;
	static StripeList<SlaveSocket> *stripeList;
	static MasterRemapMsgHandler *remapMsgHandler;
	static Latency increment;

	static LOCK_T lock;
	static uint32_t remapped;

};

#endif
