#ifndef __MASTER_REMAP_BASIC_REMAP_SCHEME_HH__
#define __MASTER_REMAP_BASIC_REMAP_SCHEME_HH__

#include "../socket/slave_socket.hh"
#include "../ds/stats.hh"
#include "../../common/stripe_list/stripe_list.hh"

class BasicRemappingScheme {
public:
	static void getRemapTarget( uint32_t originalListId, uint32_t originalChunkId, uint32_t &remappedListId, uint32_t &remappedChunkId, uint32_t dataCount, uint32_t parityCount );

	static SlaveLoading *slaveLoading;
	static OverloadedSlave *overloadedSlave;
	static StripeList<SlaveSocket> *stripeList;

};

#endif
