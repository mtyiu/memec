#include "basic_remap_scheme.hh"

SlaveLoading *BasicRemappingScheme::slaveLoading = NULL;
OverloadedSlave *BasicRemappingScheme::overloadedSlave = NULL;
StripeList<SlaveSocket> *BasicRemappingScheme::stripeList = NULL;

void BasicRemappingScheme::getRemapTarget( uint32_t originalListId, uint32_t originalChunkId, uint32_t &remappedListId, uint32_t &remappedChunkId, uint32_t dataCount, uint32_t parityCount ) {
	// best effort without locking
	SlaveSocket** data = new SlaveSocket*[ dataCount ];
	SlaveSocket** parity = new SlaveSocket*[ parityCount ];

	if ( dataCount < 2 ) {
		remappedChunkId = originalChunkId;
	} else {
		remappedListId = originalListId;
	}

	delete data;
	delete parity;
}

