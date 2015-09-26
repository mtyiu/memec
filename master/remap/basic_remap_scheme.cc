#include "basic_remap_scheme.hh"
#include "../../common/ds/sockaddr_in.hh"

SlaveLoading *BasicRemappingScheme::slaveLoading = NULL;
OverloadedSlave *BasicRemappingScheme::overloadedSlave = NULL;
StripeList<SlaveSocket> *BasicRemappingScheme::stripeList = NULL;
MasterRemapMsgHandler *BasicRemappingScheme::remapMsgHandler = NULL;

void BasicRemappingScheme::getRemapTarget( uint32_t originalListId, uint32_t originalChunkId, uint32_t &remappedListId, uint32_t &remappedChunkId, uint32_t dataCount, uint32_t parityCount ) {
	int index = -1, leastOverloadedId = -1;
	struct sockaddr_in slaveAddr;
	Latency *targetLatency, *nodeLatency, *overloadLatency;
	SlaveSocket **data, **parity;

	// baseline: no remapping
	remappedChunkId = originalChunkId;
	remappedListId = originalListId;
	leastOverloadedId = originalChunkId;

	if ( slaveLoading == NULL || overloadedSlave == NULL || stripeList == NULL || remapMsgHandler == NULL ) {
		fprintf( stderr, "The scheme is not yet initialized!! Abort remapping!!\n" );
		return;
	}

	// check if remapping is allowed
	if ( ! remapMsgHandler->allowRemapping() ) 
		return;

	// get the original mapped stripe list 
	data = new SlaveSocket*[ dataCount ];
	parity = new SlaveSocket*[ parityCount ];
	parity = stripeList->get( originalListId, parity, data );

	// TODO avoid locking?
	pthread_mutex_lock( &slaveLoading->lock );
	pthread_mutex_lock( &overloadedSlave->lock );

	slaveAddr = data[ originalChunkId ]->getAddr();
	targetLatency = slaveLoading->cumulative.set.get( slaveAddr, &index );
	overloadLatency = targetLatency;
	// cannot determine whether remap is necessary??
	if ( index == -1 ) 
		goto exit;

	if ( dataCount < 2 ) {
		// need a new stripe list if the list only consist of one node
		leastOverloadedId = originalListId;
		// search all stripe lists 
		for ( uint32_t listIndex = 0; listIndex < stripeList->getNumList(); listIndex++ ) {
			parity = stripeList->get( listIndex, parity, data );
			slaveAddr = data[ 0 ]->getAddr();
			nodeLatency = slaveLoading->cumulative.set.get( slaveAddr , &index );

			// TODO if this node had not been accessed, should we take the risk and try?
			if ( index == -1 ) 
				continue;

			if ( overloadedSlave->slaveSet.count( slaveAddr ) > 0 ) {
				if ( *overloadLatency < *nodeLatency ) {
					overloadLatency = nodeLatency;
					leastOverloadedId = listIndex;
				}
			} else if ( *nodeLatency < *targetLatency ) {
				targetLatency = nodeLatency;
				remappedListId = listIndex;
			}
		}
		if ( remappedListId == originalListId )
			remappedListId = leastOverloadedId;
		
	} else {
		// search the least-loaded node with the stripe list
		// TODO  move to more efficient and scalable data structure e.g. min-heap
		for ( uint32_t chunkIndex = 0; chunkIndex < dataCount; chunkIndex++ ) {
			// skip the original node
			if ( chunkIndex == originalChunkId )
				continue;
			slaveAddr = data[ chunkIndex ]->getAddr();
			nodeLatency = slaveLoading->cumulative.set.get( slaveAddr, &index );

			// TODO if this node had not been accessed, should we take the risk and try?
			if ( index == -1 ) 
				continue;
			if ( overloadedSlave->slaveSet.count( slaveAddr ) > 0 ) {
				// scan for the least overloaded node, in case all nodes are overloaded
				if ( *overloadLatency < *nodeLatency ) {
					overloadLatency = nodeLatency;
					leastOverloadedId = chunkIndex;
				}
			} else if ( *nodeLatency < *targetLatency ) {
				targetLatency = nodeLatency;
				remappedChunkId = chunkIndex;
			}
		}
		if ( remappedChunkId == originalChunkId )
			remappedChunkId = leastOverloadedId;
	}

exit:

	pthread_mutex_unlock( &overloadedSlave->lock );
	pthread_mutex_unlock( &slaveLoading->lock );
	fprintf( stderr, "Remap Result from list=%u chunk=%u to list=%u chunk=%u\n", originalListId, originalChunkId, remappedListId, remappedChunkId );

	delete data;
	delete parity;
}

