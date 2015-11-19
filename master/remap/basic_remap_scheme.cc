#include "basic_remap_scheme.hh"
#include "../../common/ds/sockaddr_in.hh"

SlaveLoading *BasicRemappingScheme::slaveLoading = NULL;
OverloadedSlave *BasicRemappingScheme::overloadedSlave = NULL;
StripeList<SlaveSocket> *BasicRemappingScheme::stripeList = NULL;
MasterRemapMsgHandler *BasicRemappingScheme::remapMsgHandler = NULL;

Latency BasicRemappingScheme::increment ( 0, 100 );
LOCK_T BasicRemappingScheme::lock = PTHREAD_MUTEX_INITIALIZER;
uint32_t BasicRemappingScheme::remapped = 0;
uint32_t BasicRemappingScheme::lockonly= 0;

void BasicRemappingScheme::getRemapTarget( uint32_t originalListId, uint32_t originalChunkId, uint32_t &remappedListId, uint32_t &remappedChunkId, uint32_t dataCount, uint32_t parityCount, SlaveSocket **data, SlaveSocket **parity ) {
	int index = -1, leastOverloadedId;
	struct sockaddr_in slaveAddr;
	Latency *targetLatency, *nodeLatency, *overloadLatency;

	// baseline: no remapping
	remappedChunkId = originalChunkId;
	remappedListId = originalListId;
	leastOverloadedId = originalChunkId;

	if ( slaveLoading == NULL || overloadedSlave == NULL || stripeList == NULL || remapMsgHandler == NULL ) {
		fprintf( stderr, "The scheme is not yet initialized!! Abort remapping!!\n" );
		return;
	}

	slaveAddr = data[ originalChunkId ]->getAddr();

	// check if remapping is allowed
	if ( ! remapMsgHandler->allowRemapping( slaveAddr ) ) {
		lockonly++;
		return;
	}

	// skip remap if not overloaded
	if ( overloadedSlave->slaveSet.count( slaveAddr ) < 1 )
		return;

	// TODO avoid locking?
	LOCK( &slaveLoading->lock );
	LOCK( &overloadedSlave->lock );

	targetLatency = slaveLoading->cumulativeMirror.set.get( slaveAddr, &index );
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
			nodeLatency = slaveLoading->cumulativeMirror.set.get( slaveAddr , &index );

			// TODO if this node had not been accessed, should we take the risk and try?
			if ( index == -1 )
				continue;

			if ( overloadedSlave->slaveSet.count( slaveAddr ) > 0 ) {
				if ( *nodeLatency < *overloadLatency ) {
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
		*targetLatency = *targetLatency + increment;

	} else {
		// search the least-loaded node with the stripe list
		// TODO  move to more efficient and scalable data structure e.g. min-heap
		for ( uint32_t chunkIndex = 0; chunkIndex < dataCount; chunkIndex++ ) {
			// skip the original node
			if ( chunkIndex == originalChunkId )
				continue;
			slaveAddr = data[ chunkIndex ]->getAddr();
			nodeLatency = slaveLoading->cumulativeMirror.set.get( slaveAddr, &index );

			// TODO if this node had not been accessed, should we take the risk and try?
			if ( index == -1 )
				continue;
			if ( overloadedSlave->slaveSet.count( slaveAddr ) > 0 ) {
				// scan for the least overloaded node, in case all nodes are overloaded
				if ( *nodeLatency < *overloadLatency ) {
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
		*targetLatency = *targetLatency + increment;
	}


exit:
	UNLOCK( &overloadedSlave->lock );
	UNLOCK( &slaveLoading->lock );

	if ( ! ( remappedChunkId == originalChunkId && remappedListId == originalListId ) ) {
		LOCK( &BasicRemappingScheme::lock );
		remapped++;
		UNLOCK( &BasicRemappingScheme::lock );
	}
}

// Enable degraded operation only if remapping is enabled
void BasicRemappingScheme::getDegradedOpTarget( uint32_t listId, uint32_t originalChunkId, uint32_t &newChunkId, uint32_t dataCount, uint32_t parityCount, SlaveSocket **data, SlaveSocket **parity ) {
	int index = -1, leastOverloadedId;
	struct sockaddr_in slaveAddr;
	Latency *targetLatency, *nodeLatency, *overloadLatency;

	// Baseline
	newChunkId = originalChunkId;
	leastOverloadedId = originalChunkId;

	if ( slaveLoading == NULL || overloadedSlave == NULL || stripeList == NULL || remapMsgHandler == NULL ) {
		fprintf( stderr, "The scheme is not yet initialized!! Abort degraded operation!\n" );
		return;
	}

	slaveAddr = data[ originalChunkId ]->getAddr();

	// check if remapping is allowed
	if ( ! remapMsgHandler->allowRemapping( slaveAddr ) || parityCount == 0 )
		return;

	// skip remap if not overloaded
	if ( overloadedSlave->slaveSet.count( slaveAddr ) < 1 )
		return;

	// TODO avoid locking?
	LOCK( &slaveLoading->lock );
	LOCK( &overloadedSlave->lock );

	targetLatency = slaveLoading->cumulativeMirror.set.get( slaveAddr, &index );
	overloadLatency = targetLatency;
	// cannot determine whether remap is necessary??
	if ( index == -1 )
		goto exit;


	// search the least-loaded node with the stripe list
	// TODO  move to more efficient and scalable data structure e.g. min-heap
	for ( uint32_t chunkIndex = 0; chunkIndex < dataCount + parityCount; chunkIndex++ ) {
		// skip the original node
		if ( chunkIndex == originalChunkId )
			continue;
		if ( chunkIndex < dataCount )
			slaveAddr = data[ chunkIndex ]->getAddr();
		else
			slaveAddr = parity[ chunkIndex - dataCount ]->getAddr();
		nodeLatency = slaveLoading->cumulativeMirror.set.get( slaveAddr, &index );

		// TODO if this node had not been accessed, should we take the risk and try?
		if ( index == -1 )
			continue;
		if ( overloadedSlave->slaveSet.count( slaveAddr ) > 0 ) {
			// scan for the least overloaded node, in case all nodes are overloaded
			if ( *nodeLatency < *overloadLatency ) {
				overloadLatency = nodeLatency;
				leastOverloadedId = chunkIndex;
			}
		} else if ( *nodeLatency < *targetLatency ) {
			targetLatency = nodeLatency;
			newChunkId = chunkIndex;
		}
	}
	if ( newChunkId == originalChunkId )
		newChunkId = leastOverloadedId;
	*targetLatency = *targetLatency + increment;

exit:
	UNLOCK( &overloadedSlave->lock );
	UNLOCK( &slaveLoading->lock );
}

bool BasicRemappingScheme::isOverloaded( SlaveSocket *socket ) {
	struct sockaddr_in slaveAddr;

	if ( slaveLoading == NULL || overloadedSlave == NULL || stripeList == NULL || remapMsgHandler == NULL ) {
		fprintf( stderr, "The scheme is not yet initialized!! Abort degraded operation!\n" );
		return false;
	}

	slaveAddr = socket->getAddr();

	// check if remapping is allowed
	if ( ! remapMsgHandler->allowRemapping( slaveAddr ) )
		return false;

	return ( overloadedSlave->slaveSet.count( slaveAddr ) >= 1 );
}
