#include <unordered_set>
#include "basic_remap_scheme.hh"
#include "../../common/ds/sockaddr_in.hh"
#include "../../common/util/debug.hh"

SlaveLoading *BasicRemappingScheme::slaveLoading = NULL;
OverloadedSlave *BasicRemappingScheme::overloadedSlave = NULL;
StripeList<SlaveSocket> *BasicRemappingScheme::stripeList = NULL;
MasterRemapMsgHandler *BasicRemappingScheme::remapMsgHandler = NULL;

Latency BasicRemappingScheme::increment ( 0, 100 );
pthread_mutex_t BasicRemappingScheme::lock = PTHREAD_MUTEX_INITIALIZER;
uint32_t BasicRemappingScheme::remapped = 0;
uint32_t BasicRemappingScheme::lockonly= 0;

void BasicRemappingScheme::getRemapTarget( uint32_t *original, uint32_t *remapped, uint32_t &remappedCount, uint32_t dataChunkCount, uint32_t parityChunkCount, SlaveSocket **dataSlaveSockets, SlaveSocket **paritySlaveSockets ) {
	struct sockaddr_in slaveAddr;
	std::unordered_set<struct sockaddr_in> selectedSlaves;

	if ( slaveLoading == NULL || overloadedSlave == NULL || stripeList == NULL || remapMsgHandler == NULL ) {
		fprintf( stderr, "The scheme is not yet initialized!! Abort remapping!!\n" );
		return;
	}

	LOCK( &slaveLoading->lock );
	LOCK( &overloadedSlave->lock );

	remappedCount = 0;
	for ( uint32_t i = 0; i < 1 + parityChunkCount; i++ ) {
		if ( i == 0 )
			slaveAddr = dataSlaveSockets[ original[ 1 ] ]->getAddr();
		else
			slaveAddr = paritySlaveSockets[ original[ i * 2 + 1 ] - dataChunkCount ]->getAddr();

		selectedSlaves.insert( slaveAddr ); // All original slaves should not be selected as remapped slaves

		// Check if remapping is allowed && isOverloaded
		if ( remapMsgHandler->allowRemapping( slaveAddr ) && overloadedSlave->slaveSet.count( slaveAddr ) ) {
			remapped[ remappedCount * 2     ] = original[ i * 2     ];
			remapped[ remappedCount * 2 + 1 ] = original[ i * 2 + 1 ];
			remappedCount++;
		}
	}

	if ( ! remappedCount ) {
		pthread_mutex_lock( &BasicRemappingScheme::lock );
		BasicRemappingScheme::lockonly++;
		pthread_mutex_unlock( &BasicRemappingScheme::lock );

		UNLOCK( &slaveLoading->lock );
		UNLOCK( &overloadedSlave->lock );
		return;
	} else {
		pthread_mutex_lock( &BasicRemappingScheme::lock );
		BasicRemappingScheme::remapped++;
		pthread_mutex_unlock( &BasicRemappingScheme::lock );
	}

	for ( uint32_t i = 0; i < remappedCount; i++ ) {
		original[ i * 2     ] = remapped[ i * 2     ];
		original[ i * 2 + 1 ] = remapped[ i * 2 + 1 ];
	}

	int index = -1;
	Latency *targetLatency, *nodeLatency;
	for ( uint32_t i = 0; i < remappedCount; i++ ) {
		slaveAddr = BasicRemappingScheme::stripeList->get( original[ i * 2 ], original[ i * 2 + 1 ] )->getAddr();
		targetLatency = 0; // slaveLoading->cumulativeMirror.set.get( slaveAddr, &index );

		// Baseline
		remapped[ i * 2     ] = original[ i * 2     ];
		remapped[ i * 2 + 1 ] = original[ i * 2 + 1 ];

		for ( uint32_t j = 0; j < dataChunkCount + parityChunkCount; j++ ) {
			if ( j < dataChunkCount )
				slaveAddr = dataSlaveSockets[ j ]->getAddr();
			else
				slaveAddr = paritySlaveSockets[ j - dataChunkCount ]->getAddr();

			if ( selectedSlaves.count( slaveAddr ) ){
				// skip selected and original slaves
				// printf( "--- (%u, %u) is selected ---\n", original[ i * 2 ], j );
				continue;
			} else if ( overloadedSlave->slaveSet.count( slaveAddr ) > 0 ) {
				// Skip overloaded slave
				// printf( "--- (%u, %u) is overloaded ---\n", original[ i * 2 ], j );
				continue;
			}

			nodeLatency = slaveLoading->cumulativeMirror.set.get( slaveAddr, &index );
			if ( ( remapped[ i * 2     ] == original[ i * 2     ] ) && ( remapped[ i * 2 + 1 ] == original[ i * 2 + 1 ] ) ) {
				// Always remap to another slave first
				targetLatency = nodeLatency;
				remapped[ i * 2     ] = original[ i * 2     ]; // List ID
				remapped[ i * 2 + 1 ] = j;                     // Chunk ID
			} else if ( targetLatency && nodeLatency && *nodeLatency < *targetLatency ) {
				// Search the least-loaded node with the stripe list
				targetLatency = nodeLatency;
				remapped[ i * 2     ] = original[ i * 2     ]; // List ID
				remapped[ i * 2 + 1 ] = j;                     // Chunk ID
			}
		}

		if ( remapped[ i * 2     ] == original[ i * 2     ] &&
		     remapped[ i * 2 + 1 ] == original[ i * 2 + 1 ] ) {
			__ERROR__( "BasicRemappingScheme", "getRemapTarget", "Cannot get remapping target for (%u, %u); i = %u / %u.", original[ i * 2 ], original[ i * 2 + 1 ], i, remappedCount );
		} else {
			slaveAddr = BasicRemappingScheme::stripeList->get( remapped[ i * 2 ], remapped[ i * 2 + 1 ] )->getAddr();
			nodeLatency = slaveLoading->cumulativeMirror.set.get( slaveAddr, &index );
			*nodeLatency = *nodeLatency + increment;
			selectedSlaves.insert( slaveAddr );
		}
	}

	UNLOCK( &slaveLoading->lock );
	UNLOCK( &overloadedSlave->lock );
}

/*
void BasicRemappingScheme::getRemapTarget( uint32_t originalListId, uint32_t originalChunkId, uint32_t &remappedListId, std::vector<uint32_t> &remappedChunkId, uint32_t dataCount, uint32_t parityCount, SlaveSocket **data, SlaveSocket **parity ) {
	int index = -1, leastOverloadedId;
	struct sockaddr_in slaveAddr;
	Latency *targetLatency, *nodeLatency, *overloadLatency;

	// baseline: no remapping
	remappedChunkId.clear();
	remappedChunkId.push_back( originalChunkId );
	for ( uint32_t i = 0; i < parityCount; i++ )
		remappedChunkId.push_back( dataCount + i );
	remappedListId = originalListId;
	leastOverloadedId = originalChunkId;

	if ( slaveLoading == NULL || overloadedSlave == NULL || stripeList == NULL || remapMsgHandler == NULL ) {
		fprintf( stderr, "The scheme is not yet initialized!! Abort remapping!!\n" );
		return;
	}

	slaveAddr = data[ originalChunkId ]->getAddr();

	// check if remapping is allowed, and mark the slaves to be remapped
	// allow remapping && is overload
	bool allowRemapping = false;
	std::vector<uint32_t> remapIndex; // 0: data, idx > 0: idx-1 th parity
	for ( uint32_t i = 0; i < 1 + parityCount; i++ ) {
		if ( i == 0 ) {
			allowRemapping = remapMsgHandler->allowRemapping( slaveAddr );
			allowRemapping &= overloadedSlave->slaveSet.count( slaveAddr );
		} else {
			allowRemapping = remapMsgHandler->allowRemapping( parity[ i - 1 ]->getAddr() );
			allowRemapping &= overloadedSlave->slaveSet.count( parity[ i - 1 ]->getAddr() );
		}
		if ( allowRemapping ) remapIndex.push_back( i );
	}
	if ( remapIndex.empty() ) {
		lockonly++;
		return;
	}

	LOCK( &slaveLoading->lock );
	LOCK( &overloadedSlave->lock );

	std::unordered_set<uint32_t> selectedSlaves;
	bool hasRemapped = false;
	for ( uint32_t i = 0; i < remapIndex.size(); i++ ) {
		if ( remapIndex[ i ] != 0 )
			slaveAddr = parity[ remapIndex[ i ] - 1 ]->getAddr();
		if ( remapIndex[ i ] == 0 ) {
			leastOverloadedId = originalChunkId;
		} else {
			leastOverloadedId = remapIndex[ i ] - 1 + dataCount;
		}
		targetLatency = slaveLoading->cumulativeMirror.set.get( slaveAddr, &index );
		overloadLatency = targetLatency;
		// cannot determine whether remap is necessary??
		if ( index == -1 ) {
			selectedSlaves.insert( remappedChunkId[ remapIndex [ i ] ] );
			continue;
		}

		if ( dataCount < 2 && remapIndex[ i ] == 0 ) {
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

			hasRemapped |= ( remappedListId != originalListId );

		} else if ( dataCount < 2 ) {
			// TODO handle parity remap?
		} else {
			// search the least-loaded node with the stripe list
			// TODO  move to more efficient and scalable data structure e.g. min-heap
			for ( uint32_t chunkIndex = 0; chunkIndex < dataCount; chunkIndex++ ) {
				// skip the original node
				if ( chunkIndex == originalChunkId && remapIndex[ i ] == 0 )
					continue;
				nodeLatency = slaveLoading->cumulativeMirror.set.get( slaveAddr, &index );

				// TODO if this node had not been accessed, should we take the risk and try?
				if ( index == -1 )
					continue;
				if ( overloadedSlave->slaveSet.count( slaveAddr ) > 0 ) {
					// scan for the least overloaded node, in case all nodes are overloaded
					if ( *nodeLatency < *overloadLatency && selectedSlaves.count( chunkIndex ) == 0 ) {
						overloadLatency = nodeLatency;
						leastOverloadedId = chunkIndex;
					}
				} else if ( *nodeLatency < *targetLatency && selectedSlaves.count( chunkIndex ) == 0 ) {
					targetLatency = nodeLatency;
					remappedChunkId[ remapIndex[ i ] ] = chunkIndex;
				}
			}
			if ( ( remapIndex[ i ] == 0 && remappedChunkId[ remapIndex[ i ] ] == originalChunkId ) ||
				( remapIndex[ i ] > 0 && remappedChunkId[ remapIndex[ i ] ] == remapIndex[ i ] - 1 + dataCount ) )
			{
				remappedChunkId[ remapIndex[ i ] ] = leastOverloadedId;
				*targetLatency = *targetLatency + increment;
			}
			selectedSlaves.insert( remappedChunkId[ remapIndex[ i ] ] );
			// just for debug ( count and printing )
			if ( remapIndex[ i ] == 0 )
				hasRemapped |= ( remappedChunkId[ remapIndex[ i ] ] != originalChunkId );
			else
				hasRemapped |= ( remappedChunkId[ remapIndex[ i ] ] != dataCount + remapIndex[ i ] - 1 );
		}
	}

	UNLOCK( &overloadedSlave->lock );
	UNLOCK( &slaveLoading->lock );

	if ( hasRemapped ) {
		//for ( uint32_t i = 0; i < remappedChunkId.size(); i++ )
		//	printf( "remap[%u] %u to %u\n", i, (i == 0)? originalChunkId:i-1+dataCount, remappedChunkId[ i ] );
		pthread_mutex_lock( &BasicRemappingScheme::lock );
		remapped++;
		pthread_mutex_unlock( &BasicRemappingScheme::lock );
	}
}
*/

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

	slaveAddr = originalChunkId < dataCount ? data[ originalChunkId ]->getAddr() : parity[ originalChunkId - dataCount ]->getAddr();

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
