#include <unordered_set>
#include "basic_remap_scheme.hh"
#include "../../common/ds/sockaddr_in.hh"
#include "../../common/util/debug.hh"

SlaveLoading *BasicRemappingScheme::slaveLoading = NULL;
OverloadedSlave *BasicRemappingScheme::overloadedSlave = NULL;
StripeList<SlaveSocket> *BasicRemappingScheme::stripeList = NULL;
MasterRemapMsgHandler *BasicRemappingScheme::remapMsgHandler = NULL;

Latency BasicRemappingScheme::increment ( 0, 100 );

void BasicRemappingScheme::redirect(
	uint32_t *original, uint32_t *remapped, uint32_t numEntries, uint32_t &remappedCount,
	uint32_t dataChunkCount, uint32_t parityChunkCount,
	SlaveSocket **dataSlaveSockets, SlaveSocket **paritySlaveSockets,
	bool isGet
) {
	struct sockaddr_in slaveAddr;
	std::unordered_set<struct sockaddr_in> selectedSlaves;

	if ( slaveLoading == NULL || overloadedSlave == NULL || stripeList == NULL || remapMsgHandler == NULL ) {
		fprintf( stderr, "The scheme is not yet initialized!! Abort remapping!!\n" );
		return;
	}

	LOCK( &slaveLoading->lock );
	LOCK( &overloadedSlave->lock );

	remappedCount = 0;
	for ( uint32_t i = 0; i < numEntries; i++ ) {
		uint32_t chunkId = original[ i * 2 + 1 ];
		if ( chunkId < dataChunkCount )
			slaveAddr = dataSlaveSockets[ chunkId ]->getAddr();
		else
			slaveAddr = paritySlaveSockets[ chunkId - dataChunkCount ]->getAddr();

		selectedSlaves.insert( slaveAddr ); // All original or failed slaves should not be selected as remapped slaves

		// Check if remapping is allowed
		if ( remapMsgHandler->allowRemapping( slaveAddr ) ) {
			if ( ! isGet || chunkId < dataChunkCount ) {
				remapped[ remappedCount * 2     ] = original[ i * 2     ];
				remapped[ remappedCount * 2 + 1 ] = original[ i * 2 + 1 ];
				remappedCount++;
			}
		}
	}

	if ( ! remappedCount ) {
		UNLOCK( &slaveLoading->lock );
		UNLOCK( &overloadedSlave->lock );
		return;
	}

	for ( uint32_t i = 0; i < remappedCount; i++ ) {
		original[ i * 2     ] = remapped[ i * 2     ];
		original[ i * 2 + 1 ] = remapped[ i * 2 + 1 ];
	}

	int index = -1, selected;
	Latency *targetLatency, *nodeLatency;
	for ( uint32_t i = 0; i < remappedCount; i++ ) {
		slaveAddr = BasicRemappingScheme::stripeList->get( original[ i * 2 ], original[ i * 2 + 1 ] )->getAddr();
		targetLatency = 0; // slaveLoading->cumulativeMirror.set.get( slaveAddr, &index );

		// Baseline
		remapped[ i * 2     ] = original[ i * 2     ];
		remapped[ i * 2 + 1 ] = original[ i * 2 + 1 ];
		selected = false;

		for ( uint32_t j = 0; j < dataChunkCount + parityChunkCount; j++ ) {
			if ( j < dataChunkCount )
				slaveAddr = dataSlaveSockets[ j ]->getAddr();
			else
				slaveAddr = paritySlaveSockets[ j - dataChunkCount ]->getAddr();

			if ( selectedSlaves.count( slaveAddr ) ){
				// skip selected and original slaves
				// printf( "--- (%u, %u) is selected ---\n", original[ i * 2 ], j );
				continue;
			} else if ( remapMsgHandler->useCoordinatedFlow( slaveAddr ) ) {
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
				selected = true;
			} else if ( targetLatency && nodeLatency && *nodeLatency < *targetLatency ) {
				// Search the least-loaded node with the stripe list
				targetLatency = nodeLatency;
				remapped[ i * 2     ] = original[ i * 2     ]; // List ID
				remapped[ i * 2 + 1 ] = j;                     // Chunk ID
				selected = true;
			}

			// FOR DEBUG ONLY
			// if ( original[ i * 2 + 1 ] >= dataChunkCount )
			// 	remapped[ i * 2 + 1 ] = 0;
		}

		if ( remapped[ i * 2     ] == original[ i * 2     ] &&
		     remapped[ i * 2 + 1 ] == original[ i * 2 + 1 ] ) {
			__ERROR__( "BasicRemappingScheme", "redirect", "Cannot get remapping target for (%u, %u); i = %u / %u; selected: %s.", original[ i * 2 ], original[ i * 2 + 1 ], i, remappedCount, selected ? "true" : "false" );
		} else {
			slaveAddr = BasicRemappingScheme::stripeList->get( remapped[ i * 2 ], remapped[ i * 2 + 1 ] )->getAddr();
			nodeLatency = slaveLoading->cumulativeMirror.set.get( slaveAddr, &index );
			if ( nodeLatency )
				*nodeLatency = *nodeLatency + increment;
			selectedSlaves.insert( slaveAddr );
		}
	}

	UNLOCK( &slaveLoading->lock );
	UNLOCK( &overloadedSlave->lock );
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
