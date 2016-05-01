#include <unordered_set>
#include "basic_remap_scheme.hh"
#include "../../common/ds/sockaddr_in.hh"
#include "../../common/util/debug.hh"

ServerLoading *BasicRemappingScheme::serverLoading = NULL;
OverloadedServer *BasicRemappingScheme::overloadedServer = NULL;
StripeList<ServerSocket> *BasicRemappingScheme::stripeList = NULL;
ClientStateTransitHandler *BasicRemappingScheme::stateTransitHandler = NULL;

Latency BasicRemappingScheme::increment ( 0, 100 );

void BasicRemappingScheme::redirect(
	uint32_t *original, uint32_t *remapped, uint32_t numEntries, uint32_t &remappedCount,
	uint32_t dataChunkCount, uint32_t parityChunkCount,
	ServerSocket **dataServerSockets, ServerSocket **parityServerSockets,
	bool isGet
) {
	struct sockaddr_in serverAddr;
	std::unordered_set<struct sockaddr_in> selectedServers, redirectedServers;

	if ( serverLoading == NULL || overloadedServer == NULL || stripeList == NULL || stateTransitHandler == NULL ) {
		fprintf( stderr, "The scheme is not yet initialized!! Abort remapping!!\n" );
		return;
	}

	LOCK( &serverLoading->lock );
	LOCK( &overloadedServer->lock );

	remappedCount = 0;
	for ( uint32_t i = 0; i < numEntries; i++ ) {
		uint32_t chunkId = original[ i * 2 + 1 ];
		if ( chunkId < dataChunkCount )
			serverAddr = dataServerSockets[ chunkId ]->getAddr();
		else
			serverAddr = parityServerSockets[ chunkId - dataChunkCount ]->getAddr();

		// Both original and failed servers should not be selected as remapped servers
		bool allowRemapping = stateTransitHandler->allowRemapping( serverAddr );

		selectedServers.insert( serverAddr );

		// Check if remapping is allowed
		if ( allowRemapping ) {
			if ( ! isGet || chunkId < dataChunkCount ) {
				remapped[ remappedCount * 2     ] = original[ i * 2     ];
				remapped[ remappedCount * 2 + 1 ] = original[ i * 2 + 1 ];
				remappedCount++;
			}
		}
	}

	if ( ! remappedCount ) {
		UNLOCK( &serverLoading->lock );
		UNLOCK( &overloadedServer->lock );
		return;
	}

	for ( uint32_t i = 0; i < remappedCount; i++ ) {
		original[ i * 2     ] = remapped[ i * 2     ];
		original[ i * 2 + 1 ] = remapped[ i * 2 + 1 ];
	}

	int index = -1, selected;
	Latency *targetLatency, *nodeLatency;
	for ( uint32_t i = 0; i < remappedCount; i++ ) {
		serverAddr = BasicRemappingScheme::stripeList->get( original[ i * 2 ], original[ i * 2 + 1 ] )->getAddr();
		targetLatency = 0; // serverLoading->cumulativeMirror.set.get( serverAddr, &index );

		// Baseline
		remapped[ i * 2     ] = original[ i * 2     ];
		remapped[ i * 2 + 1 ] = original[ i * 2 + 1 ];
		selected = false;

		for ( uint32_t j = 0; j < dataChunkCount + parityChunkCount; j++ ) {
			if ( j < dataChunkCount )
				serverAddr = dataServerSockets[ j ]->getAddr();
			else
				serverAddr = parityServerSockets[ j - dataChunkCount ]->getAddr();

			if ( selectedServers.count( serverAddr ) ) {
				// Skip original servers
				continue;
			} else if ( redirectedServers.count( serverAddr ) ) {
				// Skip selected servers
				continue;
			} else if ( stateTransitHandler->useCoordinatedFlow( serverAddr, true, true ) ) {
				// Skip overloaded server
				continue;
			}

			nodeLatency = serverLoading->cumulativeMirror.set.get( serverAddr, &index );
			if ( ( remapped[ i * 2     ] == original[ i * 2     ] ) && ( remapped[ i * 2 + 1 ] == original[ i * 2 + 1 ] ) ) {
				// Always remap to another server first
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
			serverAddr = BasicRemappingScheme::stripeList->get( remapped[ i * 2 ], remapped[ i * 2 + 1 ] )->getAddr();
			nodeLatency = serverLoading->cumulativeMirror.set.get( serverAddr, &index );
			if ( nodeLatency )
				*nodeLatency = *nodeLatency + increment;

			redirectedServers.insert( serverAddr );
		}
	}

	UNLOCK( &serverLoading->lock );
	UNLOCK( &overloadedServer->lock );
}

bool BasicRemappingScheme::isOverloaded( ServerSocket *socket ) {
	struct sockaddr_in serverAddr;

	if ( serverLoading == NULL || overloadedServer == NULL || stripeList == NULL || stateTransitHandler == NULL ) {
		fprintf( stderr, "The scheme is not yet initialized!! Abort degraded operation!\n" );
		return false;
	}

	serverAddr = socket->getAddr();

	// check if remapping is allowed
	if ( ! stateTransitHandler->allowRemapping( serverAddr ) )
		return false;

	return ( overloadedServer->serverSet.count( serverAddr ) >= 1 );
}
