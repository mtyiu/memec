#include "worker.hh"
#include "../main/server.hh"

int ServerWorker::findInRedirectedList( uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount, uint32_t ongoingAtChunk, bool &reconstructParity, bool &reconstructData, uint32_t dataChunkId, bool isSealed ) {
	int ret = -1;
	bool self = false;
	uint32_t listId;

	std::vector<StripeListIndex> &lists = Server::getInstance()->stripeListIndex;
	int listIndex = -1;

	reconstructParity = false;
	reconstructData = false;

	if ( reconstructedCount ) {
		listId = reconstructed[ 0 ];
		for ( uint32_t i = 0, size = lists.size(); i < size; i++ ) {
			if ( listId == lists[ i ].listId ) {
				listIndex = ( int ) i;
				break;
			}
		}

		if ( listIndex == -1 ) {
			__ERROR__( "ServerWorker", "findInRedirectedList", "This server is not in the reconstructed list." );
			return -1;
		}
	}

	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		if ( original[ i * 2 + 1 ] >= ServerWorker::dataChunkCount )
			reconstructParity = true;
		else if ( dataChunkId == original[ i * 2 + 1 ] )
			reconstructData = true;

		if ( dataChunkId == ongoingAtChunk ) {
			// Need to reconstruct
			if ( original[ i * 2 + 1 ] < ServerWorker::dataChunkCount )
				reconstructData = true;
		}

		if ( ret == -1 ) {
			if ( ! self && lists[ listIndex ].chunkId == ongoingAtChunk )
				self = true;
			if ( reconstructed[ i * 2 + 1 ] == lists[ listIndex ].chunkId ) {
				ret = ( int ) i;
				break;
			}
		}
	}

	reconstructParity = reconstructParity && ( self || ! isSealed );
	if ( reconstructData )
		reconstructParity = false;

	return ret;
}

bool ServerWorker::handleReleaseDegradedLockRequest( CoordinatorEvent event, char *buf, size_t size ) {
	struct ChunkHeader header;
	uint32_t count = 0, requestId;
	uint16_t instanceId = Server::instanceId;

	Metadata metadata;
	ChunkRequest chunkRequest;
	std::vector<Metadata> chunks;
	ServerPeerEvent serverPeerEvent;
	ServerPeerSocket *socket = NULL;
	Chunk *chunk;

	while( size ) {
		if ( ! this->protocol.parseDegradedReleaseReqHeader( header, buf, size ) ) {
			__ERROR__( "ServerWorker", "handleReleaseDegradedLockRequest", "Invalid DEGRADED_RELEASE request." );
			return false;
		}
		//__INFO__(
		//	BLUE, "ServerWorker", "handleReleaseDegradedLockRequest",
		//	"[DEGRADED_RELEASE] (%u, %u, %u) (count = %u).",
		//	header.listId, header.stripeId, header.chunkId,
		//	count
		//);
		buf += PROTO_DEGRADED_RELEASE_REQ_SIZE;
		size -= PROTO_DEGRADED_RELEASE_REQ_SIZE;

		metadata.set( header.listId, header.stripeId, header.chunkId );
		chunks.push_back( metadata );

		count++;
	}

	ServerWorker::pending->insertReleaseDegradedLock( event.instanceId, event.requestId, event.socket, count );

	for ( size_t i = 0, len = chunks.size(); i < len; i++ ) {
		// Determine the src
		// TODO Helen: comment (i==0) for temp workaround (?)
		//if ( i == 0 ) {
			// The target is the same for all chunks in this request
			this->getServers( chunks[ i ].listId );
			socket =   chunks[ i ].chunkId < ServerWorker::dataChunkCount
			         ? this->dataServerSockets[ chunks[ i ].chunkId ]
			         : this->parityServerSockets[ chunks[ i ].chunkId - ServerWorker::dataChunkCount ];
		//}

		requestId = ServerWorker::idGenerator->nextVal( this->workerId );
		chunk = ServerWorker::degradedChunkBuffer->map.deleteChunk(
			chunks[ i ].listId, chunks[ i ].stripeId, chunks[ i ].chunkId,
			&metadata
		);

		chunkRequest.set(
			chunks[ i ].listId, chunks[ i ].stripeId, chunks[ i ].chunkId,
			socket, chunk, true /* isDegraded */
		);
		if ( ! ServerWorker::pending->insertChunkRequest( PT_SERVER_PEER_SET_CHUNK, instanceId, event.instanceId, requestId, event.requestId, socket, chunkRequest ) ) {
			__ERROR__( "ServerWorker", "handleReleaseDegradedLockRequest", "Cannot insert into server CHUNK_REQUEST pending map." );
		}

		// If chunk is NULL, then the unsealed version of SET_CHUNK will be used
		serverPeerEvent.reqSetChunk( socket, instanceId, requestId, metadata, chunk, true );
		ServerWorker::eventQueue->insert( serverPeerEvent );
	}

	return true;
}

bool ServerWorker::handleDegradedGetRequest( ClientEvent event, char *buf, size_t size ) {
	struct DegradedReqHeader header;
	if ( ! this->protocol.parseDegradedReqHeader( header, PROTO_OPCODE_DEGRADED_GET, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleDegradedGetRequest", "Invalid degraded GET request." );
		return false;
	}
	__DEBUG__(
		BLUE, "ServerWorker", "handleDegradedGetRequest",
		"[GET] (%u, %u) Key: %.*s (key size = %u) at stripe: %u; is sealed? %s; is large? %s.",
		event.instanceId, event.requestId,
		( int ) header.data.key.keySize,
		header.data.key.key,
		header.data.key.keySize,
		header.stripeId,
		header.isSealed ? "true" : "false",
		header.isLarge ? "true" : "false"
	);

	int index = -1;
	bool reconstructParity, reconstructData;
	uint32_t listId, stripeId, chunkId;
	stripeId = header.stripeId;

	if ( header.reconstructedCount ) {
		this->getServers(
			header.data.key.key,
			header.data.key.keySize,
			listId,
			chunkId
		);

		if ( header.isLarge ) {
			uint32_t splitOffset = LargeObjectUtil::readSplitOffset( header.data.key.key + header.data.key.keySize );
			uint32_t splitIndex = LargeObjectUtil::getSplitIndex( header.data.key.keySize, 0, splitOffset, header.isLarge );
			chunkId = ( chunkId + splitIndex ) % ( ServerWorker::dataChunkCount );
		}

		index = this->findInRedirectedList(
			header.original, header.reconstructed, header.reconstructedCount,
			header.ongoingAtChunk, reconstructParity, reconstructData,
			chunkId, header.isSealed
		);

		if ( ( index == -1 ) ||
		     ( header.original[ index * 2 + 1 ] >= ServerWorker::dataChunkCount ) ) {
			// No need to perform degraded read if only the parity servers are redirected
			return this->handleGetRequest( event, header.data.key, true );
		}
	} else {
		// Use normal flow
		return this->handleGetRequest( event, header.data.key, true );
	}

	////////// Degraded read //////////
	Key key;
	KeyValue keyValue;
	KeyMetadata keyMetadata;
	bool ret = true, checked = false;
	DegradedMap *dmap = &ServerWorker::degradedChunkBuffer->map;

degraded_get_check:
	 // Check if the chunk is already fetched //
	Chunk *chunk = dmap->findChunkById(
		listId, stripeId, chunkId
	);
	// Check if the key exists or is in a unsealed chunk //
	bool isSealed;
	bool isKeyValueFound = dmap->findValueByKey(
		header.data.key.key,
		header.data.key.keySize,
		header.isLarge,
		isSealed,
		&keyValue, &key, &keyMetadata
	);

	if ( isKeyValueFound ) {
		// Send the key-value pair to the client
		event.resGet(
			event.socket, event.instanceId, event.requestId,
			keyValue, true // isDegraded
		);
		this->dispatch( event );
	} else if ( chunk ) {
		// Key not found
		event.resGet(
			event.socket, event.instanceId, event.requestId,
			key, true // isDegraded
		);
		this->dispatch( event );
	} else if ( dmap->findRemovedChunk( listId, stripeId, chunkId ) ) {
		// Chunk migrated
		event.resGet(
			event.socket, event.instanceId, event.requestId,
			key, true // isDegraded
		);
		this->dispatch( event );
	} else {
		bool isReconstructed;
		key.dup( key.size, key.data, 0, header.isLarge );
		ret = this->performDegradedRead(
			PROTO_OPCODE_DEGRADED_GET,
			event.socket,
			event.instanceId, event.requestId,
			listId, stripeId, chunkId,
			&key, header.isSealed,
			header.original, header.reconstructed, header.reconstructedCount,
			header.ongoingAtChunk, header.numSurvivingChunkIds, header.survivingChunkIds,
			isReconstructed,
			0, event.timestamp
		);

		if ( ! ret ) {
			if ( checked ) {
				__ERROR__( "ServerWorker", "handleDegradedGetRequest", "Failed to perform degraded read on (%u, %u, %u; key: %.*s.%u; chunk: %p) requestId = %u from id = %u.", listId, stripeId, chunkId, key.size, key.data, key.isLarge ? LargeObjectUtil::readSplitOffset( key.data + key.size ) : 0, chunk, event.requestId, event.instanceId );
				event.resGet( event.socket, event.instanceId, event.requestId, key, true );
				this->dispatch( event );
			} else if ( isReconstructed ) {
				// Check the degraded map again
				checked = true;
				goto degraded_get_check;
			}
		}
	}

	return ret;
}

bool ServerWorker::handleDegradedUpdateRequest( ClientEvent event, char *buf, size_t size ) {
	struct DegradedReqHeader header;
	if ( ! this->protocol.parseDegradedReqHeader( header, PROTO_OPCODE_DEGRADED_UPDATE, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleDegradedUpdateRequest", "Invalid degraded UPDATE request." );
		return false;
	}
	__DEBUG__(
		BLUE, "ServerWorker", "handleDegradedUpdateRequest",
		"[UPDATE] Key: %.*s (key size = %u); Value: (update size = %u, offset = %u), reconstruction count = %u; stripe ID = %u.",
		( int ) header.data.keyValueUpdate.keySize,
		header.data.keyValueUpdate.key,
		header.data.keyValueUpdate.keySize,
		header.data.keyValueUpdate.valueUpdateSize,
		header.data.keyValueUpdate.valueUpdateOffset,
		header.reconstructedCount,
		header.stripeId
	);
	return this->handleDegradedUpdateRequest( event, header );
}

bool ServerWorker::handleDegradedUpdateRequest( ClientEvent event, struct DegradedReqHeader &header, bool jump ) {
	uint32_t listId, stripeId, chunkId;
	int index = -1;
	bool reconstructParity, reconstructData, inProgress = false;

	this->getServers(
		header.data.keyValueUpdate.key,
		header.data.keyValueUpdate.keySize,
		listId,
		chunkId
	);

	if ( header.reconstructedCount ) {
		stripeId = header.stripeId;
		index = this->findInRedirectedList(
			header.original, header.reconstructed, header.reconstructedCount,
			header.ongoingAtChunk, reconstructParity, reconstructData,
			chunkId, header.isSealed
		);

		if ( reconstructParity ) {
			inProgress = false;
			for ( uint32_t i = 0; i < header.reconstructedCount; i++ ) {
				if ( index == -1 || i != ( uint32_t ) index ) {
					bool ret;

					if ( header.isSealed ) {
						ret = ServerWorker::map->insertForwardedChunk(
							header.original[ i * 2     ],
							header.stripeId,
							header.original[ i * 2 + 1 ],
							header.reconstructed[ i * 2     ],
							header.stripeId,
							header.reconstructed[ i * 2 + 1 ]
						);
					} else {
						ret = ServerWorker::map->insertForwardedKey(
							header.data.keyValueUpdate.keySize,
							header.data.keyValueUpdate.key,
							header.reconstructed[ i * 2     ],
							header.reconstructed[ i * 2 + 1 ]
						);
					}
					if ( ! ret ) {
						inProgress = true;
						break;
					}
				}
			}

			if ( ! inProgress ) {
				__DEBUG__(
					GREEN, "ServerWorker", "handleDegradedUpdateRequest",
					"Reconstructing parity chunk - isSealed? %s; %u, %u, %u; key: %.*s; ongoing: %u",
					header.isSealed ? "yes" : "no",
					listId, header.stripeId, chunkId,
					header.data.keyValueUpdate.keySize,
					header.data.keyValueUpdate.key,
					header.ongoingAtChunk
				);
			} else {
				// UPDATE data chunk and reconstructed parity chunks
				return this->handleUpdateRequest(
					event, header.data.keyValueUpdate,
					header.original, header.reconstructed, header.reconstructedCount,
					false, // reconstructParity
					0,     // chunks
					false, // endOfDegradedOp
					true   // checkGetChunk
				);
			}
		} else if ( ! reconstructData ) {
			return this->handleUpdateRequest(
				event, header.data.keyValueUpdate,
				header.original, header.reconstructed, header.reconstructedCount,
				false, // reconstructParity
				0,     // chunks
				false, // endOfDegradedOp
				true   // checkGetChunk
			);
		}
	} else {
		// Use normal flow
		return this->handleUpdateRequest( event, header.data.keyValueUpdate );
	}

	////////// Degraded read //////////
	Key key;
	KeyValue keyValue;
	KeyValueUpdate keyValueUpdate;
	KeyMetadata keyMetadata;
	Metadata metadata;
	Chunk *chunk;
	bool ret = true;
	DegradedMap *dmap = &ServerWorker::degradedChunkBuffer->map;
	bool isSealed, isKeyValueFound;

	if ( ( keyValue.data = map->findObject(
		header.data.keyValueUpdate.key,
		header.data.keyValueUpdate.keySize
	) ) ) {
		uint32_t offset;
		chunk = ServerWorker::chunkPool->getChunk( keyValue.data, offset );
		metadata = ChunkUtil::getMetadata( chunk );

		MixedChunkBuffer *chunkBuffer = ServerWorker::chunkBuffer->at( metadata.listId );
		int chunkBufferIndex = chunkBuffer->lockChunk( chunk, true );
		// Check whether the key is sealed
		if ( chunkBufferIndex != -1 ) {
			// Not sealed
			chunkBuffer->unlock( chunkBufferIndex );
			this->handleUpdateRequest(
				event, header.data.keyValueUpdate,
				header.original, header.reconstructed, header.reconstructedCount,
				false, // reconstructParity
				0,     // chunks
				false, // endOfDegradedOp
				true   // checkGetChunk
			);

			bool isReconstructed;
			return this->performDegradedRead(
				0, // No associated operation
				event.socket,
				event.instanceId, event.requestId,
				listId, stripeId, chunkId,
				0, true,
				header.original, header.reconstructed, header.reconstructedCount,
				header.ongoingAtChunk, header.numSurvivingChunkIds, header.survivingChunkIds,
				isReconstructed,
				0, event.timestamp
			);
		} else {
			// Sealed - back up the chunk
			ServerWorker::getChunkBuffer->insert( metadata, chunk );
			chunkBuffer->unlock( chunkBufferIndex );
		}
	}

	keyMetadata.offset = 0;

	if ( index == -1 ) {
		// Data chunk is NOT reconstructed

		// Set up key
		key.set( header.data.keyValueUpdate.keySize, header.data.keyValueUpdate.key );
		// Set up KeyValueUpdate
		keyValueUpdate.set( key.size, key.data, ( void * ) event.socket );
		keyValueUpdate.offset = header.data.keyValueUpdate.valueUpdateOffset;
		keyValueUpdate.length = header.data.keyValueUpdate.valueUpdateSize;
		if ( ! jump )
			goto force_degraded_read;
	}

	// Check if the chunk is already fetched
	chunk = dmap->findChunkById( listId, stripeId, chunkId );
	// Check if the key exists or is in a unsealed chunk
	isKeyValueFound = dmap->findValueByKey(
		header.data.keyValueUpdate.key,
		header.data.keyValueUpdate.keySize,
		header.isLarge,
		isSealed,
		&keyValue, &key, &keyMetadata
	);
	// Set up KeyValueUpdate
	keyValueUpdate.set( key.size, key.data, ( void * ) event.socket );
	keyValueUpdate.offset = header.data.keyValueUpdate.valueUpdateOffset;
	keyValueUpdate.length = header.data.keyValueUpdate.valueUpdateSize;
	// Set up metadata
	metadata.set( listId, stripeId, chunkId );

	if ( isKeyValueFound ) {
		keyValueUpdate.dup( 0, 0, ( void * ) event.socket );
		keyValueUpdate.isDegraded = true;
		// Insert into client UPDATE pending set
		if ( ! ServerWorker::pending->insertKeyValueUpdate( PT_CLIENT_UPDATE, event.instanceId, event.requestId, ( void * ) event.socket, keyValueUpdate ) ) {
			__ERROR__( "ServerWorker", "handleDegradedUpdateRequest", "Cannot insert into client UPDATE pending map." );
		}

		char *valueUpdate = header.data.keyValueUpdate.valueUpdate;

		if ( chunk ) {
			// Send UPDATE_CHUNK request to the parity servers
			uint32_t chunkUpdateOffset = KeyValue::getChunkUpdateOffset(
				keyMetadata.offset, // chunkOffset
				keyValueUpdate.size, // keySize
				keyValueUpdate.offset // valueUpdateOffset
			);

			ServerWorker::degradedChunkBuffer->updateKeyValue(
				keyValueUpdate.size,
				keyValueUpdate.data,
				keyValueUpdate.length,
				keyValueUpdate.offset,
				chunkUpdateOffset,
				valueUpdate,
				chunk,
				true /* isSealed */
			);

			this->sendModifyChunkRequest(
				event.instanceId, event.requestId,
				keyValueUpdate.size,
				false,
				keyValueUpdate.data,
				metadata,
				chunkUpdateOffset,
				keyValueUpdate.length, // deltaSize
				keyValueUpdate.offset,
				valueUpdate,
				true, // isSealed
				true, // isUpdate
				event.timestamp,
				event.socket,
				header.original, header.reconstructed, header.reconstructedCount,
				false, // reconstructParity - already reconstructed
				0,     // chunk
				false, // endOfDegradedOp
				true   // checkGetChunk
			);
		} else {
			// Send UPDATE request to the parity servers
			uint32_t dataUpdateOffset = KeyValue::getChunkUpdateOffset(
				0,                            // chunkOffset
				keyValueUpdate.size,  // keySize
				keyValueUpdate.offset // valueUpdateOffset
			);

			// Compute data delta
			Coding::bitwiseXOR(
				valueUpdate,
				keyValue.data + dataUpdateOffset, // original data
				valueUpdate,                      // new data
				keyValueUpdate.length
			);
			// Perform actual data update
			Coding::bitwiseXOR(
				keyValue.data + dataUpdateOffset,
				keyValue.data + dataUpdateOffset, // original data
				valueUpdate,                      // new data
				keyValueUpdate.length
			);

			// Send UPDATE request to the parity servers
			this->sendModifyChunkRequest(
				event.instanceId, event.requestId,
				keyValueUpdate.size,
				false,
				keyValueUpdate.data,
				metadata,
				0, // chunkUpdateOffset
				keyValueUpdate.length, // deltaSize
				keyValueUpdate.offset,
				valueUpdate,
				false, // isSealed
				true,  // isUpdate
				event.timestamp,
				event.socket,
				header.original, header.reconstructed, header.reconstructedCount,
				false, // reconstructParity - already reconstructed
				0,     // chunk
				false, // endOfDegradedOp
				true   // checkGetChunk
			);
		}
	} else if ( chunk ) {
		// Key not found
		event.resUpdate(
			event.socket, event.instanceId, event.requestId, key,
			header.data.keyValueUpdate.valueUpdateOffset,
			header.data.keyValueUpdate.valueUpdateSize,
			false, /* success */
			false, /* needsFree */
			true   /* isDegraded */
		);
		this->dispatch( event );
	} else if ( dmap->findRemovedChunk( listId, stripeId, chunkId ) ) {
		// Chunk migrated
		event.resUpdate(
			event.socket, event.instanceId, event.requestId, key,
			header.data.keyValueUpdate.valueUpdateOffset,
			header.data.keyValueUpdate.valueUpdateSize,
			false, /* success */
			false, /* needsFree */
			true   /* isDegraded */
		);
		this->dispatch( event );
	} else {
force_degraded_read:
		bool isReconstructed;

		key.set( header.data.keyValueUpdate.keySize, header.data.keyValueUpdate.key );
		key.dup();
		keyValueUpdate.dup( key.size, key.data, ( void * ) event.socket );

		// Backup valueUpdate
		char *valueUpdate = new char[ keyValueUpdate.length ];
		memcpy( valueUpdate, header.data.keyValueUpdate.valueUpdate, keyValueUpdate.length );
		keyValueUpdate.ptr = valueUpdate;

		ret = this->performDegradedRead(
			PROTO_OPCODE_DEGRADED_UPDATE,
			event.socket,
			event.instanceId, event.requestId,
			listId, stripeId, chunkId,
			&key, header.isSealed,
			header.original, header.reconstructed, header.reconstructedCount,
			header.ongoingAtChunk, header.numSurvivingChunkIds, header.survivingChunkIds,
			isReconstructed,
			&keyValueUpdate,
			event.timestamp
		);

		if ( ! ret ) {
			key.free();
			delete[] valueUpdate;
			if ( isReconstructed ) {
				// UPDATE data chunk and reconstructed parity chunks
				return this->handleUpdateRequest(
					event, header.data.keyValueUpdate,
					header.original, header.reconstructed, header.reconstructedCount,
					false, // reconstructParity
					0,     // chunks
					false, // endOfDegradedOp
					true   // checkGetChunk
				);
			} else {
				event.resUpdate(
					event.socket, event.instanceId, event.requestId, key,
					header.data.keyValueUpdate.valueUpdateOffset,
					header.data.keyValueUpdate.valueUpdateSize,
					false, /* success */
					false, /* needsFree */
					true   /* isDegraded */
				);
				this->dispatch( event );
				__ERROR__( "ServerWorker", "handleDegradedUpdateRequest", "Failed to perform degraded read on (%u, %u, %u); key: %.*s.", listId, stripeId, chunkId, key.size, key.data );
			}
		}
	}

	return ret;
}

bool ServerWorker::handleDegradedDeleteRequest( ClientEvent event, char *buf, size_t size ) {
	struct DegradedReqHeader header;
	uint32_t listId, stripeId, chunkId;
	bool reconstructParity, reconstructData;
	int index = -1;
	if ( ! this->protocol.parseDegradedReqHeader( header, PROTO_OPCODE_DEGRADED_DELETE, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleDegradedDeleteRequest", "Invalid degraded DELETE request." );
		return false;
	}
	__DEBUG__(
		BLUE, "ServerWorker", "handleDegradedDeleteRequest",
		"[DELETE] Key: %.*s (key size = %u).",
		( int ) header.data.key.keySize,
		header.data.key.key,
		header.data.key.keySize
	);

	if ( header.reconstructedCount ) {
		stripeId = header.stripeId;
		this->getServers(
			header.data.keyValueUpdate.key,
			header.data.keyValueUpdate.keySize,
			listId,
			chunkId
		);
		index = this->findInRedirectedList(
			header.original, header.reconstructed, header.reconstructedCount,
			header.ongoingAtChunk, reconstructParity, reconstructData,
			chunkId, header.isSealed
		);
	} else {
		// Use normal flow
		return this->handleDeleteRequest( event, header.data.key );
	}

	////////// Degraded read //////////
	Key key;
	KeyValue keyValue;
	KeyMetadata keyMetadata;
	Metadata metadata;
	bool ret = true, isSealed, isKeyValueFound;
	DegradedMap *dmap = &ServerWorker::degradedChunkBuffer->map;
	Chunk *chunk;

	keyMetadata.offset = 0;

	if ( index == -1 ) {
		// Data chunk is NOT reconstructed
		__ERROR__( "ServerWorker", "handleDegradedDeleteRequest", "TODO: Handle the case when the data chunk does NOT need reconstruction." );
		key.set( header.data.keyValueUpdate.keySize, header.data.keyValueUpdate.key );
		goto force_degraded_read;
	}

	// Check if the chunk is already fetched
	chunk = dmap->findChunkById( listId, stripeId, chunkId );
	// Check if the key exists or is in a unsealed chunk
	isKeyValueFound = dmap->findValueByKey(
		header.data.key.key,
		header.data.key.keySize,
		header.isLarge,
		isSealed,
		&keyValue, &key, &keyMetadata
	);
	// Set up metadata
	metadata.set( listId, stripeId, chunkId );

	if ( isKeyValueFound ) {
		key.dup( 0, 0, ( void * ) event.socket );
		if ( ! ServerWorker::pending->insertKey( PT_CLIENT_DEL, event.instanceId, event.requestId, ( void * ) event.socket, key ) ) {
			__ERROR__( "ServerWorker", "handleDegradedDeleteRequest", "Cannot insert into client DELETE pending map." );
		}

		uint32_t timestamp;
		uint32_t deltaSize = this->buffer.size;
		char *delta = this->buffer.data;

		if ( chunk ) {
			ServerWorker::degradedChunkBuffer->deleteKey(
				PROTO_OPCODE_DELETE, timestamp,
				key.size, key.data,
				metadata,
				true, /* isSealed */
				deltaSize, delta, chunk
			);

			// Send DELETE_CHUNK request to the parity servers
			this->sendModifyChunkRequest(
				event.instanceId, event.requestId,
				key.size,
				false,
				key.data,
				metadata,
				keyMetadata.offset,
				deltaSize,
				0,   /* valueUpdateOffset */
				delta,
				true /* isSealed */,
				false /* isUpdate */,
				event.timestamp,
				event.socket,
				header.original, header.reconstructed, header.reconstructedCount,
				false // reconstructParity - already reconstructed
			);
		} else {
			uint32_t tmp = 0;
			ServerWorker::degradedChunkBuffer->deleteKey(
				PROTO_OPCODE_DELETE, timestamp,
				key.size, key.data,
				metadata,
				false,
				tmp, 0, 0
			);

			// Send DELETE request to the parity servers
			this->sendModifyChunkRequest(
				event.instanceId, event.requestId,
				key.size,
				false,
				key.data,
				metadata,
				// not needed for deleting a key-value pair in an unsealed chunk:
				0, 0, 0, 0,
				false /* isSealed */,
				false /* isUpdate */,
				event.timestamp,
				event.socket,
				header.original, header.reconstructed, header.reconstructedCount,
				false // reconstructParity - already reconstructed
			);
		}
	} else if ( chunk ) {
		// Key not found
		event.resDelete(
			event.socket, event.instanceId, event.requestId, key,
			false, /* needsFree */
			true   /* isDegraded */
		);
		this->dispatch( event );
	} else {
force_degraded_read:
		bool isReconstructed;
		key.dup();
		ret = this->performDegradedRead(
			PROTO_OPCODE_DEGRADED_DELETE,
			event.socket,
			event.instanceId, event.requestId,
			listId, stripeId, chunkId,
			&key, header.isSealed,
			header.original, header.reconstructed, header.reconstructedCount,
			header.ongoingAtChunk, header.numSurvivingChunkIds, header.survivingChunkIds,
			isReconstructed,
			0, event.timestamp
		);

		if ( ! ret ) {
			if ( isReconstructed ) {
				// Use normal flow
				return this->handleDeleteRequest( event, header.data.key );
			} else {
				__ERROR__( "ServerWorker", "handleDegradedDeleteRequest", "Failed to perform degraded read on (%u, %u, %u).", listId, stripeId, chunkId );
			}
		}
	}

	return ret;
}

bool ServerWorker::handleForwardChunkRequest( ServerPeerEvent event, char *buf, size_t size ) {
	struct ChunkDataHeader header;
	if ( ! this->protocol.parseChunkDataHeader( header, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleForwardChunkRequest", "Invalid FORWARD_CHUNK request." );
		return false;
	}
	return this->handleForwardChunkRequest( header, true );
}

bool ServerWorker::handleForwardChunkRequest( struct ChunkDataHeader &header, bool xorIfExists ) {
	std::unordered_map<Metadata, Chunk *> *cache;
	LOCK_T *lock;
	DegradedMap *dmap = &ServerWorker::degradedChunkBuffer->map;

	__DEBUG__(
		GREEN, "ServerWorker", "handleForwardChunkRequest",
		"Received forwarded chunk: (%u, %u, %u; size = %u).",
		header.listId, header.stripeId, header.chunkId,
		header.size
	);

	dmap->getCacheMap( cache, lock );

	LOCK( lock );
	Chunk *chunk = dmap->findChunkById(
		header.listId, header.stripeId, header.chunkId,
		0, false, false
	);
	if ( chunk ) {
		// Already exists
		xorIfExists = true;
		if ( xorIfExists ) {
			char *parity = ChunkUtil::getData( chunk );
			Coding::bitwiseXOR(
				parity + header.offset,
				parity + header.offset,
				header.data,
				header.size
			);
		}
	} else {
		chunk = this->tempChunkPool.alloc();
		ChunkUtil::set( chunk, header.listId, header.stripeId, header.chunkId );
		ChunkUtil::copy( chunk, header.offset, header.data, header.size );

		bool ret = dmap->insertChunk(
			header.listId, header.stripeId, header.chunkId, chunk,
			header.chunkId >= ServerWorker::dataChunkCount,
			false, false
		);

		if ( ! ret ) {
			__ERROR__(
				"ServerWorker", "handleForwardChunkRequest",
				"This chunk (%u, %u, %u) cannot be inserted to DegradedMap.",
				header.listId, header.stripeId, header.chunkId
			);
			this->tempChunkPool.free( chunk );
		}
	}
	UNLOCK( lock );

	// Check if there are any pending degraded operations that wait for the forwarded chunk
	std::vector<struct pid_s> pids;
	if ( dmap->deleteDegradedChunk( header.listId, header.stripeId, header.chunkId, pids ) ) {
		PendingIdentifier pid;
		DegradedOp op;
		Key key;
		for ( int pidsIndex = 0, len = pids.size(); pidsIndex < len; pidsIndex++ ) {
			if ( ! ServerWorker::pending->eraseDegradedOp( PT_SERVER_PEER_DEGRADED_OPS, pids[ pidsIndex ].instanceId, pids[ pidsIndex ].requestId, 0, &pid, &op ) ) {
				__ERROR__( "ServerWorker", "handleForwardChunkRequest", "Cannot find a pending server DEGRADED_OPS request that matches the response. This message will be discarded." );
				continue;
			}

			switch( op.opcode ) {
				case PROTO_OPCODE_DEGRADED_GET:
				case PROTO_OPCODE_DEGRADED_DELETE:
					key.set( op.data.key.size, op.data.key.data, 0, op.data.key.isLarge );
					break;
				case PROTO_OPCODE_DEGRADED_UPDATE:
					key.set( op.data.keyValueUpdate.size, op.data.keyValueUpdate.data, 0, op.data.key.isLarge );
					break;
				default:
					continue;
			}

			// Find the chunk from the map
			ClientEvent clientEvent;
			clientEvent.instanceId = pid.parentInstanceId;
			clientEvent.requestId = pid.parentRequestId;
			clientEvent.socket = op.socket;

			KeyValue keyValue;
			KeyMetadata keyMetadata;
			uint32_t listId, stripeId, chunkId;
			DegradedMap *dmap = &ServerWorker::degradedChunkBuffer->map;

			stripeId = op.stripeId;
			this->getServers( key.data, key.size, listId, chunkId );

			// Check if the chunk is already fetched //
			Chunk *chunk = dmap->findChunkById(
				listId, stripeId, chunkId
			);
			// Check if the key exists or is in a unsealed chunk //
			bool isSealed;
			bool isKeyValueFound = dmap->findValueByKey(
				key.data, key.size,
				key.isLarge,
				isSealed,
				&keyValue, 0, &keyMetadata
			);

			if ( op.opcode == PROTO_OPCODE_DEGRADED_GET ) {
				if ( isKeyValueFound ) {
					// Send the key-value pair to the client
					clientEvent.resGet(
						clientEvent.socket,
						clientEvent.instanceId,
						clientEvent.requestId,
						keyValue, true // isDegraded
					);
					this->dispatch( clientEvent );
				} else {
					// Key not found
					clientEvent.resGet(
						clientEvent.socket,
						clientEvent.instanceId,
						clientEvent.requestId,
						key, true // isDegraded
					);
					this->dispatch( clientEvent );
				}
			} else if ( op.opcode == PROTO_OPCODE_DEGRADED_UPDATE ) {
				if ( isKeyValueFound ) {
					struct DegradedReqHeader header;
					header.isSealed = chunk != 0;
					header.stripeId = stripeId;
					header.reconstructedCount = op.reconstructedCount;
					header.original = op.original;
					header.reconstructed = op.reconstructed;
					header.ongoingAtChunk = op.ongoingAtChunk;
					header.data.keyValueUpdate.keySize = key.size;
					header.data.keyValueUpdate.valueUpdateSize = op.data.keyValueUpdate.length;
					header.data.keyValueUpdate.valueUpdateOffset = op.data.keyValueUpdate.offset;
					header.data.keyValueUpdate.key = key.data,
					header.data.keyValueUpdate.valueUpdate = ( char * ) op.data.keyValueUpdate.ptr;

					this->handleDegradedUpdateRequest( clientEvent, header, true );
				} else {
					// Key not found
					clientEvent.resUpdate(
						clientEvent.socket,
						clientEvent.instanceId,
						clientEvent.requestId,
						key,
						op.data.keyValueUpdate.offset,
						op.data.keyValueUpdate.length,
						false, // success
						false, // needsFree
						true   // isDegraded
					);
					this->dispatch( clientEvent );
				}
			} else if ( op.opcode == PROTO_OPCODE_DEGRADED_DELETE ) {
				// TODO
			}
		}
	}

	return true;
}

bool ServerWorker::handleForwardChunkResponse( ServerPeerEvent event, bool success, char *buf, size_t size ) {
	struct ChunkHeader header;
	if ( ! this->protocol.parseChunkHeader( header, buf, size ) ) {
		__ERROR__( "ServerWorker", "handleForwardChunkResponse", "Invalid FORWARD_CHUNK response." );
		return false;
	}
	__DEBUG__(
		BLUE, "ServerWorker", "handleForwardChunkResponse",
		"[%u, %u] Chunk: (%u, %u, %u) is received.",
		event.instanceId, event.requestId,
		header.listId, header.stripeId, header.chunkId
	);

	return true;
}

bool ServerWorker::performDegradedRead(
	uint8_t opcode,
	ClientSocket *clientSocket,
	uint16_t parentInstanceId, uint32_t parentRequestId,
	uint32_t listId, uint32_t stripeId, uint32_t chunkId,
	Key *key, bool isSealed,
	uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
	uint32_t ongoingAtChunk, uint8_t numSurvivingChunkIds, uint32_t *survivingChunkIds,
	bool &isReconstructed,
	KeyValueUpdate *keyValueUpdate, uint32_t timestamp
) {
	Key mykey;
	ServerPeerEvent event;
	ServerPeerSocket *socket = 0;
	uint32_t selected = 0;

	ServerWorker::stripeList->get( listId, this->parityServerSockets, this->dataServerSockets );

	// Determine the list of surviving nodes
	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		uint32_t originalChunkId = original[ i * 2 + 1 ];
		if ( originalChunkId < ServerWorker::dataChunkCount ) {
			this->dataServerSockets[ originalChunkId ] = 0;
		} else {
			this->parityServerSockets[ originalChunkId - ServerWorker::dataChunkCount ] = 0;
		}
	}

	if ( isSealed ) {
		// Check whether the number of surviving nodes >= k
		if ( ! ( ServerWorker::chunkCount - reconstructedCount >= ServerWorker::dataChunkCount ) ) {
			__ERROR__( "ServerWorker", "performDegradedRead", "The number of surviving nodes is less than k. The data cannot be recovered." );
			return false;
		}
	} else {
		if ( this->dataServerSockets[ chunkId ] && this->dataServerSockets[ chunkId ]->self ) {
			socket = this->dataServerSockets[ chunkId ];
		} else {
			// Check whether there are surviving parity servers
			uint32_t numSurvivingParity = ServerWorker::parityChunkCount;
			for ( uint32_t j = 0; j < reconstructedCount; j++ ) {
				if ( original[ j * 2 + 1 ] >= ServerWorker::dataChunkCount ) {
					numSurvivingParity--;
				}
			}
			if ( numSurvivingParity == 0 ) {
				__ERROR__(
					"ServerWorker", "performDegradedRead",
					"There are no surviving parity servers. The data cannot be recovered. (numSurvivingParity = %u, reconstructedCount = %u)",
					numSurvivingParity, reconstructedCount
				);
				return false;
			} else {
				for ( uint32_t i = 0; i < ServerWorker::parityChunkCount; i++ ) {
					ServerPeerSocket *tmp = this->parityServerSockets[ ( parentRequestId + i ) % ServerWorker::parityChunkCount ]; // Add "randomness"
					if ( tmp ) {
						socket = tmp;
						break;
					}
				}
			}
		}
	}

	// Add to degraded operation pending set
	uint16_t instanceId = Server::instanceId;
	uint32_t requestId = ServerWorker::idGenerator->nextVal( this->workerId );
	DegradedOp op;
	op.set(
		opcode, isSealed, clientSocket,
		listId, stripeId, chunkId,
		original, reconstructed, reconstructedCount,
		ongoingAtChunk,
		timestamp, true
	);
	if ( opcode == PROTO_OPCODE_DEGRADED_UPDATE ) {
		op.data.keyValueUpdate = *keyValueUpdate;
		mykey.set( keyValueUpdate->size, keyValueUpdate->data, 0, keyValueUpdate->isLarge );
	} else if ( opcode ) {
		op.data.key = *key;
		mykey.set( key->size, key->data, 0, key->isLarge );
	}

	if ( isSealed || ! socket->self ) {
		if ( ! ServerWorker::pending->insertDegradedOp( PT_SERVER_PEER_DEGRADED_OPS, instanceId, parentInstanceId, requestId, parentRequestId, 0, op ) ) {
			__ERROR__( "ServerWorker", "performDegradedRead", "Cannot insert into server DEGRADED_OPS pending map." );
		}
	}

	// Insert the degraded operation into degraded chunk buffer pending set
	Key k;
	bool needsContinue;

	if ( isSealed ) {
		if ( ongoingAtChunk != ServerWorker::chunkBuffer->at( listId )->getChunkId() ) {
			ServerWorker::degradedChunkBuffer->map.insertDegradedChunk(
				listId, stripeId, chunkId,
				instanceId, requestId,
				isReconstructed
			);
			needsContinue = false;
			isReconstructed = false;
		} else {
			needsContinue = ServerWorker::degradedChunkBuffer->map.insertDegradedChunk(
				listId, stripeId, chunkId,
				instanceId, requestId,
				isReconstructed
			);
		}

		if ( ! needsContinue ) {
			if ( isReconstructed ) {
				// The chunk is already reconstructed
				return false;
			} else {
				// Reconstruction in progress
				return true;
			}
		}

force_reconstruct_chunks:
		// Send GET_CHUNK requests to surviving nodes
		Metadata metadata;
		metadata.set( listId, stripeId, 0 );
		selected = 0;

		for ( uint32_t x = 0; x < numSurvivingChunkIds; x++ ) {
			uint32_t i = survivingChunkIds[ x ];

			if ( selected >= ServerWorker::dataChunkCount )
				break;

			socket = ( i < ServerWorker::dataChunkCount ) ?
			         ( this->dataServerSockets[ i ] ) :
			         ( this->parityServerSockets[ i - ServerWorker::dataChunkCount ] );

			if ( ! socket )
				continue;

			// Add to pending GET_CHUNK request set
			ChunkRequest chunkRequest;
			chunkRequest.set( listId, stripeId, i, socket, 0, true );
			if ( socket->self ) {
				chunkRequest.self = true;
				assert( chunkRequest.chunkId == ServerWorker::chunkBuffer->at( listId )->getChunkId() );
			} else if ( socket->ready() ) {
				chunkRequest.chunk = 0;
			} else {
				continue;
			}
			if ( ! ServerWorker::pending->insertChunkRequest( PT_SERVER_PEER_GET_CHUNK, instanceId, parentInstanceId, requestId, parentRequestId, socket, chunkRequest ) ) {
				__ERROR__( "ServerWorker", "performDegradedRead", "Cannot insert into server CHUNK_REQUEST pending map." );
			}
			selected++;
		}

		selected = 0;
		for ( uint32_t x = 0; x < numSurvivingChunkIds; x++ ) {
			uint32_t i = survivingChunkIds[ x ];

			if ( selected >= ServerWorker::dataChunkCount )
				break;

			socket = ( i < ServerWorker::dataChunkCount ) ?
			         ( this->dataServerSockets[ i ] ) :
			         ( this->parityServerSockets[ i - ServerWorker::dataChunkCount ] );

			if ( ! socket )
				continue;

			if ( socket->self ) {
				selected++;
			} else if ( socket->ready() ) {
				metadata.chunkId = i;
				event.reqGetChunk( socket, instanceId, requestId, metadata );
				ServerWorker::eventQueue->insert( event );
				selected++;
			}
		}

		return ( selected >= ServerWorker::dataChunkCount );
	} else {
		if ( opcode == PROTO_OPCODE_DEGRADED_UPDATE )
			k.set( keyValueUpdate->size, keyValueUpdate->data, 0, keyValueUpdate->isLarge );
		else
			k = *key;
		needsContinue = ServerWorker::degradedChunkBuffer->map.insertDegradedKey( k, instanceId, requestId, isReconstructed );

		////////// Key-value pairs in unsealed chunks //////////
		if ( ! needsContinue || isReconstructed ) {
			return false;
		}

		bool success = true;
		if ( socket->self ) {
			// Get the requested key-value pairs from local key-value store
			KeyValue keyValue;
			ClientEvent clientEvent;
			ServerPeerEvent serverPeerEvent;
			char *keyStr = 0, *valueStr = 0;
			uint8_t keySize = 0;
			uint32_t valueSize = 0, splitOffset = 0;

			if ( mykey.isLarge ) {
				success = ServerWorker::map->findObject( mykey.data, mykey.size, &keyValue, &mykey ) != 0;
			} else {
				success = ServerWorker::map->findLargeObject( mykey.data, mykey.size, &keyValue, &mykey ) != 0;
			}

			if ( ! success )
				success = ServerWorker::chunkBuffer->at( listId )->findValueByKey( mykey.data, mykey.size, mykey.isLarge, &keyValue, &mykey );

			if ( success ) {
				keyValue.deserialize( keyStr, keySize, valueStr, valueSize, splitOffset );
				keyValue.dup( keyStr, keySize, valueStr, valueSize, splitOffset );
			} else {
				__ERROR__( "ServerWorker", "performDegradedRead", "findValueByKey() failed (list ID: %u, key: %.*s).", listId, mykey.size, mykey.data );
			}

			// Insert into degraded chunk buffer if this is not the original data server (i.e., the data server fails)
			if ( ! this->dataServerSockets[ chunkId ]->self ) {
				if ( success && opcode != PROTO_OPCODE_DEGRADED_DELETE ) {
					// Insert into degradedChunkBuffer
					Metadata metadata;
					metadata.set( listId, stripeId, chunkId );
					if ( ! ServerWorker::degradedChunkBuffer->map.insertValue( keyValue, metadata ) ) {
						__ERROR__( "ServerWorker", "performDegradedRead", "Cannot insert into degraded chunk buffer values map. (Key: %.*s)", keySize, keyStr );
						keyValue.free();
						success = false;
					}
				}
			}

			// Forward the key-value pair to the reconstructed servers
			if ( success ) {
				// Need to send the key-value pair to reconstructed servers even if this is a DELETE request because of the need of creating backup
				if ( ! ServerWorker::pending->insertDegradedOp( PT_SERVER_PEER_DEGRADED_OPS, instanceId, parentInstanceId, requestId, parentRequestId, 0, op ) ) {
					__ERROR__( "ServerWorker", "performDegradedRead", "Cannot insert into server DEGRADED_OPS pending map." );
				}

				for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
					ServerPeerSocket *s = ServerWorker::stripeList->get(
						reconstructed[ i * 2     ],
						reconstructed[ i * 2 + 1 ]
					);
					if ( opcode == PROTO_OPCODE_DEGRADED_UPDATE ) {
						if ( s->self ) {
							struct ForwardKeyHeader forwardKeyHeader;
							ServerPeerEvent emptyEvent;

							forwardKeyHeader = {
								.opcode = opcode,
								.listId = listId,
								.stripeId = stripeId,
								.chunkId = original[ i * 2 + 1 ],
								.keySize = keySize,
								.valueSize = valueSize,
								.key = keyStr,
								.value = valueStr,
								.valueUpdateSize = keyValueUpdate->length,
								.valueUpdateOffset = keyValueUpdate->offset,
								.valueUpdate = ( char * ) keyValueUpdate->ptr
							};

							this->handleForwardKeyRequest( emptyEvent, forwardKeyHeader, true );
						} else {
							serverPeerEvent.reqForwardKey(
								s,
								opcode,
								instanceId, requestId,
								listId, stripeId, original[ i * 2 + 1 ],
								keySize, valueSize,
								keyStr, valueStr,
								keyValueUpdate->offset,
								keyValueUpdate->length,
								( char * ) keyValueUpdate->ptr
							);
						}
					} else {
						if ( s->self ) {
							struct ForwardKeyHeader forwardKeyHeader;
							ServerPeerEvent emptyEvent;

							forwardKeyHeader = {
								.opcode = opcode,
								.listId = listId,
								.stripeId = stripeId,
								.chunkId = original[ i * 2 + 1 ],
								.keySize = keySize,
								.valueSize = valueSize,
								.key = keyStr,
								.value = valueStr
							};

							this->handleForwardKeyRequest( emptyEvent, forwardKeyHeader, true );
						} else {
							serverPeerEvent.reqForwardKey(
								s,
								opcode,
								instanceId, requestId,
								listId, stripeId, original[ i * 2 + 1 ],
								keySize, valueSize,
								keyStr, valueStr
							);
						}
					}
					if ( ! s->self ) {
						this->dispatch( serverPeerEvent );
					}
				}
			} else {
				switch( opcode ) {
					case PROTO_OPCODE_DEGRADED_GET:
						// Return failure to client
						clientEvent.resGet( clientSocket, parentInstanceId, parentRequestId, mykey, true );
						this->dispatch( clientEvent );
						op.data.key.free();
						break;
					case PROTO_OPCODE_DEGRADED_UPDATE:
						clientEvent.resUpdate(
							clientSocket, parentInstanceId, parentRequestId, mykey,
							keyValueUpdate->offset,
							keyValueUpdate->length,
							false, false, true
						);
						this->dispatch( clientEvent );
						op.data.keyValueUpdate.free();
						delete[] ( ( char * ) op.data.keyValueUpdate.ptr );
						break;
					case PROTO_OPCODE_DEGRADED_DELETE:
						clientEvent.resDelete(
							clientSocket,
							parentInstanceId, parentRequestId,
							mykey,
							false, // needsFree
							true   // isDegraded
						);
						this->dispatch( clientEvent );
						op.data.key.free();
						break;
				}
			}
		} else {
			// Send GET request to surviving parity server
			if ( ! ServerWorker::pending->insertKey( PT_SERVER_PEER_GET, instanceId, parentInstanceId, requestId, parentRequestId, socket, op.data.key ) ) {
				__ERROR__( "ServerWorker", "performDegradedRead", "Cannot insert into server GET pending map." );
			}
			event.reqGet( socket, instanceId, requestId, listId, chunkId, op.data.key );
			this->dispatch( event );
		}

		///////////////////////////////////////////////////////////
		// Reconstructed sealed chunks that need to be forwarded //
		///////////////////////////////////////////////////////////
		if ( ongoingAtChunk == ServerWorker::chunkBuffer->at( listId )->getChunkId() ) {
			// Check if there are any other lost data chunks
			uint32_t lostDataChunkId = ServerWorker::chunkCount;

			needsContinue = false;
			for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
				if ( original[ i * 2 + 1 ] < ServerWorker::dataChunkCount ) {
					lostDataChunkId = original[ i * 2 + 1 ];
				}
				if ( original[ i * 2 + 1 ] < ServerWorker::dataChunkCount && original[ i * 2 + 1 ] != chunkId ) {
					needsContinue = true;
					break;
				}
			}

			if ( needsContinue ) {
				needsContinue = ServerWorker::degradedChunkBuffer->map.insertDegradedChunk(
					listId, stripeId, lostDataChunkId == ServerWorker::chunkCount ? chunkId : lostDataChunkId,
					isReconstructed
				);
			}

			if ( needsContinue ) {
				op.opcode = 0;
				requestId = ServerWorker::idGenerator->nextVal( this->workerId );
				if ( ! ServerWorker::pending->insertDegradedOp( PT_SERVER_PEER_DEGRADED_OPS, instanceId, parentInstanceId, requestId, parentRequestId, 0, op ) ) {
					__ERROR__( "ServerWorker", "performDegradedRead", "Cannot insert into server DEGRADED_OPS pending map." );
				}
				goto force_reconstruct_chunks;
			}
		}

		return success;
	}
}

bool ServerWorker::sendModifyChunkRequest(
	uint16_t parentInstanceId, uint32_t parentRequestId,
	uint8_t keySize, bool isLarge, char *keyStr,
	Metadata &metadata, uint32_t offset,
	uint32_t deltaSize, uint32_t valueUpdateOffset, char *delta,
	bool isSealed, bool isUpdate,
	uint32_t timestamp, ClientSocket *clientSocket,
	uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
	bool reconstructParity,
	Chunk **chunks, bool endOfDegradedOp,
	bool checkGetChunk
) {
	Key key;
	KeyValueUpdate keyValueUpdate;
	uint16_t instanceId = Server::instanceId;
	uint32_t requestId = ServerWorker::idGenerator->nextVal( this->workerId );
	bool isDegraded = original && reconstructed && reconstructedCount;

	key.set( keySize, keyStr, 0, isLarge );
	keyValueUpdate.set( keySize, keyStr );
	keyValueUpdate.offset = valueUpdateOffset;
	keyValueUpdate.length = deltaSize;
	this->getServers( metadata.listId );

	if ( isDegraded ) {
		for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
			if ( original[ i * 2 + 1 ] >= ServerWorker::dataChunkCount ) {
				if ( reconstructParity ) {
					this->forward.chunks[ original[ i * 2 + 1 ] ] = chunks[ original[ i * 2 + 1 ] ];
					this->parityServerSockets[ original[ i * 2 + 1 ] - ServerWorker::dataChunkCount ] = 0;
				} else {
					this->parityServerSockets[ original[ i * 2 + 1 ] - ServerWorker::dataChunkCount ] = ServerWorker::stripeList->get(
						reconstructed[ i * 2     ],
						reconstructed[ i * 2 + 1 ]
					);
				}
			}
		}
	}

	if ( isSealed ) {
		// Send UPDATE_CHUNK / DELETE_CHUNK requests to parity servers if the chunk is sealed
		ChunkUpdate chunkUpdate;
		chunkUpdate.set(
			metadata.listId, metadata.stripeId, metadata.chunkId,
			offset, deltaSize
		);
		chunkUpdate.setKeyValueUpdate( key.size, key.data, offset );

		for ( uint32_t i = 0; i < ServerWorker::parityChunkCount; i++ ) {
			if ( ! this->parityServerSockets[ i ] || this->parityServerSockets[ i ]->self ) {
				continue;
			}

			chunkUpdate.chunkId = ServerWorker::dataChunkCount + i; // updatingChunkId
			chunkUpdate.ptr = ( void * ) this->parityServerSockets[ i ];
			if ( ! ServerWorker::pending->insertChunkUpdate(
				isUpdate ? PT_SERVER_PEER_UPDATE_CHUNK : PT_SERVER_PEER_DEL_CHUNK,
				instanceId, parentInstanceId, requestId, parentRequestId,
				( void * ) this->parityServerSockets[ i ],
				chunkUpdate
			) ) {
				__ERROR__( "ServerWorker", "sendModifyChunkRequest", "Cannot insert into server %s pending map.", isUpdate ? "UPDATE_CHUNK" : "DELETE_CHUNK" );
			}
		}

		// Start sending packets only after all the insertion to the server peer DELETE_CHUNK pending set is completed
		uint32_t numSurvivingParity = 0;
		for ( uint32_t i = 0; i < ServerWorker::parityChunkCount; i++ ) {
			if ( ! this->parityServerSockets[ i ] ) {
				/////////////////////////////////////////////////////
				// Update the parity chunks that will be forwarded //
				/////////////////////////////////////////////////////
				// Update in the chunks pointer
				if ( ! isDegraded ) {
					__ERROR__( "ServerWorker", "sendModifyChunkRequest", "Invalid degraded operation." );
					continue;
				}

				// Prepare the data delta
				ChunkUtil::clear( this->forward.dataChunk );
				ChunkUtil::copy( this->forward.dataChunk, offset, delta, deltaSize );

				// Prepare the stripe
				for ( uint32_t j = 0; j < ServerWorker::dataChunkCount; j++ ) {
					this->forward.chunks[ j ] = Coding::zeros;
				}
				this->forward.chunks[ metadata.chunkId ] = this->forward.dataChunk;

				ChunkUtil::clear( this->forward.parityChunk );

				// Compute parity delta
				Server::getInstance()->coding->encode(
					this->forward.chunks, this->forward.parityChunk,
					i + 1, // Parity chunk index
					offset + metadata.chunkId * ChunkUtil::chunkSize,
					offset + metadata.chunkId * ChunkUtil::chunkSize + deltaSize
				);

				char *parity = ChunkUtil::getData( this->forward.chunks[ i + ServerWorker::dataChunkCount ] );
				Coding::bitwiseXOR(
					parity,
					parity,
					ChunkUtil::getData( this->forward.parityChunk ),
					ChunkUtil::chunkSize
				);
				/////////////////////////////////////////////////////
			} else if ( this->parityServerSockets[ i ]->self ) {
				uint32_t myChunkId = ServerWorker::chunkBuffer->at( metadata.listId )->getChunkId(), tmpChunkId;
				if ( myChunkId >= ServerWorker::dataChunkCount ) {
					bool checkGetChunk = true; // Force backup
					if ( checkGetChunk ) {
						Chunk *chunk = map->findChunkById( metadata.listId, metadata.stripeId, myChunkId );
						MixedChunkBuffer *chunkBuffer = ServerWorker::chunkBuffer->at( metadata.listId );

						uint8_t sealIndicatorCount = 0;
						bool *sealIndicator = 0;
						LOCK_T *parityChunkBufferLock = 0;

						sealIndicator = chunkBuffer->getSealIndicator( metadata.stripeId, sealIndicatorCount, true, false, &parityChunkBufferLock );

						tmpChunkId = metadata.chunkId;
						metadata.chunkId = tmpChunkId;
						ServerWorker::getChunkBuffer->insert( metadata, chunk, sealIndicatorCount, sealIndicator );
						metadata.chunkId = tmpChunkId;

						if ( parityChunkBufferLock )
							UNLOCK( parityChunkBufferLock );
					}
					ServerWorker::chunkBuffer->at( metadata.listId )->update(
						metadata.stripeId, metadata.chunkId,
						offset, deltaSize, delta,
						this->chunks, this->dataChunk, this->parityChunk,
						! isUpdate /* isDelete */
					);
				}
			} else {
				// Prepare DELETE_CHUNK request
				size_t size;
				Packet *packet = ServerWorker::packetPool->malloc();
				packet->setReferenceCount( 1 );

				if ( isUpdate ) {
					size = this->protocol.generateChunkUpdateHeader(
						PROTO_MAGIC_REQUEST, PROTO_MAGIC_TO_SERVER,
						checkGetChunk ? PROTO_OPCODE_UPDATE_CHUNK_CHECK : PROTO_OPCODE_UPDATE_CHUNK,
						parentInstanceId, requestId,
						metadata.listId, metadata.stripeId, metadata.chunkId,
						offset,
						deltaSize,                        // length
						ServerWorker::dataChunkCount + i, // updatingChunkId
						delta,
						packet->data,
						timestamp
					);
				} else {
					size = this->protocol.generateChunkUpdateHeader(
						PROTO_MAGIC_REQUEST, PROTO_MAGIC_TO_SERVER,
						checkGetChunk ? PROTO_OPCODE_DELETE_CHUNK_CHECK : PROTO_OPCODE_DELETE_CHUNK,
						parentInstanceId, requestId,
						metadata.listId, metadata.stripeId, metadata.chunkId,
						offset,
						deltaSize,                       // length
						ServerWorker::dataChunkCount + i, // updatingChunkId
						delta,
						packet->data,
						timestamp
					);
				}
				packet->size = ( uint32_t ) size;

				// Insert into event queue
				ServerPeerEvent serverPeerEvent;
				serverPeerEvent.send( this->parityServerSockets[ i ], packet );
				numSurvivingParity++;

#ifdef SERVER_WORKER_SEND_REPLICAS_PARALLEL
				if ( i == ServerWorker::parityChunkCount - 1 )
					this->dispatch( serverPeerEvent );
				else
					ServerWorker::eventQueue->prioritizedInsert( serverPeerEvent );
#else
				this->dispatch( serverPeerEvent );
#endif
			}
		}

		if ( ! numSurvivingParity ) {
			// No UPDATE_CHUNK / DELETE_CHUNK requests
			if ( isUpdate ) {
				Key key;
				KeyValueUpdate keyValueUpdate;
				ClientEvent clientEvent;
				PendingIdentifier pid;

				if ( ! ServerWorker::pending->eraseKeyValueUpdate( PT_CLIENT_UPDATE, parentInstanceId, parentRequestId, 0, &pid, &keyValueUpdate ) ) {
					__ERROR__( "ServerWorker", "sendModifyChunkRequest", "Cannot find a pending client UPDATE request that matches the response. This message will be discarded." );
					return false;
				}

				key.set( keyValueUpdate.size, keyValueUpdate.data, keyValueUpdate.ptr );
				clientEvent.resUpdate(
					( ClientSocket * ) pid.ptr, pid.instanceId, pid.requestId, key,
					keyValueUpdate.offset, keyValueUpdate.length,
					true, // success
					true,
					keyValueUpdate.isDegraded // isDegraded
				);
				this->dispatch( clientEvent );
			} else {
				printf( "TODO: Handle DELETE request.\n" );
			}
		}
	} else {
		// Send UPDATE / DELETE request if the chunk is not yet sealed

		// Check whether any of the parity servers are self-socket
		uint32_t self = 0;
		uint32_t parityServerCount = ServerWorker::parityChunkCount;
		for ( uint32_t i = 0; i < ServerWorker::parityChunkCount; i++ ) {
			if ( ! this->parityServerSockets[ i ] ) {
				parityServerCount--;
			} else if ( this->parityServerSockets[ i ]->self ) {
				parityServerCount--;
				self = i + 1;
				break;
			}
		}

		// Prepare UPDATE / DELETE request
		size_t size = 0;
		Packet *packet = 0;

		if ( parityServerCount ) {
			packet = ServerWorker::packetPool->malloc();
			packet->setReferenceCount( parityServerCount );
			if ( isUpdate ) {
				size = this->protocol.generateChunkKeyValueUpdateHeader(
					PROTO_MAGIC_REQUEST, PROTO_MAGIC_TO_SERVER,
					PROTO_OPCODE_UPDATE,
					parentInstanceId, requestId,
					metadata.listId, metadata.stripeId, metadata.chunkId,
					keySize, isLarge, keyStr,
					valueUpdateOffset,
					deltaSize, // valueUpdateSize
					offset,    // Chunk update offset
					delta,     // valueUpdate
					packet->data,
					timestamp
				);
			} else {
				size = this->protocol.generateChunkKeyHeader(
					PROTO_MAGIC_REQUEST, PROTO_MAGIC_TO_SERVER,
					PROTO_OPCODE_DELETE,
					parentInstanceId, requestId,
					metadata.listId, metadata.stripeId, metadata.chunkId,
					keySize, keyStr,
					packet->data,
					timestamp
				);
			}
			packet->size = ( uint32_t ) size;
		}

		for ( uint32_t i = 0; i < ServerWorker::parityChunkCount; i++ ) {
			if ( ! this->parityServerSockets[ i ] || this->parityServerSockets[ i ]->self )
				continue;

			if ( isUpdate ) {
				if ( ! ServerWorker::pending->insertKeyValueUpdate(
					PT_SERVER_PEER_UPDATE, instanceId, parentInstanceId, requestId, parentRequestId,
					( void * ) this->parityServerSockets[ i ],
					keyValueUpdate
				) ) {
					__ERROR__( "ServerWorker", "handleUpdateRequest", "Cannot insert into server UPDATE pending map." );
				}
			} else {
				if ( ! ServerWorker::pending->insertKey(
					PT_SERVER_PEER_DEL, instanceId, parentInstanceId, requestId, parentRequestId,
					( void * ) this->parityServerSockets[ i ],
					key
				) ) {
					__ERROR__( "ServerWorker", "handleDeleteRequest", "Cannot insert into server DELETE pending map." );
				}
			}
		}

		// Start sending packets only after all the insertion to the server peer DELETE pending set is completed
		for ( uint32_t i = 0; i < ServerWorker::parityChunkCount; i++ ) {
			if ( ! this->parityServerSockets[ i ] ) {
				continue;
			} else if ( this->parityServerSockets[ i ]->self ) {
				if ( ! isUpdate ) {
					__ERROR__( "ServerWorker", "sendModifyChunkRequest", "TODO: Handle DELETE request on self server socket." );
				}
			} else {
				// Insert into event queue
				ServerPeerEvent serverPeerEvent;
				serverPeerEvent.send( this->parityServerSockets[ i ], packet );

#ifdef SERVER_WORKER_SEND_REPLICAS_PARALLEL
				if ( i == ServerWorker::parityChunkCount - 1 )
					this->dispatch( serverPeerEvent );
				else
					ServerWorker::eventQueue->prioritizedInsert( serverPeerEvent );
#else
				this->dispatch( serverPeerEvent );
#endif
			}
		}

		if ( ! self ) {
			self--; // Get the self parity server index
			if ( isUpdate ) {
				bool ret = ServerWorker::chunkBuffer->at( metadata.listId )->updateKeyValue(
					keyStr, keySize, false,
					valueUpdateOffset, deltaSize, delta
				);
				if ( ! ret ) {
					// Use the chunkUpdateOffset
					ServerWorker::chunkBuffer->at( metadata.listId )->update(
						metadata.stripeId, metadata.chunkId,
						offset, deltaSize, delta,
						this->chunks, this->dataChunk, this->parityChunk
					);
					ret = true;
				}
			} else {
				ServerWorker::chunkBuffer->at( metadata.listId )->deleteKey( keyStr, keySize );
			}
		}
	}
	return true;
}
