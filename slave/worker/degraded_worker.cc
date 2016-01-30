#include "worker.hh"
#include "../main/slave.hh"

int SlaveWorker::findInRedirectedList( uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount, uint32_t ongoingAtChunk, bool &reconstructParity, bool &reconstructData ) {
	int ret = -1;
	bool self = false;
	uint32_t listId;

	std::vector<StripeListIndex> &lists = Slave::getInstance()->stripeListIndex;
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
			__ERROR__( "SlaveWorker", "findInRedirectedList", "This slave is not in the reconstructed list." );
			return -1;
		}
	}

	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		if ( original[ i * 2 + 1 ] >= SlaveWorker::dataChunkCount )
			reconstructParity = true;
		else
			reconstructData = true;

		if ( ret == -1 ) {
			if ( ! self && lists[ listIndex ].chunkId == ongoingAtChunk )
				self = true;
			if ( reconstructed[ i * 2 + 1 ] == lists[ listIndex ].chunkId ) {
				ret = ( int ) i;
				break;
			}
		}
	}

	reconstructParity = reconstructParity && self;

	return ret;
}

bool SlaveWorker::handleReleaseDegradedLockRequest( CoordinatorEvent event, char *buf, size_t size ) {
	struct ChunkHeader header;
	uint32_t count = 0, requestId;
	uint16_t instanceId = Slave::instanceId;

	Metadata metadata;
	ChunkRequest chunkRequest;
	std::vector<Metadata> chunks;
	SlavePeerEvent slavePeerEvent;
	SlavePeerSocket *socket = NULL;
	Chunk *chunk;

	while( size ) {
		if ( ! this->protocol.parseDegradedReleaseReqHeader( header, buf, size ) ) {
			__ERROR__( "SlaveWorker", "handleReleaseDegradedLockRequest", "Invalid DEGRADED_RELEASE request." );
			return false;
		}
		__INFO__(
			BLUE, "SlaveWorker", "handleReleaseDegradedLockRequest",
			"[DEGRADED_RELEASE] (%u, %u, %u) (count = %u).",
			header.listId, header.stripeId, header.chunkId,
			count
		);
		buf += PROTO_DEGRADED_RELEASE_REQ_SIZE;
		size -= PROTO_DEGRADED_RELEASE_REQ_SIZE;

		metadata.set( header.listId, header.stripeId, header.chunkId );
		chunks.push_back( metadata );

		count++;
	}

	SlaveWorker::pending->insertReleaseDegradedLock( event.instanceId, event.requestId, event.socket, count );

	for ( size_t i = 0, len = chunks.size(); i < len; i++ ) {
		// Determine the src
		if ( i == 0 ) {
			// The target is the same for all chunks in this request
			this->getSlaves( chunks[ i ].listId );
			socket =   chunks[ i ].chunkId < SlaveWorker::dataChunkCount
			         ? this->dataSlaveSockets[ chunks[ i ].chunkId ]
			         : this->paritySlaveSockets[ chunks[ i ].chunkId - SlaveWorker::dataChunkCount ];
		}

		requestId = SlaveWorker::idGenerator->nextVal( this->workerId );
		chunk = SlaveWorker::degradedChunkBuffer->map.deleteChunk(
			chunks[ i ].listId, chunks[ i ].stripeId, chunks[ i ].chunkId,
			&metadata
		);

		chunkRequest.set(
			chunks[ i ].listId, chunks[ i ].stripeId, chunks[ i ].chunkId,
			socket, chunk, true /* isDegraded */
		);
		if ( ! SlaveWorker::pending->insertChunkRequest( PT_SLAVE_PEER_SET_CHUNK, instanceId, event.instanceId, requestId, event.requestId, socket, chunkRequest ) ) {
			__ERROR__( "SlaveWorker", "handleReleaseDegradedLockRequest", "Cannot insert into slave CHUNK_REQUEST pending map." );
		}

		// If chunk is NULL, then the unsealed version of SET_CHUNK will be used
		slavePeerEvent.reqSetChunk( socket, instanceId, requestId, metadata, chunk, true );
		SlaveWorker::eventQueue->insert( slavePeerEvent );
	}

	return true;
}

bool SlaveWorker::handleDegradedGetRequest( MasterEvent event, char *buf, size_t size ) {
	struct DegradedReqHeader header;
	if ( ! this->protocol.parseDegradedReqHeader( header, PROTO_OPCODE_DEGRADED_GET, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleDegradedGetRequest", "Invalid degraded GET request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleDegradedGetRequest",
		"[GET] Key: %.*s (key size = %u); is sealed? %s.",
		( int ) header.data.key.keySize,
		header.data.key.key,
		header.data.key.keySize,
		header.isSealed ? "true" : "false"
	);

	int index = -1;
	bool reconstructParity, reconstructData;
	if ( header.reconstructedCount ) {
		index = this->findInRedirectedList(
			header.original, header.reconstructed, header.reconstructedCount,
			header.ongoingAtChunk, reconstructParity, reconstructData
		);
		if ( ( index == -1 ) ||
		     ( header.original[ index * 2 + 1 ] >= SlaveWorker::dataChunkCount ) ) {
			// No need to perform degraded read if only the parity slaves are redirected
			return this->handleGetRequest( event, header.data.key, true );
		}
	} else {
		// Use normal flow
		return this->handleGetRequest( event, header.data.key, true );
	}

	////////// Degraded read //////////
	uint32_t listId = header.original[ index * 2 ],
	         stripeId = header.stripeId,
	         chunkId = header.original[ index * 2 + 1 ];
	Key key;
	KeyValue keyValue;
	KeyMetadata keyMetadata;
	bool ret = true, checked = false;
	DegradedMap *dmap = &SlaveWorker::degradedChunkBuffer->map;

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
		isSealed,
		&keyValue, &key, &keyMetadata
	);

	if ( isKeyValueFound ) {
		// Send the key-value pair to the master
		event.resGet( event.socket, event.instanceId, event.requestId, keyValue, true /* isDegraded */ );
		this->dispatch( event );
	} else if ( chunk ) {
		// Key not found
		event.resGet( event.socket, event.instanceId, event.requestId, key, true /* isDegraded */ );
		this->dispatch( event );
	} else {
		bool isReconstructed;
		key.dup();
		ret = this->performDegradedRead(
			PROTO_OPCODE_DEGRADED_GET,
			event.socket,
			event.instanceId, event.requestId,
			listId, stripeId, chunkId,
			&key, header.isSealed,
			header.original, header.reconstructed, header.reconstructedCount,
			header.ongoingAtChunk,
			isReconstructed,
			0, event.timestamp
		);

		if ( ! ret ) {
			if ( checked ) {
				__ERROR__( "SlaveWorker", "handleDegradedGetRequest", "Failed to perform degraded read on (%u, %u, %u).", listId, stripeId, chunkId );
			} else if ( isReconstructed ) {
				// Check the degraded map again
				checked = true;
				goto degraded_get_check;
			}
		}
	}

	return ret;
}

bool SlaveWorker::handleDegradedUpdateRequest( MasterEvent event, char *buf, size_t size ) {
	struct DegradedReqHeader header;
	uint32_t listId, stripeId, chunkId;
	int index = -1;
	bool reconstructParity, reconstructData, inProgress = false;
	if ( ! this->protocol.parseDegradedReqHeader( header, PROTO_OPCODE_DEGRADED_UPDATE, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleDegradedUpdateRequest", "Invalid degraded UPDATE request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleDegradedUpdateRequest",
		"[UPDATE] Key: %.*s (key size = %u); Value: (update size = %u, offset = %u), reconstruction count = %u.",
		( int ) header.data.keyValueUpdate.keySize,
		header.data.keyValueUpdate.key,
		header.data.keyValueUpdate.keySize,
		header.data.keyValueUpdate.valueUpdateSize,
		header.data.keyValueUpdate.valueUpdateOffset,
		header.reconstructedCount
	);

	if ( header.reconstructedCount ) {
		stripeId = header.stripeId;
		this->getSlaves(
			header.data.keyValueUpdate.key,
			header.data.keyValueUpdate.keySize,
			listId,
			chunkId
		);
		index = this->findInRedirectedList(
			header.original, header.reconstructed, header.reconstructedCount,
			header.ongoingAtChunk, reconstructParity, reconstructData
		);

		if ( reconstructParity ) {
			inProgress = false;
			for ( uint32_t i = 0; i < header.reconstructedCount; i++ ) {
				if ( index == -1 || i != ( uint32_t ) index ) {
					bool ret;

					if ( header.isSealed ) {
						ret = SlaveWorker::map->insertForwardedChunk(
							header.original[ i * 2     ],
							header.stripeId,
							header.original[ i * 2 + 1 ],
							header.reconstructed[ i * 2     ],
							header.stripeId,
							header.reconstructed[ i * 2 + 1 ]
						);
					} else {
						ret = SlaveWorker::map->insertForwardedKey(
							header.data.keyValueUpdate.keySize,
							header.data.keyValueUpdate.key,
							header.reconstructed[ i * 2     ],
							header.reconstructed[ i * 2 + 1 ]
						);
					}
					if ( ! ret ) {
						// printf( "Reconstruction and forwarding already in progress (%u, %u, %u).\n", listId, header.stripeId, chunkId );
						inProgress = true;
						break;
					}
				}
			}

			if ( ! inProgress ) {
				__DEBUG__(
					GREEN, "SlaveWorker", "handleDegradedUpdateRequest",
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
					false // reconstructParity
				);
			}
		} else if ( ! reconstructData ) {
			// UPDATE data chunk and reconstructed parity chunks
			/*
			for ( uint32_t i = 0; i < header.reconstructedCount; i++ ) {
				printf(
					"%s(%u, %u) |-> (%u, %u)%s",
					i == 0 ? "" : ", ",
					header.original[ i * 2     ],
					header.original[ i * 2 + 1 ],
					header.reconstructed[ i * 2     ],
					header.reconstructed[ i * 2 + 1 ],
					i == header.reconstructedCount - 1 ? "\n" : ""
				);
			}
			*/
			return this->handleUpdateRequest(
				event, header.data.keyValueUpdate,
				header.original, header.reconstructed, header.reconstructedCount,
				false // reconstructParity
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
	bool ret = true;
	DegradedMap *dmap = &SlaveWorker::degradedChunkBuffer->map;
	bool isSealed, isKeyValueFound;
	Chunk *chunk;

	keyMetadata.offset = 0;

	if ( index == -1 ) {
		// Data chunk is NOT reconstructed

		// Set up key
		key.set( header.data.keyValueUpdate.keySize, header.data.keyValueUpdate.key );
		// Set up KeyValueUpdate
		keyValueUpdate.set( key.size, key.data, ( void * ) event.socket );
		keyValueUpdate.offset = header.data.keyValueUpdate.valueUpdateOffset;
		keyValueUpdate.length = header.data.keyValueUpdate.valueUpdateSize;
		goto force_degraded_read;
	}

	// Check if the chunk is already fetched
	chunk = dmap->findChunkById( listId, stripeId, chunkId );
	// Check if the key exists or is in a unsealed chunk
	isKeyValueFound = dmap->findValueByKey(
		header.data.keyValueUpdate.key,
		header.data.keyValueUpdate.keySize,
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
		// Insert into master UPDATE pending set
		if ( ! SlaveWorker::pending->insertKeyValueUpdate( PT_MASTER_UPDATE, event.instanceId, event.requestId, ( void * ) event.socket, keyValueUpdate ) ) {
			__ERROR__( "SlaveWorker", "handleDegradedUpdateRequest", "Cannot insert into master UPDATE pending map." );
		}

		char *valueUpdate = header.data.keyValueUpdate.valueUpdate;

		if ( chunk ) {
			// Send UPDATE_CHUNK request to the parity slaves
			uint32_t chunkUpdateOffset = KeyValue::getChunkUpdateOffset(
				keyMetadata.offset, // chunkOffset
				keyValueUpdate.size, // keySize
				keyValueUpdate.offset // valueUpdateOffset
			);

			SlaveWorker::degradedChunkBuffer->updateKeyValue(
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
				keyValueUpdate.data,
				metadata,
				chunkUpdateOffset,
				keyValueUpdate.length /* deltaSize */,
				keyValueUpdate.offset,
				valueUpdate,
				true /* isSealed */,
				true /* isUpdate */,
				event.timestamp,
				event.socket
			);
		} else {
			// Send UPDATE request to the parity slaves
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

			// Send UPDATE request to the parity slaves
			this->sendModifyChunkRequest(
				event.instanceId, event.requestId,
				keyValueUpdate.size,
				keyValueUpdate.data,
				metadata,
				0, /* chunkUpdateOffset */
				keyValueUpdate.length, /* deltaSize */
				keyValueUpdate.offset,
				valueUpdate,
				false /* isSealed */,
				true /* isUpdate */,
				event.timestamp,
				event.socket
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
			header.ongoingAtChunk,
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
					false // reconstructParity
				);
			} else {
				__ERROR__( "SlaveWorker", "handleDegradedUpdateRequest", "Failed to perform degraded read on (%u, %u, %u); key: %.*s.", listId, stripeId, chunkId, key.size, key.data );
			}
		}
	}

	return ret;
}

bool SlaveWorker::handleDegradedDeleteRequest( MasterEvent event, char *buf, size_t size ) {
	struct DegradedReqHeader header;
	uint32_t listId, stripeId, chunkId;
	bool reconstructParity, reconstructData;
	int index = -1;
	if ( ! this->protocol.parseDegradedReqHeader( header, PROTO_OPCODE_DEGRADED_DELETE, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleDegradedDeleteRequest", "Invalid degraded DELETE request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleDegradedDeleteRequest",
		"[DELETE] Key: %.*s (key size = %u).",
		( int ) header.data.key.keySize,
		header.data.key.key,
		header.data.key.keySize
	);

	if ( header.reconstructedCount ) {
		stripeId = header.stripeId;
		this->getSlaves(
			header.data.keyValueUpdate.key,
			header.data.keyValueUpdate.keySize,
			listId,
			chunkId
		);
		index = this->findInRedirectedList(
			header.original, header.reconstructed, header.reconstructedCount,
			header.ongoingAtChunk, reconstructParity, reconstructData
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
	DegradedMap *dmap = &SlaveWorker::degradedChunkBuffer->map;
	Chunk *chunk;

	keyMetadata.offset = 0;

	if ( index == -1 ) {
		// Data chunk is NOT reconstructed
		__ERROR__( "SlaveWorker", "handleDegradedDeleteRequest", "TODO: Handle the case when the data chunk does NOT need reconstruction." );
		key.set( header.data.keyValueUpdate.keySize, header.data.keyValueUpdate.key );
		goto force_degraded_read;
	}

	// Check if the chunk is already fetched
	chunk = dmap->findChunkById( listId, stripeId, chunkId );
	// Check if the key exists or is in a unsealed chunk
	isKeyValueFound = dmap->findValueByKey(
		header.data.key.key,
		header.data.key.keySize,
		isSealed,
		&keyValue, &key, &keyMetadata
	);
	// Set up metadata
	metadata.set( listId, stripeId, chunkId );

	if ( isKeyValueFound ) {
		key.dup( 0, 0, ( void * ) event.socket );
		if ( ! SlaveWorker::pending->insertKey( PT_MASTER_DEL, event.instanceId, event.requestId, ( void * ) event.socket, key ) ) {
			__ERROR__( "SlaveWorker", "handleDegradedDeleteRequest", "Cannot insert into master DELETE pending map." );
		}

		uint32_t timestamp;
		uint32_t deltaSize = this->buffer.size;
		char *delta = this->buffer.data;

		if ( chunk ) {
			SlaveWorker::degradedChunkBuffer->deleteKey(
				PROTO_OPCODE_DELETE, timestamp,
				key.size, key.data,
				metadata,
				true, /* isSealed */
				deltaSize, delta, chunk
			);

			// Send DELETE_CHUNK request to the parity slaves
			this->sendModifyChunkRequest(
				event.instanceId, event.requestId,
				key.size,
				key.data,
				metadata,
				keyMetadata.offset,
				deltaSize,
				0,   /* valueUpdateOffset */
				delta,
				true /* isSealed */,
				false /* isUpdate */,
				event.timestamp,
				event.socket
			);
		} else {
			uint32_t tmp = 0;
			SlaveWorker::degradedChunkBuffer->deleteKey(
				PROTO_OPCODE_DELETE, timestamp,
				key.size, key.data,
				metadata,
				false,
				tmp, 0, 0
			);

			// Send DELETE request to the parity slaves
			this->sendModifyChunkRequest(
				event.instanceId, event.requestId,
				key.size,
				key.data,
				metadata,
				// not needed for deleting a key-value pair in an unsealed chunk:
				0, 0, 0, 0,
				false /* isSealed */,
				false /* isUpdate */,
				event.timestamp,
				event.socket
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
			header.ongoingAtChunk,
			isReconstructed,
			0, event.timestamp
		);

		if ( ! ret ) {
			if ( isReconstructed ) {
				// Use normal flow
				return this->handleDeleteRequest( event, header.data.key );
			} else {
				__ERROR__( "SlaveWorker", "handleDegradedDeleteRequest", "Failed to perform degraded read on (%u, %u, %u).", listId, stripeId, chunkId );
			}
		}
	}

	return ret;
}

bool SlaveWorker::handleForwardChunkRequest( SlavePeerEvent event, char *buf, size_t size ) {
	struct ChunkDataHeader header;
	if ( ! this->protocol.parseChunkDataHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleForwardChunkRequest", "Invalid FORWARD_CHUNK request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleForwardChunkRequest",
		"[%u, %u] Chunk: (%u, %u, %u). size = %u, offset = %u.",
		event.instanceId, event.requestId,
		header.listId, header.stripeId, header.chunkId,
		header.size, header.offset
	);
	return this->handleForwardChunkRequest( header );
}

bool SlaveWorker::handleForwardChunkRequest( struct ChunkDataHeader &header ) {
	std::unordered_map<Metadata, Chunk *> *cache;
	LOCK_T *lock;
	DegradedMap *dmap = &SlaveWorker::degradedChunkBuffer->map;

	dmap->getCacheMap( cache, lock );

	LOCK( lock );
	Chunk *chunk = dmap->findChunkById(
		header.listId, header.stripeId, header.chunkId,
		0, false, false
	);
	if ( chunk ) {
		// Already exists
		char *parity = chunk->getData();
		Coding::bitwiseXOR(
			parity + header.offset,
			parity + header.offset,
			header.data,
			header.size
		);
	} else {
		chunk = SlaveWorker::chunkPool->malloc();
		chunk->status = CHUNK_STATUS_RECONSTRUCTED;
		chunk->metadata.set( header.listId, header.stripeId, header.chunkId );
		chunk->lastDelPos = 0;
		chunk->loadFromSetChunkRequest(
			header.data, header.size, header.offset,
			header.chunkId >= SlaveWorker::dataChunkCount
		);

		bool ret = dmap->insertChunk(
			header.listId, header.stripeId, header.chunkId, chunk,
			header.chunkId >= SlaveWorker::dataChunkCount,
			false, false
		);

		if ( ! ret ) {
			__ERROR__(
				"SlaveWorker", "handleForwardChunkRequest",
				"This chunk (%u, %u, %u) cannot be inserted to DegradedMap.",
				header.listId, header.stripeId, header.chunkId
			);
			SlaveWorker::chunkPool->free( chunk );
		}
	}
	UNLOCK( lock );

	return true;
}

bool SlaveWorker::handleForwardChunkResponse( SlavePeerEvent event, bool success, char *buf, size_t size ) {
	struct ChunkHeader header;
	if ( ! this->protocol.parseChunkHeader( header, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleForwardChunkResponse", "Invalid FORWARD_CHUNK response." );
		return false;
	}
	__INFO__(
		BLUE, "SlaveWorker", "handleForwardChunkResponse",
		"[%u, %u] Chunk: (%u, %u, %u) is received.",
		event.instanceId, event.requestId,
		header.listId, header.stripeId, header.chunkId
	);

	return true;
}

bool SlaveWorker::performDegradedRead(
	uint8_t opcode,
	MasterSocket *masterSocket,
	uint16_t parentInstanceId, uint32_t parentRequestId,
	uint32_t listId, uint32_t stripeId, uint32_t chunkId,
	Key *key, bool isSealed,
	uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
	uint32_t ongoingAtChunk,
	bool &isReconstructed,
	KeyValueUpdate *keyValueUpdate, uint32_t timestamp
) {
	Key mykey;
	SlavePeerEvent event;
	SlavePeerSocket *socket = 0;
	uint32_t selected = 0;

	SlaveWorker::stripeList->get( listId, this->paritySlaveSockets, this->dataSlaveSockets );

	// Determine the list of surviving nodes
	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		uint32_t originalChunkId = original[ i * 2 + 1 ];
		if ( originalChunkId < SlaveWorker::dataChunkCount )
			this->dataSlaveSockets[ originalChunkId ] = 0;
		else
			this->paritySlaveSockets[ originalChunkId - SlaveWorker::dataChunkCount ] = 0;
	}

	if ( isSealed ) {
		// Check whether the number of surviving nodes >= k
		if ( ! ( SlaveWorker::chunkCount - reconstructedCount >= SlaveWorker::dataChunkCount ) ) {
			__ERROR__( "SlaveWorker", "performDegradedRead", "The number of surviving nodes is less than k. The data cannot be recovered." );
			return false;
		}
	} else {
		if ( this->dataSlaveSockets[ chunkId ] && this->dataSlaveSockets[ chunkId ]->self ) {
			socket = this->dataSlaveSockets[ chunkId ];
		} else {
			// Check whether there are surviving parity slaves
			uint32_t numSurvivingParity = SlaveWorker::parityChunkCount;
			for ( uint32_t j = 0; j < reconstructedCount; j++ ) {
				if ( original[ j * 2 + 1 ] >= SlaveWorker::dataChunkCount ) {
					numSurvivingParity--;
				}
			}
			if ( numSurvivingParity == 0 ) {
				__ERROR__(
					"SlaveWorker", "performDegradedRead",
					"There are no surviving parity slaves. The data cannot be recovered. (numSurvivingParity = %u, reconstructedCount = %u)",
					numSurvivingParity, reconstructedCount
				);
				for ( uint32_t j = 0; j < reconstructedCount; j++ ) {
					printf(
						"%s(%u, %u) |--> (%u, %u)%s",
						j == 0 ? "" : ", ",
						original[ j * 2     ],
						original[ j * 2 + 1 ],
						reconstructed[ j * 2     ],
						reconstructed[ j * 2 + 1 ],
						j == reconstructedCount - 1 ? "\n" : ""
					);
				}
				return false;
			} else {
				for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
					SlavePeerSocket *tmp = this->paritySlaveSockets[ ( parentRequestId + i ) % SlaveWorker::parityChunkCount ]; // Add "randomness"
					if ( tmp ) {
						socket = tmp;
						break;
					}
				}
			}
		}
	}

	// Add to degraded operation pending set
	uint16_t instanceId = Slave::instanceId;
	uint32_t requestId = SlaveWorker::idGenerator->nextVal( this->workerId );
	DegradedOp op;
	op.set(
		opcode, isSealed, masterSocket,
		listId, stripeId, chunkId,
		original, reconstructed, reconstructedCount,
		ongoingAtChunk,
		timestamp, true
	);
	if ( opcode == PROTO_OPCODE_DEGRADED_UPDATE ) {
		op.data.keyValueUpdate = *keyValueUpdate;
		mykey.set( keyValueUpdate->size, keyValueUpdate->data );
	} else {
		op.data.key = *key;
		mykey.set( key->size, key->data );
	}

	if ( isSealed || ! socket->self ) {
		if ( ! SlaveWorker::pending->insertDegradedOp( PT_SLAVE_PEER_DEGRADED_OPS, instanceId, parentInstanceId, requestId, parentRequestId, 0, op ) ) {
			__ERROR__( "SlaveWorker", "performDegradedRead", "Cannot insert into slave DEGRADED_OPS pending map." );
		}
	}

	// Insert the degraded operation into degraded chunk buffer pending set
	bool needsContinue;
	if ( isSealed ) {
		needsContinue = SlaveWorker::degradedChunkBuffer->map.insertDegradedChunk( listId, stripeId, chunkId, instanceId, requestId, isReconstructed );
		// printf( "insertDegradedChunk(): (%u, %u, %u) - needsContinue: %d\n", listId, stripeId, chunkId, needsContinue );
	} else {
		Key k;
		if ( opcode == PROTO_OPCODE_DEGRADED_UPDATE )
			k.set( keyValueUpdate->size, keyValueUpdate->data );
		else
			k = *key;
		needsContinue = SlaveWorker::degradedChunkBuffer->map.insertDegradedKey( k, instanceId, requestId, isReconstructed );
		// printf( "insertDegradedKey(): (%.*s) - needsContinue: %d\n", k.size, k.data, needsContinue );
	}

	if ( isSealed ) {
		if ( ! needsContinue ) {
			if ( isReconstructed ) {
				// The chunk is already reconstructed
				return false;
			} else {
				// Reconstruction in progress
				return true;
			}
		}

		// Send GET_CHUNK requests to surviving nodes
		Metadata metadata;
		metadata.set( listId, stripeId, 0 );
		selected = 0;

		// printf( "Reconstructing using: " );
		for ( uint32_t i = 0; i < SlaveWorker::chunkCount; i++ ) {
			if ( selected >= SlaveWorker::dataChunkCount )
				break;

			socket = ( i < SlaveWorker::dataChunkCount ) ?
			         ( this->dataSlaveSockets[ i ] ) :
			         ( this->paritySlaveSockets[ i - SlaveWorker::dataChunkCount ] );

			if ( ! socket )
				continue;

			// Add to pending GET_CHUNK request set
			ChunkRequest chunkRequest;
			chunkRequest.set( listId, stripeId, i, socket, 0, true );
			// printf( "(%u, %u, %u) ", listId, stripeId, i );
			if ( socket->self ) {
				chunkRequest.chunk = SlaveWorker::map->findChunkById( listId, stripeId, i );
				// Check whether the chunk is sealed or not
				if ( ! chunkRequest.chunk ) {
					chunkRequest.chunk = Coding::zeros;
				} else {
					MixedChunkBuffer *chunkBuffer = SlaveWorker::chunkBuffer->at( listId );
					int chunkBufferIndex = chunkBuffer->lockChunk( chunkRequest.chunk, true );
					bool isSealed = ( chunkBufferIndex == -1 );
					if ( ! isSealed )
						chunkRequest.chunk = Coding::zeros;
					chunkBuffer->unlock( chunkBufferIndex );
				}

				if ( ! SlaveWorker::pending->insertChunkRequest( PT_SLAVE_PEER_GET_CHUNK, instanceId, parentInstanceId, requestId, parentRequestId, socket, chunkRequest ) ) {
					__ERROR__( "SlaveWorker", "performDegradedRead", "Cannot insert into slave CHUNK_REQUEST pending map." );
				}
			} else if ( socket->ready() ) {
				chunkRequest.chunk = 0;

				if ( ! SlaveWorker::pending->insertChunkRequest( PT_SLAVE_PEER_GET_CHUNK, instanceId, parentInstanceId, requestId, parentRequestId, socket, chunkRequest ) ) {
					__ERROR__( "SlaveWorker", "performDegradedRead", "Cannot insert into slave CHUNK_REQUEST pending map." );
				}
			} else {
				continue;
			}
			selected++;
		}
		// printf( "\n" );

		selected = 0;
		for ( uint32_t i = 0; i < SlaveWorker::chunkCount; i++ ) {
			if ( selected >= SlaveWorker::dataChunkCount )
				break;

			socket = ( i < SlaveWorker::dataChunkCount ) ?
			         ( this->dataSlaveSockets[ i ] ) :
			         ( this->paritySlaveSockets[ i - SlaveWorker::dataChunkCount ] );

			if ( ! socket )
				continue;

			if ( socket->self ) {
				selected++;
			} else if ( socket->ready() ) {
				metadata.chunkId = i;
				event.reqGetChunk( socket, instanceId, requestId, metadata );
				SlaveWorker::eventQueue->insert( event );
				selected++;
			}
		}

		return ( selected >= SlaveWorker::dataChunkCount );
	} else {
		////////// Key-value pairs in unsealed chunks //////////
		if ( socket->self ) {
			// Get the requested key-value pairs from local key-value store
			KeyValue keyValue;
			MasterEvent masterEvent;
			SlavePeerEvent slavePeerEvent;
			char *keyStr = 0, *valueStr = 0;
			uint8_t keySize = 0;
			uint32_t valueSize = 0;

			bool success = SlaveWorker::map->findValueByKey( mykey.data, mykey.size, &keyValue, &mykey );
			if ( ! success )
				success = SlaveWorker::chunkBuffer->at( listId )->findValueByKey( mykey.data, mykey.size, &keyValue, &mykey );

			if ( success ) {
				keyValue.deserialize( keyStr, keySize, valueStr, valueSize );
				keyValue.dup( keyStr, keySize, valueStr, valueSize );
			} else {
				__ERROR__( "SlaveWorker", "performDegradedRead", "findValueByKey() failed (list ID: %u, key: %.*s).", listId, mykey.size, mykey.data );
			}

			// Insert into degraded chunk buffer if this is not the original data server (i.e., the data server fails)
			if ( ! this->dataSlaveSockets[ chunkId ]->self ) {
				if ( success && opcode != PROTO_OPCODE_DEGRADED_DELETE ) {
					// Insert into degradedChunkBuffer
					Metadata metadata;
					metadata.set( listId, stripeId, chunkId );
					if ( ! SlaveWorker::degradedChunkBuffer->map.insertValue( keyValue, metadata ) ) {
						__ERROR__( "SlaveWorker", "performDegradedRead", "Cannot insert into degraded chunk buffer values map. (Key: %.*s)", keySize, keyStr );
						keyValue.free();
						success = false;
					}
				}
			}

			// Forward the key-value pair to the reconstructed servers
			if ( success ) {
				// Need to send the key-value pair to reconstructed servers even if this is a DELETE request because of the need of creating backup

				if ( ! SlaveWorker::pending->insertDegradedOp( PT_SLAVE_PEER_DEGRADED_OPS, instanceId, parentInstanceId, requestId, parentRequestId, 0, op ) ) {
					__ERROR__( "SlaveWorker", "performDegradedRead", "Cannot insert into slave DEGRADED_OPS pending map." );
				}

				for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
					if ( ! SlaveWorker::pending->insertKeyValue( PT_SLAVE_PEER_SET, instanceId, parentInstanceId, requestId, parentRequestId, socket, keyValue ) ) {
						__ERROR__( "SlaveWorker", "performDegradedRead", "Cannot insert into slave SET pending map." );
					}
				}

				for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
					SlavePeerSocket *s = SlaveWorker::stripeList->get(
						reconstructed[ i * 2     ],
						reconstructed[ i * 2 + 1 ]
					);
					if ( opcode == PROTO_OPCODE_DEGRADED_UPDATE ) {
						if ( s->self ) {
							struct ForwardKeyHeader forwardKeyHeader;
							SlavePeerEvent emptyEvent;

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
							slavePeerEvent.reqForwardKey(
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
							SlavePeerEvent emptyEvent;

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
							slavePeerEvent.reqForwardKey(
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
						this->dispatch( slavePeerEvent );
					}
				}

				return true;
			}

			////////////////////////////////////////////////////////////////////
			switch( opcode ) {
				case PROTO_OPCODE_DEGRADED_GET:
					if ( success ) {
						// masterEvent.resGet( masterSocket, parentInstanceId, parentRequestId, keyValue, true );
					} else {
						// Return failure to master
						masterEvent.resGet( masterSocket, parentInstanceId, parentRequestId, mykey, true );
					}
					this->dispatch( masterEvent );
					op.data.key.free();
					break;
				case PROTO_OPCODE_DEGRADED_UPDATE:
					if ( success ) {
						/*
						if ( ! needsSend ) {
							Metadata metadata;
							metadata.set( listId, stripeId, chunkId );

							uint32_t dataUpdateOffset = KeyValue::getChunkUpdateOffset(
								0,                     // chunkOffset
								keyValueUpdate->size,  // keySize
								keyValueUpdate->offset // valueUpdateOffset
							);

							char *valueUpdate = ( char * ) keyValueUpdate->ptr;

							// Compute data delta
							Coding::bitwiseXOR(
								valueUpdate,
								keyValue.data + dataUpdateOffset, // original data
								valueUpdate,                      // new data
								keyValueUpdate->length
							);
							// Perform actual data update
							Coding::bitwiseXOR(
								keyValue.data + dataUpdateOffset,
								keyValue.data + dataUpdateOffset, // original data
								valueUpdate,                      // new data
								keyValueUpdate->length
							);

							// Send UPDATE request to the parity slaves
							this->sendModifyChunkRequest(
								event.instanceId, event.requestId,
								keyValueUpdate->size,
								keyValueUpdate->data,
								metadata,
								0, // chunkUpdateOffset
								keyValueUpdate->length, // deltaSize
								keyValueUpdate->offset,
								valueUpdate,
								false, // isSealed
								true,  // isUpdate
								0,     // timestamp
								0,     // masterSocket
								original, reconstructed, reconstructedCount
							);
						}
						*/
					} else {
						masterEvent.resUpdate(
							masterSocket, parentInstanceId, parentRequestId, mykey,
							keyValueUpdate->offset,
							keyValueUpdate->length,
							false, false, true
						);
						this->dispatch( masterEvent );
					}
					op.data.keyValueUpdate.free();
					delete[] ( ( char * ) op.data.keyValueUpdate.ptr );
					break;
				case PROTO_OPCODE_DEGRADED_DELETE:
					if ( success ) {
						/*
						Metadata metadata;
						KeyMetadata keyMetadata;
						uint32_t timestamp;
						metadata.set( listId, stripeId, chunkId );
						keyMetadata.set( listId, stripeId, chunkId );

						// SlaveWorker::map->insertOpMetadata(
						// 	PROTO_OPCODE_DELETE, timestamp,
						// 	mykey, keyMetadata
						// );

						uint32_t tmp = 0;
						SlaveWorker::degradedChunkBuffer->deleteKey(
							PROTO_OPCODE_DELETE, timestamp,
							mykey.size, mykey.data,
							metadata,
							true, // isSealed
							tmp, 0, 0
						);

						this->sendModifyChunkRequest(
							parentInstanceId, parentRequestId,
							mykey.size, mykey.data,
							metadata,
							// not needed for deleting a key-value pair in an unsealed chunk:
							0, 0, 0, 0,
							false, // isSealed
							false, // isUpdate
							0,     // timestamp
							0,     // masterSocket
							original, reconstructed, reconstructedCount
						);
						*/
					} else {
						masterEvent.resDelete(
							masterSocket,
							parentInstanceId, parentRequestId,
							mykey,
							false, // needsFree
							true   // isDegraded
						);
						this->dispatch( masterEvent );
					}
					op.data.key.free();
					break;
			}

			return success;
		} else if ( needsContinue ) {
			// Send GET request to surviving parity slave
			if ( ! SlaveWorker::pending->insertKey( PT_SLAVE_PEER_GET, instanceId, parentInstanceId, requestId, parentRequestId, socket, op.data.key ) ) {
				__ERROR__( "SlaveWorker", "performDegradedRead", "Cannot insert into slave GET pending map." );
			}
			event.reqGet( socket, instanceId, requestId, listId, chunkId, op.data.key );
			this->dispatch( event );
		} else if ( isReconstructed ) {
			return false;
		}
		return true;
	}
}

bool SlaveWorker::sendModifyChunkRequest(
	uint16_t parentInstanceId, uint32_t parentRequestId,
	uint8_t keySize, char *keyStr,
	Metadata &metadata, uint32_t offset,
	uint32_t deltaSize, uint32_t valueUpdateOffset, char *delta,
	bool isSealed, bool isUpdate,
	uint32_t timestamp, MasterSocket *masterSocket,
	uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
	bool reconstructParity,
	Chunk **chunks, bool endOfDegradedOp
) {
	Key key;
	KeyValueUpdate keyValueUpdate;
	uint16_t instanceId = Slave::instanceId;
	uint32_t requestId = SlaveWorker::idGenerator->nextVal( this->workerId );
	bool isDegraded = original && reconstructed && reconstructedCount;

	if ( SlaveWorker::disableSeal ) {
		isSealed = false;
	}

	key.set( keySize, keyStr );
	keyValueUpdate.set( keySize, keyStr );
	keyValueUpdate.offset = valueUpdateOffset;
	keyValueUpdate.length = deltaSize;
	this->getSlaves( metadata.listId );

	if ( isDegraded ) {
		for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
			if ( original[ i * 2 + 1 ] >= SlaveWorker::dataChunkCount ) {
				if ( reconstructParity ) {
					this->forward.chunks[ original[ i * 2 + 1 ] ] = chunks[ original[ i * 2 + 1 ] ];
					this->paritySlaveSockets[ original[ i * 2 + 1 ] - SlaveWorker::dataChunkCount ] = 0;
				} else {
					this->paritySlaveSockets[ original[ i * 2 + 1 ] - SlaveWorker::dataChunkCount ] = SlaveWorker::stripeList->get(
						reconstructed[ i * 2     ],
						reconstructed[ i * 2 + 1 ]
					);
				}
			}
		}
	}

	if ( isSealed ) {
		// Send UPDATE_CHUNK / DELETE_CHUNK requests to parity slaves if the chunk is sealed
		ChunkUpdate chunkUpdate;
		chunkUpdate.set(
			metadata.listId, metadata.stripeId, metadata.chunkId,
			offset, deltaSize
		);
		chunkUpdate.setKeyValueUpdate( key.size, key.data, offset );

		for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
			if ( ! this->paritySlaveSockets[ i ] || this->paritySlaveSockets[ i ]->self ) {
				continue;
			}

			chunkUpdate.chunkId = SlaveWorker::dataChunkCount + i; // updatingChunkId
			chunkUpdate.ptr = ( void * ) this->paritySlaveSockets[ i ];
			if ( ! SlaveWorker::pending->insertChunkUpdate(
				isUpdate ? PT_SLAVE_PEER_UPDATE_CHUNK : PT_SLAVE_PEER_DEL_CHUNK,
				instanceId, parentInstanceId, requestId, parentRequestId,
				( void * ) this->paritySlaveSockets[ i ],
				chunkUpdate
			) ) {
				__ERROR__( "SlaveWorker", "sendModifyChunkRequest", "Cannot insert into slave %s pending map.", isUpdate ? "UPDATE_CHUNK" : "DELETE_CHUNK" );
			}
		}

		// Start sending packets only after all the insertion to the slave peer DELETE_CHUNK pending set is completed
		for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
			if ( ! this->paritySlaveSockets[ i ] ) {
				/////////////////////////////////////////////////////
				// Update the parity chunks that will be forwarded //
				/////////////////////////////////////////////////////
				// Update in the chunks pointer
				if ( ! isDegraded ) {
					__ERROR__( "SlaveWorker", "sendModifyChunkRequest", "Invalid degraded operation." );
					continue;
				}

				// Prepare the data delta
				this->forward.dataChunk->clear();
				this->forward.dataChunk->setSize( offset + deltaSize );
				memcpy( this->forward.dataChunk->getData() + offset, delta, deltaSize );

				// Prepare the stripe
				for ( uint32_t j = 0; j < SlaveWorker::dataChunkCount; j++ ) {
					this->forward.chunks[ j ] = Coding::zeros;
				}
				this->forward.chunks[ metadata.chunkId ] = this->forward.dataChunk;

				this->forward.parityChunk->clear();

				// Compute parity delta
				SlaveWorker::coding->encode(
					this->forward.chunks, this->forward.parityChunk,
					i + 1, // Parity chunk index
					offset + metadata.chunkId * Chunk::capacity,
					offset + metadata.chunkId * Chunk::capacity + deltaSize
				);

				// Apply the parity delta
				/*
				if ( ! this->forward.chunks[ i + SlaveWorker::dataChunkCount ] ) {
					for ( uint32_t j = 0; j < SlaveWorker::chunkCount; j++ ) {
						printf( "%p ", this->forward.chunks[ j ] );
					}
					printf( "(reconstructParity = %s)\n", reconstructParity ? "true" : "false" );

					for ( uint32_t j = 0; j < reconstructedCount; j++ ) {
						printf(
							"%s(%u, %u) |-> (%u, %u)%s",
							j == 0 ? "" : ", ",
							original[ i * 2     ],
							original[ i * 2 + 1 ],
							reconstructed[ i * 2     ],
							reconstructed[ i * 2 + 1 ],
							j == reconstructedCount - 1 ? "\n" : ""
						);
					}
				}
				*/
				assert( this->forward.chunks[ i + SlaveWorker::dataChunkCount ]->getData() );
				assert( this->forward.chunks[ i + SlaveWorker::dataChunkCount ] );
				assert( this->forward.chunks[ i + SlaveWorker::dataChunkCount ]->getData() );
				assert( this->forward.parityChunk );
				assert( this->forward.parityChunk->getData() );

				char *parity = this->forward.chunks[ i + SlaveWorker::dataChunkCount ]->getData();
				Coding::bitwiseXOR(
					parity,
					parity,
					this->forward.parityChunk->getData(),
					Chunk::capacity
				);
				/////////////////////////////////////////////////////
			} else if ( this->paritySlaveSockets[ i ]->self ) {
				SlaveWorker::chunkBuffer->at( metadata.listId )->update(
					metadata.stripeId, metadata.chunkId,
					offset, deltaSize, delta,
					this->chunks, this->dataChunk, this->parityChunk,
					true /* isDelete */
				);
			} else {
				// Prepare DELETE_CHUNK request
				size_t size;
				Packet *packet = SlaveWorker::packetPool->malloc();
				packet->setReferenceCount( 1 );

				// backup data delta, insert a pending record for each parity slave
				/* Seems that we don't need the data delta...
				if ( masterSocket != 0 ) {
					Timestamp ts( timestamp );
					Value value;
					value.set( deltaSize, delta );
					if ( isUpdate )
						masterSocket->backup.insertDataUpdate( ts, key, value, metadata, isSealed, valueUpdateOffset, offset, requestId, this->paritySlaveSockets[ i ]->instanceId, this->paritySlaveSockets[ i ] );
					else
						masterSocket->backup.insertDataDelete( ts, key, value, metadata, isSealed, valueUpdateOffset, offset, requestId, this->paritySlaveSockets[ i ]->instanceId, this->paritySlaveSockets[ i ] );
				}
				*/

				if ( isUpdate ) {
					this->protocol.reqUpdateChunk(
						size,
						parentInstanceId, 				// master Id
						requestId, 						// slave request Id
						metadata.listId,
						metadata.stripeId,
						metadata.chunkId,
						offset,
						deltaSize,                       // length
						SlaveWorker::dataChunkCount + i, // updatingChunkId
						delta,
						packet->data,
						timestamp
					);
				} else {
					this->protocol.reqDeleteChunk(
						size,
						parentInstanceId, 				// master Id
						requestId,						// slave request Id
						metadata.listId,
						metadata.stripeId,
						metadata.chunkId,
						offset,
						deltaSize,                       // length
						SlaveWorker::dataChunkCount + i, // updatingChunkId
						delta,
						packet->data,
						timestamp
					);
				}
				packet->size = ( uint32_t ) size;

				// Insert into event queue
				SlavePeerEvent slavePeerEvent;
				slavePeerEvent.send( this->paritySlaveSockets[ i ], packet );

#ifdef SLAVE_WORKER_SEND_REPLICAS_PARALLEL
				if ( i == SlaveWorker::parityChunkCount - 1 )
					this->dispatch( slavePeerEvent );
				else
					SlaveWorker::eventQueue->prioritizedInsert( slavePeerEvent );
#else
				this->dispatch( slavePeerEvent );
#endif
			}
		}

		if ( isDegraded && endOfDegradedOp ) {
			////////////////////////////////////////
			// Forward the modified parity chunks //
			////////////////////////////////////////
			SlavePeerEvent event;
			requestId = SlaveWorker::idGenerator->nextVal( this->workerId );

			for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
				if ( original[ i * 2 + 1 ] >= SlaveWorker::dataChunkCount ) {
					SlavePeerSocket *s = SlaveWorker::stripeList->get( reconstructed[ i * 2 ], reconstructed[ i * 2 + 1 ] );

					if ( s->self ) {
						struct ChunkDataHeader chunkDataHeader = {
							.listId = metadata.listId,
							.stripeId = metadata.stripeId,
							.chunkId = original[ i * 2 + 1 ],
							.size = chunks[ original[ i * 2 + 1 ] ]->getSize(),
							.offset = 0,
							.data = chunks[ original[ i * 2 + 1 ] ]->getData()
						};
						this->handleForwardChunkRequest( chunkDataHeader );
					} else {
						metadata.chunkId = original[ i * 2 + 1 ];
						event.reqForwardChunk(
							SlaveWorker::stripeList->get( reconstructed[ i * 2 ], reconstructed[ i * 2 + 1 ] ),
							instanceId, requestId,
							metadata, chunks[ original[ i * 2 + 1 ] ], false
						);

						// printf(
						// 	"Forwarding chunk (%u, %u, %u) to #%u...\n",
						// 	metadata.listId, metadata.stripeId,
						// 	original[ i * 2 + 1 ],
						// 	reconstructed[ i * 2 + 1 ]
						// );

						this->dispatch( event );
					}
				} else {
					// This case never happens
					// this->dataSlaveSockets[ original[ i * 2 + 1 ] ] = 0;
				}
			}
		}
	} else {
		// Send UPDATE / DELETE request if the chunk is not yet sealed

		// Check whether any of the parity slaves are self-socket
		uint32_t self = 0;
		uint32_t parityServerCount = SlaveWorker::parityChunkCount;
		for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
			if ( ! this->paritySlaveSockets[ i ] ) {
				parityServerCount--;
			} else if ( this->paritySlaveSockets[ i ]->self ) {
				parityServerCount--;
				self = i + 1;
				break;
			}
		}

		// Prepare UPDATE / DELETE request
		size_t size = 0;
		Packet *packet = 0;

		if ( parityServerCount ) {
			packet = SlaveWorker::packetPool->malloc();
			packet->setReferenceCount( parityServerCount );
			// packet->setReferenceCount( self == 0 ? SlaveWorker::parityChunkCount : SlaveWorker::parityChunkCount - 1 );
			if ( isUpdate ) {
				this->protocol.reqUpdate(
					size,
					parentInstanceId, /* master Id */
					requestId, /* slave request Id */
					metadata.listId,
					metadata.stripeId,
					metadata.chunkId,
					keyStr,
					keySize,
					delta /* valueUpdate */,
					valueUpdateOffset,
					deltaSize /* valueUpdateSize */,
					offset, // Chunk update offset
					packet->data,
					timestamp
				);
			} else {
				this->protocol.reqDelete(
					size,
					parentInstanceId, /* master Id */
					requestId, /* slave request Id */
					metadata.listId,
					metadata.stripeId,
					metadata.chunkId,
					keyStr,
					keySize,
					packet->data,
					timestamp
				);
			}
			packet->size = ( uint32_t ) size;
		}

		for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
			if ( ! this->paritySlaveSockets[ i ] || this->paritySlaveSockets[ i ]->self )
				continue;

			// backup data delta, insert a pending record for each parity slave
			/* Seems that we don't need the data delta...
			if ( masterSocket != 0 ) {
				Timestamp ts ( timestamp );
				Value value;
				value.set( deltaSize, delta );
				if ( isUpdate )
					masterSocket->backup.insertDataUpdate( ts, key, value, metadata, isSealed, valueUpdateOffset, offset, requestId, this->paritySlaveSockets[ i ]->instanceId, this->paritySlaveSockets[ i ] );
				else
					masterSocket->backup.insertDataDelete( ts, key, value, metadata, isSealed, valueUpdateOffset, offset, requestId, this->paritySlaveSockets[ i ]->instanceId, this->paritySlaveSockets[ i ] );
			}
			*/

			if ( isUpdate ) {
				if ( ! SlaveWorker::pending->insertKeyValueUpdate(
					PT_SLAVE_PEER_UPDATE, instanceId, parentInstanceId, requestId, parentRequestId,
					( void * ) this->paritySlaveSockets[ i ],
					keyValueUpdate
				) ) {
					__ERROR__( "SlaveWorker", "handleUpdateRequest", "Cannot insert into slave UPDATE pending map." );
				}
			} else {
				if ( ! SlaveWorker::pending->insertKey(
					PT_SLAVE_PEER_DEL, instanceId, parentInstanceId, requestId, parentRequestId,
					( void * ) this->paritySlaveSockets[ i ],
					key
				) ) {
					__ERROR__( "SlaveWorker", "handleDeleteRequest", "Cannot insert into slave DELETE pending map." );
				}
			}
		}

		// Start sending packets only after all the insertion to the slave peer DELETE pending set is completed
		for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
			if ( ! this->paritySlaveSockets[ i ] ) {
				continue;
			} else if ( this->paritySlaveSockets[ i ]->self ) {
				bool r = false;
				if ( isUpdate ) {
					r = SlaveWorker::chunkBuffer->at( metadata.listId )->updateKeyValue(
						keyStr, keySize,
						valueUpdateOffset, deltaSize, delta
					);
					if ( ! r ) {
						KeyValue keyValue;
						bool found;
						found = map->findValueByKey( keyStr, keySize, &keyValue, 0 );
						Metadata myMetadata = metadata;
						myMetadata.chunkId = i + SlaveWorker::dataChunkCount;

						r = SlaveWorker::degradedChunkBuffer->updateKeyValue(
							keySize, keyStr,
							deltaSize, valueUpdateOffset,
							0,       // chunkUpdateOffset
							delta,
							0,       // chunk
							false,   // isSealed
							found ? &keyValue : 0,
							found ? &myMetadata : 0
						);
					}
				} else {
					__ERROR__( "SlaveWorker", "sendModifyChunkRequest", "TODO: Handle DELETE request on self slave socket." );
				}

				__DEBUG__(
					YELLOW, "SlaveWorker", "sendModifyChunkRequest",
					"SELF SOCKET (updated? %s), my chunk ID = %u.",
					r ? "true" : "false",
					SlaveWorker::chunkBuffer->at( metadata.listId )->getChunkId()
				);
			} else {
				// Insert into event queue
				SlavePeerEvent slavePeerEvent;
				slavePeerEvent.send( this->paritySlaveSockets[ i ], packet );

#ifdef SLAVE_WORKER_SEND_REPLICAS_PARALLEL
				if ( i == SlaveWorker::parityChunkCount - 1 )
					this->dispatch( slavePeerEvent );
				else
					SlaveWorker::eventQueue->prioritizedInsert( slavePeerEvent );
#else
				this->dispatch( slavePeerEvent );
#endif
				// printf( "Sending UPDATE request for key: %.*s...\n", keySize, keyStr );
			}
		}

		/*
		if ( isDegraded && endOfDegradedOp ) {
			////////////////////////////////////////
			// Forward the modified parity chunks //
			////////////////////////////////////////
			SlavePeerEvent event;
			requestId = SlaveWorker::idGenerator->nextVal( this->workerId );

			for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
				if ( original[ i * 2 + 1 ] >= SlaveWorker::dataChunkCount ) {
					metadata.chunkId = original[ i * 2 + 1 ];
					event.reqForwardKey(
						SlaveWorker::stripeList->get( reconstructed[ i * 2 ], reconstructed[ i * 2 + 1 ] ),
						instanceId, requestId,
						metadata.listId, metadata.chunkId,

					);

					this->dispatch( event );

					printf(
						"Forwarding chunk (%u, %u, %u) to #%u...\n",
						metadata.listId, metadata.stripeId,
						original[ i * 2 + 1 ],
						reconstructed[ i * 2 + 1 ]
					);
				} else {
					// This case never happens
					// this->dataSlaveSockets[ original[ i * 2 + 1 ] ] = 0;
				}
			}
		}
		*/

		if ( ! self ) {
			self--; // Get the self parity server index
			if ( isUpdate ) {
				bool ret = SlaveWorker::chunkBuffer->at( metadata.listId )->updateKeyValue(
					keyStr, keySize,
					valueUpdateOffset, deltaSize, delta
				);
				if ( ! ret ) {
					// Use the chunkUpdateOffset
					SlaveWorker::chunkBuffer->at( metadata.listId )->update(
						metadata.stripeId, metadata.chunkId,
						offset, deltaSize, delta,
						this->chunks, this->dataChunk, this->parityChunk
					);
					ret = true;
				}
			} else {
				SlaveWorker::chunkBuffer->at( metadata.listId )->deleteKey( keyStr, keySize );
			}
		}
	}
	return true;
}
