#include "worker.hh"
#include "../main/slave.hh"

int SlaveWorker::findInRedirectedList( uint32_t *reconstructed, uint32_t reconstructedCount ) {
	std::vector<StripeListIndex> &lists = Slave::getInstance()->stripeListIndex;
	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		for ( size_t j = 0, size = lists.size(); j < size; j++ ) {
			if ( reconstructed[ i * 2     ] == lists[ j ].listId &&
			     reconstructed[ i * 2 + 1 ] == lists[ j ].chunkId ) {
				return ( int ) i;
			}
		}
	}
	return -1;
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
		__DEBUG__(
			BLUE, "SlaveWorker", "handleGetRequest",
			"[DEGRADED_RELEASE] (%u, %u, %u) (remaining = %lu).",
			header.listId, header.stripeId, header.chunkId
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
			__ERROR__( "SlaveWorker", "performDegradedRead", "Cannot insert into slave CHUNK_REQUEST pending map." );
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
		__ERROR__( "SlaveWorker", "handleDegradedRequest", "Invalid degraded GET request." );
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
	if ( header.reconstructedCount ) {
		index = this->findInRedirectedList( header.reconstructed, header.reconstructedCount );
		if ( ( index == -1 ) ||
		     ( header.original[ index * 2 + 1 ] >= SlaveWorker::dataChunkCount ) ) {
			// No need to perform degraded read if only the parity slaves are redirected
			return this->handleGetRequest( event, header.data.key );
		}
	} else {
		// Use normal flow
		return this->handleGetRequest( event, header.data.key );
	}

	uint32_t listId = header.original[ index * 2 ],
	         stripeId = header.stripeId,
	         chunkId = header.original[ index * 2 + 1 ];
	Key key;
	KeyValue keyValue;
	KeyMetadata keyMetadata;
	bool ret = true;
	DegradedMap *dmap = &SlaveWorker::degradedChunkBuffer->map;
	 // Check if the chunk is already fetched
	Chunk *chunk = dmap->findChunkById(
		listId, stripeId, chunkId
	);
	// Check if the key exists or is in a unsealed chunk
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
		key.dup();
		ret = this->performDegradedRead(
			PROTO_OPCODE_DEGRADED_GET,
			event.socket,
			event.instanceId, event.requestId,
			listId, stripeId, chunkId,
			&key, header.isSealed,
			header.original, header.reconstructed, header.reconstructedCount
		);

		if ( ! ret ) {
			__ERROR__( "SlaveWorker", "handleDegradedGetRequest", "Failed to perform degraded read on (%u, %u).", listId, stripeId );
		}
	}

	return ret;
}

bool SlaveWorker::handleDegradedUpdateRequest( MasterEvent event, char *buf, size_t size ) {
	struct DegradedReqHeader header;
	uint32_t listId, stripeId, chunkId;
	int index = -1;
	if ( ! this->protocol.parseDegradedReqHeader( header, PROTO_OPCODE_DEGRADED_UPDATE, buf, size ) ) {
		__ERROR__( "SlaveWorker", "handleDegradedUpdateRequest", "Invalid degraded UPDATE request." );
		return false;
	}
	__DEBUG__(
		BLUE, "SlaveWorker", "handleDegradedRequest",
		"[UPDATE] Key: %.*s (key size = %u); Value: (update size = %u, offset = %u).",
		( int ) header.data.keyValueUpdate.keySize,
		header.data.keyValueUpdate.key,
		header.data.keyValueUpdate.keySize,
		header.data.keyValueUpdate.valueUpdateSize,
		header.data.keyValueUpdate.valueUpdateOffset
	);

	if ( header.reconstructedCount ) {
		stripeId = header.stripeId;
		this->getSlaves(
			header.data.keyValueUpdate.key,
			header.data.keyValueUpdate.keySize,
			listId,
			chunkId
		);
		index = this->findInRedirectedList( header.reconstructed, header.reconstructedCount );
	} else {
		// Use normal flow
		return this->handleUpdateRequest( event, header.data.keyValueUpdate );
	}

	if ( index == -1 ) {
		// Data chunk is NOT reconstructed
		__ERROR__( "SlaveWorker", "handleDegradedUpdateRequest", "TODO: Handle the case when the data chunk does NOT need reconstruction." );
	}

	Key key;
	KeyValue keyValue;
	KeyValueUpdate keyValueUpdate;
	KeyMetadata keyMetadata;
	Metadata metadata;
	bool ret = true;
	DegradedMap *dmap = &SlaveWorker::degradedChunkBuffer->map;

	keyMetadata.offset = 0;

	// Check if the chunk is already fetched
	Chunk *chunk = dmap->findChunkById( listId, stripeId, chunkId );
	// Check if the key exists or is in a unsealed chunk
	bool isSealed;
	bool isKeyValueFound = dmap->findValueByKey(
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
		// Insert into master UPDATE pending set
		if ( ! SlaveWorker::pending->insertKeyValueUpdate( PT_MASTER_UPDATE, event.instanceId, event.requestId, ( void * ) event.socket, keyValueUpdate ) ) {
			__ERROR__( "SlaveWorker", "handleDegradedRequest", "Cannot insert into master UPDATE pending map." );
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
				true /* isUpdate */
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
				true /* isUpdate */
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
		key.dup();
		keyValueUpdate.dup( 0, 0, ( void * ) event.socket );

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
			&keyValueUpdate
		);

		if ( ! ret ) {
			__ERROR__( "SlaveWorker", "handleDegradedUpdateRequest", "Failed to perform degraded read on (%u, %u).", listId, stripeId );
		}
	}

	return ret;
}

bool SlaveWorker::handleDegradedDeleteRequest( MasterEvent event, char *buf, size_t size ) {
	struct DegradedReqHeader header;
	uint32_t listId, stripeId, chunkId;
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
		index = this->findInRedirectedList( header.reconstructed, header.reconstructedCount );
	} else {
		// Use normal flow
		return this->handleDeleteRequest( event, header.data.key );
	}

	if ( index == -1 ) {
		// Data chunk is NOT reconstructed
		__ERROR__( "SlaveWorker", "handleDegradedDeleteRequest", "TODO: Handle the case when the data chunk does NOT need reconstruction." );
	}

	Key key;
	KeyValue keyValue;
	KeyMetadata keyMetadata;
	Metadata metadata;
	bool ret = true;
	DegradedMap *dmap = &SlaveWorker::degradedChunkBuffer->map;

	keyMetadata.offset = 0;

	// Check if the chunk is already fetched
	Chunk *chunk = dmap->findChunkById( listId, stripeId, chunkId );
	// Check if the key exists or is in a unsealed chunk
	bool isSealed;
	bool isKeyValueFound = dmap->findValueByKey(
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
				false /* isUpdate */
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
				false /* isUpdate */
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
		key.dup();
		ret = this->performDegradedRead(
			PROTO_OPCODE_DEGRADED_DELETE,
			event.socket,
			event.instanceId, event.requestId,
			listId, stripeId, chunkId,
			&key, header.isSealed,
			header.original, header.reconstructed, header.reconstructedCount
		);

		if ( ! ret ) {
			__ERROR__( "SlaveWorker", "handleDegradedDeleteRequest", "Failed to perform degraded read on (%u, %u).", listId, stripeId );
		}
	}

	return ret;
}

bool SlaveWorker::performDegradedRead(
	uint8_t opcode,
	MasterSocket *masterSocket,
	uint16_t parentInstanceId, uint32_t parentRequestId,
	uint32_t listId, uint32_t stripeId, uint32_t lostChunkId,
	Key *key, bool isSealed,
	uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
	KeyValueUpdate *keyValueUpdate
) {
	Key mykey;
	SlavePeerEvent event;
	SlavePeerSocket *socket = 0;
	uint32_t selected = 0;

	SlaveWorker::stripeList->get( listId, this->paritySlaveSockets, this->dataSlaveSockets );

	if ( ! isSealed ) {
		// Check whether there are surviving parity slaves
		for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
			socket = this->paritySlaveSockets[ ( parentRequestId + i ) % SlaveWorker::parityChunkCount ];
			if ( socket->ready() ) break;
		}
		if ( ! socket ) {
			__ERROR__( "SlaveWorker", "performDegradedRead", "There are no surviving parity slaves. The data cannot be recovered." );
			return false;
		}
	} else {
		// Check whether the number of surviving nodes >= k
		for ( uint32_t i = 0; i < SlaveWorker::chunkCount; i++ ) {
			// Never get from the overloaded slave (even if it is still "ready")
			if ( i == lostChunkId )
				continue;
			socket = ( i < SlaveWorker::dataChunkCount ) ?
			         ( this->dataSlaveSockets[ i ] ) :
			         ( this->paritySlaveSockets[ i - SlaveWorker::dataChunkCount ] );
			if ( socket->ready() ) selected++;
		}
		if ( selected < SlaveWorker::dataChunkCount ) {
			__ERROR__( "SlaveWorker", "performDegradedRead", "The number of surviving nodes is less than k. The data cannot be recovered." );
			return false;
		}
	}

	// Add to degraded operation pending set
	uint16_t instanceId = Slave::instanceId;
	uint32_t requestId = SlaveWorker::idGenerator->nextVal( this->workerId );
	DegradedOp op;
	op.set( listId, stripeId, lostChunkId, isSealed, opcode, masterSocket );
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
		needsContinue = SlaveWorker::degradedChunkBuffer->map.insertDegradedChunk( listId, stripeId, lostChunkId, instanceId, requestId );
		// printf( "insertDegradedChunk(): (%u, %u, %u) - needsContinue: %d\n", listId, stripeId, lostChunkId, needsContinue );
	} else {
		Key k;
		if ( opcode == PROTO_OPCODE_DEGRADED_UPDATE )
			k.set( keyValueUpdate->size, keyValueUpdate->data );
		else
			k = *key;
		needsContinue = SlaveWorker::degradedChunkBuffer->map.insertDegradedKey( k, instanceId, requestId );
		// printf( "insertDegradedKey(): (%.*s) - needsContinue: %d\n", k.size, k.data, needsContinue );
	}

	if ( isSealed ) {
		if ( ! needsContinue )
			return true;

		// Send GET_CHUNK requests to surviving nodes
		Metadata metadata;
		metadata.set( listId, stripeId, 0 );
		selected = 0;
		for ( uint32_t i = 0; i < SlaveWorker::chunkCount; i++ ) {
			if ( selected >= SlaveWorker::dataChunkCount )
				break;
			if ( i == lostChunkId )
				continue;

			socket = ( i < SlaveWorker::dataChunkCount ) ?
			         ( this->dataSlaveSockets[ i ] ) :
			         ( this->paritySlaveSockets[ i - SlaveWorker::dataChunkCount ] );

			// Add to pending GET_CHUNK request set
			ChunkRequest chunkRequest;
			chunkRequest.set( listId, stripeId, i, socket, 0, true );
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

		selected = 0;
		for ( uint32_t i = 0; i < SlaveWorker::chunkCount; i++ ) {
			if ( selected >= SlaveWorker::dataChunkCount )
				break;
			if ( i == lostChunkId )
				continue;

			socket = ( i < SlaveWorker::dataChunkCount ) ?
			         ( this->dataSlaveSockets[ i ] ) :
			         ( this->paritySlaveSockets[ i - SlaveWorker::dataChunkCount ] );

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
		// Send GET request to surviving parity slave
		if ( socket->self ) {
			KeyValue keyValue;
			MasterEvent masterEvent;

			bool success = SlaveWorker::chunkBuffer->at( listId )->findValueByKey( mykey.data, mykey.size, &keyValue, &mykey );
			if ( success && opcode != PROTO_OPCODE_DEGRADED_DELETE ) {
				// Insert into degradedChunkBuffer
				char *key, *value;
				uint8_t keySize;
				uint32_t valueSize;
				Metadata metadata;

				metadata.set( listId, stripeId, lostChunkId );

				keyValue.deserialize( key, keySize, value, valueSize );
				keyValue.dup( key, keySize, value, valueSize );

				if ( ! SlaveWorker::degradedChunkBuffer->map.insertValue( keyValue, metadata ) ) {
					__ERROR__( "SlaveWorker", "performDegradedRead", "Cannot insert into degraded chunk buffer values map. (Key: %.*s)", keySize, key );
					// keyValue.free();
					// success = false;
				}
			}

			switch( opcode ) {
				case PROTO_OPCODE_DEGRADED_GET:
					if ( success ) {
						masterEvent.resGet( masterSocket, parentInstanceId, parentRequestId, keyValue, true );
					} else {
						// Return failure to master
						masterEvent.resGet( masterSocket, parentInstanceId, parentRequestId, mykey, true );
					}
					this->dispatch( masterEvent );
					op.data.key.free();
					break;
				case PROTO_OPCODE_DEGRADED_UPDATE:
					if ( success ) {
						Metadata metadata;
						metadata.set( listId, stripeId, lostChunkId );

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
							0, /* chunkUpdateOffset */
							keyValueUpdate->length, /* deltaSize */
							keyValueUpdate->offset,
							valueUpdate,
							false /* isSealed */,
							true /* isUpdate */
						);
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
						Metadata metadata;
						KeyMetadata keyMetadata;
						uint32_t timestamp;
						metadata.set( listId, stripeId, lostChunkId );
						keyMetadata.set( listId, stripeId, lostChunkId );

						// SlaveWorker::map->insertOpMetadata(
						// 	PROTO_OPCODE_DELETE, timestamp,
						// 	mykey, keyMetadata
						// );

						uint32_t tmp = 0;
						SlaveWorker::degradedChunkBuffer->deleteKey(
							PROTO_OPCODE_DELETE, timestamp,
							mykey.size, mykey.data,
							metadata,
							true /* isSealed */,
							tmp, 0, 0
						);

						this->sendModifyChunkRequest(
							parentInstanceId, parentRequestId,
							mykey.size, mykey.data,
							metadata,
							// not needed for deleting a key-value pair in an unsealed chunk:
							0, 0, 0, 0,
							false /* isSealed */,
							false /* isUpdate */
						);
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
			if ( ! SlaveWorker::pending->insertKey( PT_SLAVE_PEER_GET, instanceId, parentInstanceId, requestId, parentRequestId, socket, op.data.key ) ) {
				__ERROR__( "SlaveWorker", "performDegradedRead", "Cannot insert into slave GET pending map." );
			}
			event.reqGet( socket, instanceId, requestId, listId, lostChunkId, op.data.key );
			this->dispatch( event );
		}
		return true;
	}
}

bool SlaveWorker::sendModifyChunkRequest( uint16_t parentInstanceId, uint32_t parentRequestId, uint8_t keySize, char *keyStr, Metadata &metadata, uint32_t offset, uint32_t deltaSize, uint32_t valueUpdateOffset, char *delta, bool isSealed, bool isUpdate, uint32_t timestamp, MasterSocket *masterSocket ) {
	Key key;
	KeyValueUpdate keyValueUpdate;
	uint16_t instanceId = Slave::instanceId;
	uint32_t requestId = SlaveWorker::idGenerator->nextVal( this->workerId );

	key.set( keySize, keyStr );
	this->getSlaves( metadata.listId );

	if ( isSealed ) {
		// Send UPDATE_CHUNK / DELETE_CHUNK requests to parity slaves if the chunk is sealed
		ChunkUpdate chunkUpdate;
		chunkUpdate.set(
			metadata.listId, metadata.stripeId, metadata.chunkId,
			offset, deltaSize
		);
		chunkUpdate.setKeyValueUpdate( key.size, key.data, offset );

		for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
			if ( this->paritySlaveSockets[ i ]->self )
				continue;

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
			if ( this->paritySlaveSockets[ i ]->self ) {
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
				if ( masterSocket != 0 ) {
					Timestamp timestamp ( timestamp );
					Value value;
					value.set( deltaSize, delta );
					if ( isUpdate )
						masterSocket->backup.insertDataUpdate( timestamp , key, value, metadata, isSealed, offset, requestId, this->paritySlaveSockets[ i ] );
					else
						masterSocket->backup.insertDataDelete( timestamp , key, value, metadata, isSealed, offset, requestId, this->paritySlaveSockets[ i ] );
				}

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
	} else {
		// Send UPDATE / DELETE request if the chunk is not yet sealed

		// Check whether any of the parity slaves are self-socket
		uint32_t self = 0;
		for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
			if ( this->paritySlaveSockets[ i ]->self ) {
				self = i + 1;
				break;
			}
		}

		// Prepare UPDATE / DELETE request
		size_t size;
		Packet *packet = SlaveWorker::packetPool->malloc();
		packet->setReferenceCount( self == 0 ? SlaveWorker::parityChunkCount : SlaveWorker::parityChunkCount - 1 );
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

		for ( uint32_t i = 0; i < SlaveWorker::parityChunkCount; i++ ) {
			if ( this->paritySlaveSockets[ i ]->self )
				continue;

			// backup data delta, insert a pending record for each parity slave
			if ( masterSocket != 0 ) {
				Timestamp timestamp ( timestamp );
				Value value;
				value.set( deltaSize, delta );
				if ( isUpdate )
					masterSocket->backup.insertDataUpdate( timestamp , key, value, metadata, isSealed, offset, requestId, this->paritySlaveSockets[ i ] );
				else
					masterSocket->backup.insertDataDelete( timestamp , key, value, metadata, isSealed, offset, requestId, this->paritySlaveSockets[ i ] );
			}

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

		if ( ! self ) {
			self--;
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
