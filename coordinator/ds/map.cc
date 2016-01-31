#include "map.hh"

uint32_t *Map::stripes;
LOCK_T Map::stripesLock;
std::unordered_map<ListStripe, DegradedLock> Map::degradedLocks;
std::unordered_map<ListStripe, DegradedLock> Map::releasingDegradedLocks;
LOCK_T Map::degradedLocksLock;

void Map::init( uint32_t numStripeList ) {
	Map::stripes = new uint32_t[ numStripeList ];
	LOCK_INIT( &Map::stripesLock );
	for ( uint32_t i = 0; i < numStripeList; i++ )
		Map::stripes[ i ] = 0;
	LOCK_INIT( &Map::degradedLocksLock );
}

void Map::free() {
	delete Map::stripes;
}

Map::Map() {
	LOCK_INIT( &this->chunksLock );
	LOCK_INIT( &this->keysLock );
}

bool Map::updateMaxStripeId( uint32_t listId, uint32_t stripeId ) {
	bool ret = false;
	LOCK( &Map::stripesLock );
	if ( Map::stripes[ listId ] < stripeId ) {
		Map::stripes[ listId ] = stripeId;
		ret = true;
	}
	UNLOCK( &Map::stripesLock );
	return ret;
}

bool Map::insertChunk( uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool needsLock, bool needsUnlock ) {
	Metadata metadata;
	metadata.set( listId, stripeId, chunkId );

	std::pair<std::unordered_set<Metadata>::iterator, bool> ret;

	if ( needsLock ) LOCK( &this->chunksLock );
	ret = this->chunks.insert( metadata );
	if ( needsUnlock ) UNLOCK( &this->chunksLock );

	this->updateMaxStripeId( listId, stripeId );

	return ret.second;
}

bool Map::insertKey( char *keyStr, uint8_t keySize, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint8_t opcode, uint32_t timestamp, bool needsLock, bool needsUnlock ) {
	Key key;
	key.set( keySize, keyStr );

	OpMetadata opMetadata;
	opMetadata.set( listId, stripeId, chunkId );
	opMetadata.opcode = opcode;
	opMetadata.timestamp = timestamp;

	std::unordered_map<Key, OpMetadata>::iterator it;
	bool ret = true, exist;

	if ( needsLock ) LOCK( &this->keysLock );
	it = this->keys.find( key );
	exist = it != this->keys.end();

	switch( opcode ) {
		case PROTO_OPCODE_SET:
		case PROTO_OPCODE_REMAPPING_SET:
			if ( exist ) {
				if ( it->second.timestamp > timestamp ) {
					switch( it->second.opcode ) {
						case PROTO_OPCODE_SET:
						case PROTO_OPCODE_REMAPPING_SET:
							// Replace with the latest record
							it->second = opMetadata;
							break;
						case PROTO_OPCODE_DELETE:
							// This key should be deleted
							key = it->first;
							key.free();
							this->keys.erase( it );
							break;
						default:
							ret = false;
					}
				} else {
					// Replace with the latest record
					it->second = opMetadata;
				}
			} else {
				key.dup();

				std::pair<Key, OpMetadata> p( key, opMetadata );
				std::pair<std::unordered_map<Key, OpMetadata>::iterator, bool> r;

				r = this->keys.insert( p );
				this->lockedKeys.erase( key );

				ret = r.second;
			}
			break;
		case PROTO_OPCODE_DELETE:
			if ( exist ) {
				key = it->first;
				key.free();
				this->keys.erase( it );
			} else {
				key.dup();

				std::pair<Key, OpMetadata> p( key, opMetadata );
				std::pair<std::unordered_map<Key, OpMetadata>::iterator, bool> r;

				r = this->keys.insert( p );
				this->lockedKeys.erase( key );

				ret = r.second;
			}
			break;
		case PROTO_OPCODE_REMAPPING_LOCK:
			if ( exist ) {
				fprintf(
					stderr, "TODO: Map::insertKey(): opcode == PROTO_OPCODE_REMAPPING_LOCK && exist. Key = %.*s at (%u, %u, %u).\n",
					it->first.size, it->first.data,
					it->second.listId, it->second.stripeId, it->second.chunkId
				);
				ret = false;
			} else {
				// check if lock is already acquired by others
				if ( this->lockedKeys.count( key ) ) {
					ret = false;
				} else {
					key.dup();
					this->lockedKeys.insert( key );
				}
			}
			break;
		default:
			printf( "Unknown opcode %d for key: %.*s for (timestamp: %u vs. %u).\n", opcode, keySize, keyStr, timestamp, it->second.timestamp );
			ret = false;
			break;
	}
	if ( needsUnlock ) UNLOCK( &this->keysLock );

	Map::updateMaxStripeId( listId, stripeId );

	return ret;
}

bool Map::insertDegradedLock( uint32_t listId, uint32_t stripeId,
uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount, uint32_t ongoingAtChunk, bool needsLock, bool needsUnlock ) {
	ListStripe listStripe;
	DegradedLock degradedLock( original, reconstructed, reconstructedCount, ongoingAtChunk );
	listStripe.set( listId, stripeId );

	std::unordered_map<ListStripe, DegradedLock>::iterator it;
	bool ret = true;

	if ( needsLock ) LOCK( &this->degradedLocksLock );
	it = this->degradedLocks.find( listStripe );
	if ( it == this->degradedLocks.end() ) {
		degradedLock.dup();

		std::pair<std::unordered_map<ListStripe, DegradedLock>::iterator, bool> r;
		std::pair<ListStripe, DegradedLock> p( listStripe, degradedLock );

		r = this->degradedLocks.insert( p );

		ret = r.second;
	} else {
		ret = false;
	}
	if ( needsUnlock ) UNLOCK( &this->degradedLocksLock );

	return ret;
}

bool Map::expandDegradedLock( uint32_t listId, uint32_t stripeId,
uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount, uint32_t ongoingAtChunk, DegradedLock &degradedLock, bool needsLock, bool needsUnlock ) {
	ListStripe listStripe;
	listStripe.set( listId, stripeId );

	std::unordered_map<ListStripe, DegradedLock>::iterator it;
	bool ret = true;

	if ( needsLock ) LOCK( &this->degradedLocksLock );
	it = this->degradedLocks.find( listStripe );
	if ( it == this->degradedLocks.end() ) {
		ret = false;
	} else {
		DegradedLock &d = it->second;
		d.expand( original, reconstructed, reconstructedCount, ongoingAtChunk );
		degradedLock = d;
	}
	if ( needsUnlock ) UNLOCK( &this->degradedLocksLock );

	return ret;
}

bool Map::findMetadataByKey( char *keyStr, uint8_t keySize, Metadata &metadata ) {
	std::unordered_map<Key, OpMetadata>::iterator it;
	Key key;

	key.set( keySize, keyStr );

	LOCK( &this->keysLock );
	it = this->keys.find( key );
	if ( it == this->keys.end() ) {
		UNLOCK( &this->keysLock );
		return false;
	} else {
		metadata = it->second;
	}
	UNLOCK( &this->keysLock );
	return true;
}

bool Map::findDegradedLock( uint32_t listId, uint32_t stripeId, DegradedLock &degradedLock, bool needsLock, bool needsUnlock, LOCK_T **lock ) {
	std::unordered_map<ListStripe, DegradedLock>::iterator it;
	ListStripe listStripe;
	listStripe.set( listId, stripeId );

	if ( lock ) *lock = &this->degradedLocksLock;
	if ( needsLock ) LOCK( &this->degradedLocksLock );
	it = this->degradedLocks.find( listStripe );
	if ( it == this->degradedLocks.end() ) {
		if ( needsUnlock ) UNLOCK( &this->degradedLocksLock );
		return false;
	} else {
		degradedLock = it->second;
	}
	if ( needsUnlock ) UNLOCK( &this->degradedLocksLock );
	return true;
}

bool Map::isSealed( Metadata metadata ) {
	std::unordered_set<Metadata>::iterator it;
	bool ret;

	LOCK( &this->chunksLock );
	it = this->chunks.find( metadata );
	ret = ( it != this->chunks.end() );
	UNLOCK( &this->chunksLock );
	return ret;
}

size_t Map::dump( FILE *f ) {
	size_t numKeys;

	fprintf( f, "List of sealed chunks:\n----------------------\n" );
	LOCK( &this->chunksLock );
	if ( ! this->chunks.size() ) {
		fprintf( f, "(None)\n" );
	} else {
		// for ( std::unordered_set<Metadata>::iterator it = this->chunks.begin(); it != this->chunks.end(); it++ ) {
		// 	const Metadata &m = *it;
		// 	fprintf(
		// 		f, "(%u, %u, %u)\n",
		// 		m.listId, m.stripeId, m.chunkId
		// 	);
		// }
		fprintf( f, "Count: %lu\n", this->chunks.size() );
	}
	UNLOCK( &this->chunksLock );
	fprintf( f, "\n" );

	fprintf( f, "List of key-value pairs:\n------------------------\n" );
	LOCK( &this->keysLock );
	if ( ! this->keys.size() ) {
		fprintf( f, "(None)\n" );
	} else {
		/*
		for ( std::unordered_map<Key, OpMetadata>::iterator it = this->keys.begin(); it != this->keys.end(); it++ ) {
			fprintf(
				f, "%.*s --> (list ID: %u, stripe ID: %u, chunk ID: %u)\n",
				it->first.size, it->first.data,
				it->second.listId, it->second.stripeId, it->second.chunkId
			);
		}
		*/
		fprintf( f, "Count: %lu\n", this->keys.size() );
	}
	numKeys = this->keys.size();
	UNLOCK( &this->keysLock );
	fprintf( f, "\n" );

	return numKeys;
}

size_t Map::dumpDegradedLocks( FILE *f ) {
	size_t ret;
	fprintf( f, "#### List of degraded locks ####\n" );
	LOCK( &Map::degradedLocksLock );
	if ( ! Map::degradedLocks.size() ) {
		fprintf( f, "(None)\n" );
	} else {
		for ( std::unordered_map<ListStripe, DegradedLock>::iterator it = Map::degradedLocks.begin(); it != Map::degradedLocks.end(); it++ ) {
			fprintf(
				f, "(%u, %u): ",
				it->first.listId, it->first.stripeId
			);
			for ( uint32_t i = 0; i < it->second.reconstructedCount; i++ ) {
				fprintf(
					f, "%s(%u, %u) |-> (%u, %u)",
					i == 0 ? "" : "; ",
					it->second.original[ i * 2     ],
					it->second.original[ i * 2 + 1 ],
					it->second.reconstructed[ i * 2     ],
					it->second.reconstructed[ i * 2 + 1 ]
				);
			}
			fprintf( f, "%s\n", it->second.reconstructedCount ? "" : " (none)" );
		}
		fprintf( f, "Count: %lu\n", Map::degradedLocks.size() );
	}
	ret = Map::degradedLocks.size();
	UNLOCK( &Map::degradedLocksLock );
	fprintf( f, "\n" );

	return ret;
}

void Map::persist( FILE *f ) {
	LOCK( &this->keysLock );
	for ( std::unordered_map<Key, OpMetadata>::iterator it = this->keys.begin(); it != this->keys.end(); it++ ) {
		Key key = it->first;
		OpMetadata opMetadata = it->second;

		fprintf( f, "%.*s\t%u\t%u\t%u\n", key.size, key.data, opMetadata.listId, opMetadata.stripeId, opMetadata.chunkId );
	}
	UNLOCK( &this->keysLock );
}
