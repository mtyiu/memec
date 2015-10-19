#include "map.hh"

uint32_t *Map::stripes;
LOCK_T Map::stripesLock;

void Map::init( uint32_t numStripeList ) {
	Map::stripes = new uint32_t[ numStripeList ];
	LOCK_INIT( &Map::stripesLock );
	for ( uint32_t i = 0; i < numStripeList; i++ )
		Map::stripes[ i ] = 0;
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

bool Map::seal( uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool needsLock, bool needsUnlock ) {
	Metadata metadata;
	metadata.set( listId, stripeId, chunkId );

	std::pair<std::unordered_set<Metadata>::iterator, bool> ret;

	if ( needsLock ) LOCK( &this->chunksLock );
	ret = this->chunks.insert( metadata );
	if ( needsUnlock ) UNLOCK( &this->chunksLock );

	Map::updateMaxStripeId( listId, stripeId );

	return ret.second;
}

bool Map::setKey( char *keyStr, uint8_t keySize, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint8_t opcode, bool needsLock, bool needsUnlock ) {
	Key key;
	key.size = keySize;
	key.dup( keySize, keyStr );

	Metadata metadata;
	metadata.set( listId, stripeId, chunkId );

	std::unordered_map<Key, Metadata>::iterator it;
	std::pair<Key, Metadata> p( key, metadata );
	bool ret = true;

	if ( needsLock ) LOCK( &this->keysLock );
	it = this->keys.find( key );
	if ( it == this->keys.end() && opcode == PROTO_OPCODE_SET ) {
		std::pair<std::unordered_map<Key, Metadata>::iterator, bool> r;
		r = this->keys.insert( p );
		ret = r.second;
	} else if ( it != this->keys.end() && opcode == PROTO_OPCODE_DELETE ) {
		this->keys.erase( it );
	} else {
		ret = false;
	}
	if ( needsUnlock ) UNLOCK( &this->keysLock );

	Map::updateMaxStripeId( listId, stripeId );

	return ret;
}

void Map::dump( FILE *f ) {
	fprintf( f, "List of key-value pairs:\n------------------------\n" );
	if ( ! this->keys.size() ) {
		fprintf( f, "(None)\n" );
	} else {
		for ( std::unordered_map<Key, Metadata>::iterator it = this->keys.begin(); it != this->keys.end(); it++ ) {
			fprintf(
				f, "%.*s --> (list ID: %u, stripe ID: %u, chunk ID: %u)\n",
				it->first.size, it->first.data,
				it->second.listId, it->second.stripeId, it->second.chunkId
			);
		}
		fprintf( f, "Count: %lu\n", this->keys.size() );
	}
	fprintf( f, "\n" );
}

void Map::persist( FILE *f ) {
	LOCK( &this->keysLock );
	for ( std::unordered_map<Key, Metadata>::iterator it = this->keys.begin(); it != this->keys.end(); it++ ) {
		Key key = it->first;
		Metadata metadata = it->second;

		fprintf( f, "%.*s\t%u\t%u\t%u\n", key.size, key.data, metadata.listId, metadata.stripeId, metadata.chunkId );
	}
	UNLOCK( &this->keysLock );
}
