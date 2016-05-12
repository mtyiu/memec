#include <cassert>

#include "cuckoo_hash.hh"
#include "../util/debug.hh"
#include "../ds/key_value.hh"

void CuckooHash::init( uint32_t power ) {
	this->size = ( uint32_t ) 1 << power;
	this->hashPower = power;
	this->hashMask = this->size - 1;
	this->buckets = ( Bucket * ) malloc( this->size * sizeof( struct Bucket ) );

	this->kickCount = 0;

	if ( ! this->buckets ) {
		__ERROR__( "CuckooHash", "init", "Cannot initialize hashtable." );
		exit( 1 );
	}
	memset( this->buckets, 0, sizeof( struct Bucket ) * this->size );

	#ifdef CUCKOO_HASH_LOCK_OPT
		memset( this->keyver_array, 0, sizeof( keyver_array ) );
	#endif

	#ifdef CUCKOO_HASH_LOCK_FINEGRAIN
		for ( size_t i = 0; i < FG_LOCK_COUNT; i++ )
			pthread_spin_init( &this->fg_locks[ i ], PTHREAD_PROCESS_PRIVATE );
	#endif
}

void CuckooHash::setKeySize( uint8_t keySize ) {
	this->keySize = keySize;
}

size_t CuckooHash::indexHash( uint32_t hashValue ) {
	return ( hashValue >> ( 32 - this->hashPower ) );
}

uint8_t CuckooHash::tagHash( uint32_t hashValue ) {
	uint32_t r =  hashValue & TAG_MASK;
	return ( uint8_t ) r + ( r == 0 );
}

size_t CuckooHash::altIndex( size_t index, uint8_t tag ) {
	// 0x5bd1e995 is the hash constant from MurmurHash2
	return ( index ^ ( ( tag & TAG_MASK ) * 0x5bd1e995 ) ) & this->hashMask;
}

size_t CuckooHash::lockIndex( size_t i1, size_t i2, uint8_t tag ) {
	return i1 < i2 ? i1 : i2;
}

CuckooHash::CuckooHash() {
	this->init( HASHPOWER_DEFAULT );
}

CuckooHash::CuckooHash( uint32_t power ) {
	this->init( power );
}

CuckooHash::~CuckooHash() {
	free( this->buckets );
}

char *CuckooHash::tryRead( char *key, uint8_t keySize, uint8_t tag, size_t i ) {
#ifdef CUCKOO_HASH_ENABLE_TAG
	volatile uint32_t tmp = *( ( uint32_t * ) &( buckets[ i ] ) );
#endif
	Key target;

	target.set( keySize, key );

	for ( size_t j = 0; j < BUCKET_SIZE; j++ ) {
#ifdef CUCKOO_HASH_ENABLE_TAG
		uint8_t ch = ( ( uint8_t * ) &tmp )[ j ];
		if ( ch == tag )
#endif
		{
			char *ptr = buckets[ i ].ptr[ j ];
			if ( ! ptr ) return 0;

			if ( this->keySize == 0 ) {
				KeyValue keyValue;
				keyValue.set( ptr );

				Key key = keyValue.key( true );

				if ( key.equal( target ) )
					return ptr;
			} else {
				if ( memcmp( ptr, key, this->keySize ) == 0 )
					return ptr;
			}
		}
	}

	return 0;
}

char *CuckooHash::find( char *key, uint8_t keySize ) {
	Key target;
	uint32_t hashValue = HashFunc::hash( key, keySize );
	uint8_t tag = this->tagHash( hashValue );
	size_t i1 = this->indexHash( hashValue );
	size_t i2 = this->altIndex( i1, tag );
	char *result = 0;

	target.set( keySize, key );

#ifdef CUCKOO_HASH_LOCK_OPT
	size_t lock = this->lockIndex( i1, i2, tag );
	uint32_t vs, ve;
TryRead:
	vs = READ_KEYVER( lock );
#endif

#ifdef CUCKOO_HASH_LOCK_FINEGRAIN
	this->fg_lock( i1, i2 );
#endif

#ifdef CUCKOO_HASH_ENABLE_TAG
	volatile uint32_t tags1, tags2;
	tags1 = *( ( uint32_t * ) &( buckets[ i1 ] ) );
	tags2 = *( ( uint32_t * ) &( buckets[ i2 ] ) );
#endif

	for ( size_t j = 0; j < BUCKET_SIZE; j++ ) {
#ifdef CUCKOO_HASH_ENABLE_TAG
		uint8_t ch = ( ( uint8_t * ) &tags1 )[ j ];
		if ( ch == tag )
#endif
		{
			char *ptr = buckets[ i1 ].ptr[ j ];

			if ( ! ptr ) continue;

			if ( this->keySize == 0 ) {
				KeyValue keyValue;
				keyValue.set( ptr );

				Key key = keyValue.key( true );

				if ( key.equal( target ) ) {
					result = ptr;
					break;
				}
			} else {
				if ( memcmp( ptr, key, this->keySize ) == 0 ) {
					result = ptr;
					break;
				}
			}
		}
	}

	if ( ! result ) {
		for ( size_t j = 0; j < 4; j++ ) {
#ifdef CUCKOO_HASH_ENABLE_TAG
			uint8_t ch = ( ( uint8_t * ) &tags2 )[ j ];
			if ( ch == tag )
#endif
			{
				char *ptr = buckets[ i2 ].ptr[ j ];

				if ( ! ptr ) continue;

				if ( this->keySize == 0 ) {
					KeyValue keyValue;
					keyValue.set( ptr );

					Key key = keyValue.key( true );

					if ( key.equal( target ) ) {
						result = ptr;
						break;
					}
				} else {
					if ( memcmp( ptr, key, this->keySize ) == 0 ) {
						result = ptr;
						break;
					}
				}
			}
		}
	}

#ifdef CUCKOO_HASH_LOCK_OPT
	ve = READ_KEYVER( lock );

	if ( vs & 1 || vs != ve )
		goto TryRead;
#endif

#ifdef CUCKOO_HASH_LOCK_FINEGRAIN
	this->fg_unlock( i1, i2 );
#endif

	return result;
}

int CuckooHash::cpSearch( size_t depthStart, size_t *cpIndex ) {
	size_t depth = depthStart;
	while(
		( this->kickCount < MAX_CUCKOO_COUNT ) &&
		( depth >= 0 ) &&
		( depth < MAX_CUCKOO_COUNT - 1 )
	) {
		size_t *from = &( this->cp[ depth     ][ 0 ].bucket );
		size_t *to   = &( this->cp[ depth + 1 ][ 0 ].bucket );

		for ( size_t index = 0; index < CUCKOO_HASH_WIDTH; index++ ) {
			size_t i = from[ index ], j;
			for ( j = 0; j < BUCKET_SIZE; j++ ) {
				if ( IS_SLOT_EMPTY( i, j ) ) {
					this->cp[ depth ][ index ].slot = j;
					*cpIndex = index;
					return depth;
				}
			}

			j = rand() % BUCKET_SIZE;
			this->cp[ depth ][ index ].slot = j;
			this->cp[ depth ][ index ].ptr = this->buckets[ i ].ptr[ j ];
#ifdef CUCKOO_HASH_ENABLE_TAG
			to[ index ] = this->altIndex( i, this->buckets[ i ].tags[ j ] );
#else
			if ( this->keySize == 0 ) {
				char *key, *value;
				uint8_t keySize;
				uint32_t valueSize, splitOffset;

				KeyValue::deserialize( this->buckets[ i ].ptr[ j ], key, keySize, value, valueSize, splitOffset );

				bool isLarge = LargeObjectUtil::isLarge( keySize, valueSize );

				uint32_t hashValue = HashFunc::hash( key - ( isLarge ? SPLIT_OFFSET_SIZE : 0 ), keySize + ( isLarge ? SPLIT_OFFSET_SIZE : 0 ) );
				uint8_t tag = this->tagHash( hashValue );

				to[ index ] = this->altIndex( i, tag );
			} else {
				char *key = this->buckets[ i ].ptr[ j ];
				uint32_t hashValue = HashFunc::hash( key, this->keySize );
				uint8_t tag = this->tagHash( hashValue );

				to[ index ] = this->altIndex( i, tag );
			}
#endif
		}

		this->kickCount += CUCKOO_HASH_WIDTH;
		depth++;
	}

	__ERROR__( "CuckooHash", "cpSearch", "%u max cuckoo achieved, abort", this->kickCount );
	return -1;
}

int CuckooHash::cpBackmove( size_t depthStart, size_t index ) {
	int depth = depthStart;
	while( depth > 0 ) {
		size_t i1 = this->cp[ depth - 1 ][ index ].bucket,
		       i2 = this->cp[ depth     ][ index ].bucket,
		       j1 = this->cp[ depth - 1 ][ index ].slot,
		       j2 = this->cp[ depth     ][ index ].slot;

		if ( this->buckets[ i1 ].ptr[ j1 ] != this->cp[ depth - 1 ][ index ].ptr )
			return depth;

		assert( IS_SLOT_EMPTY( i2, j2 ) );

#ifdef CUCKOO_HASH_LOCK_OPT
		size_t lock = this->lockIndex( i1, i2, 0 );
		INCR_KEYVER( lock );
#endif

#ifdef CUCKOO_HASH_LOCK_FINEGRAIN
		this->fg_lock( i1, i2 );
#endif

#ifdef CUCKOO_HASH_ENABLE_TAG
		this->buckets[ i2 ].tags[ j2 ] = this->buckets[ i1 ].tags[ j1 ];
		this->buckets[ i1 ].tags[ j1 ] = 0;
#endif

		this->buckets[ i2 ].ptr[ j2 ] = this->buckets[ i1 ].ptr[ j1 ];
		this->buckets[ i1 ].ptr[ j1 ] = 0;

#ifdef CUCKOO_HASH_LOCK_OPT
		INCR_KEYVER( lock );
#endif

#ifdef CUCKOO_HASH_LOCK_FINEGRAIN
		this->fg_unlock( i1, i2 );
#endif

		depth--;
	}

	return depth;
}

int CuckooHash::cuckoo( int depth ) {
	int current;
	size_t index;

	this->kickCount = 0;

	while( 1 ) {
		current = this->cpSearch( depth, &index );
		if ( current < 0 )
			return -1;
		assert( index >= 0 );
		current = this->cpBackmove( current, index );
		if ( current == 0 )
			return index;

		depth = current - 1;
	}

	return -1;
}

bool CuckooHash::tryAdd( char *ptr, uint8_t tag, size_t i, size_t lock ) {
	for ( size_t j = 0; j < BUCKET_SIZE; j++ ) {
		if ( IS_SLOT_EMPTY( i, j ) ) {
#ifdef CUCKOO_HASH_LOCK_OPT
			INCR_KEYVER( lock );
#endif

#ifdef CUCKOO_HASH_LOCK_FINEGRAIN
			this->fg_lock( i, i );
#endif

#ifdef CUCKOO_HASH_ENABLE_TAG
			this->buckets[ i ].tags[ j ] = tag;
#endif
			this->buckets[ i ].ptr[ j ] = ptr;

#ifdef CUCKOO_HASH_LOCK_OPT
			INCR_KEYVER( lock );
#endif

#ifdef CUCKOO_HASH_LOCK_FINEGRAIN
			this->fg_unlock( i, i );
#endif

			return true;
		}
	}

	return false;
}

bool CuckooHash::insert( char *key, uint8_t keySize, char *ptr ) {
	uint32_t hashValue = HashFunc::hash( key, keySize );
	uint8_t tag = this->tagHash( hashValue );
	size_t i1 = this->indexHash( hashValue );
	size_t i2 = this->altIndex( i1, tag );
	size_t lock = this->lockIndex( i1, i2, tag );

	if ( this->tryAdd( ptr, tag, i1, lock ) ) return 1;
	if ( this->tryAdd( ptr, tag, i2, lock ) ) return 1;

	int index;
	size_t depth = 0;
	for ( index = 0; index < CUCKOO_HASH_WIDTH; index++ ) {
		this->cp[ depth ][ index ].bucket = ( index < CUCKOO_HASH_WIDTH / 2 ) ? i1 : i2;
	}

	size_t j;
	index = this->cuckoo( depth );
	if ( index >= 0 ) {
		i1 = this->cp[ depth ][ index ].bucket;
		j  = this->cp[ depth ][ index ].slot;

		if ( this->buckets[ i1 ].ptr[ j ] != 0 )
			__ERROR__( "CuckooHash", "insert", "Error: this->buckets[ i1 ].ptr[ j ] != 0." );

		if ( this->tryAdd( ptr, tag, i1, lock ) )
			return true;

		__ERROR__( "CuckooHash", "insert", "Error: i1 = %zu, i = %d.", i1, index );
	}

	__ERROR__( "CuckooHash", "insert", "Error: Hash table is full: power = %d. Need to increase power...", this->hashPower );
	return false;
}

bool CuckooHash::tryDel( char *key, uint8_t keySize, uint8_t tag, size_t i, size_t lock ) {
	Key target;
	target.set( keySize, key );

	for ( size_t j = 0; j < BUCKET_SIZE; j++ ) {
#ifdef CUCKOO_HASH_ENABLE_TAG
		if ( IS_TAG_EQUAL( this->buckets[ i ], j, tag ) )
#endif
		{
			char *ptr = this->buckets[ i ].ptr[ j ];
			if ( ! ptr ) {
#ifdef CUCKOO_HASH_ENABLE_TAG
				return false;
#else
				continue;
#endif
			}

			if ( this->keySize == 0 ) {
				KeyValue keyValue;
				keyValue.set( ptr );

				Key key = keyValue.key( true );

				if ( key.equal( target ) ) {
#ifdef CUCKOO_HASH_LOCK_OPT
					INCR_KEYVER( lock );
#endif

#ifdef CUCKOO_HASH_LOCK_FINEGRAIN
					this->fg_lock( i, i );
#endif

#ifdef CUCKOO_HASH_ENABLE_TAG
					this->buckets[ i ].tags[ j ] = 0;
#endif

					this->buckets[ i ].ptr[ j ] = 0;

#ifdef CUCKOO_HASH_LOCK_OPT
					INCR_KEYVER( lock );
#endif

#ifdef CUCKOO_HASH_LOCK_FINEGRAIN
					this->fg_unlock( i, i );
#endif

					return true;
				}
			} else {
				if ( memcmp( ptr, key, this->keySize ) == 0 ) {
#ifdef CUCKOO_HASH_LOCK_OPT
					INCR_KEYVER( lock );
#endif

#ifdef CUCKOO_HASH_LOCK_FINEGRAIN
					this->fg_lock( i, i );
#endif

#ifdef CUCKOO_HASH_ENABLE_TAG
					this->buckets[ i ].tags[ j ] = 0;
#endif

					this->buckets[ i ].ptr[ j ] = 0;

#ifdef CUCKOO_HASH_LOCK_OPT
					INCR_KEYVER( lock );
#endif

#ifdef CUCKOO_HASH_LOCK_FINEGRAIN
					this->fg_unlock( i, i );
#endif

					return true;
				}
			}
		}
	}
	return false;
}

void CuckooHash::del( char *key, uint8_t keySize ) {
	uint32_t hashValue = HashFunc::hash( key, keySize );
	uint8_t tag = this->tagHash( hashValue );
	size_t i1 = this->indexHash( hashValue );
	size_t i2 = this->altIndex( i1, tag );
	size_t lock = this->lockIndex( i1, i2, tag );

	if ( this->tryDel( key, keySize, tag, i1, lock ) ) return;
	if ( this->tryDel( key, keySize, tag, i2, lock ) ) return;
}
