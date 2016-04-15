#ifndef __COMMON_HASH_CUCKOO_HASH_HH__
#define __COMMON_HASH_CUCKOO_HASH_HH__

#include <stdint.h>
#include <pthread.h>

/********** Configuration **********/
// #define CUCKOO_HASH_LOCK_OPT
#define CUCKOO_HASH_LOCK_FINEGRAIN

/********** Constants **********/
#ifdef CUCKOO_HASH_LOCK_OPT
	#define KEYVER_COUNT ( ( uint32_t ) 1 << 13 )
	#define KEYVER_MASK ( KEYVER_COUNT - 1 )
#endif
#ifdef CUCKOO_HASH_LOCK_FINEGRAIN
	#define FG_LOCK_COUNT ( ( uint32_t ) 1 << 13 )
	#define FG_LOCK_MASK  ( FG_LOCK_COUNT - 1 )
#endif

#define CUCKOO_HASH_WIDTH 1
#define HASHPOWER_DEFAULT 25
#define MAX_CUCKOO_COUNT 500
#define BUCKET_SIZE      4
#define TAG_MASK         ( ( uint32_t ) 0x000000FF )

/********** Data structures **********/
struct Bucket {
	uint8_t tags[ BUCKET_SIZE ]; // Tag: 1-byte summary of key
	char reserved[ 4 ];
	char *ptr[ BUCKET_SIZE ];    // ptr: Object pointer
} __attribute__( ( __packed__ ) );

/********** Macros **********/
#define IS_SLOT_EMPTY( i, j ) ( buckets[ i ].tags[ j ] == 0 )
#define IS_TAG_EQUAL( bucket, j, tag ) ( ( bucket.tags[ j ] & TAG_MASK ) == tag )

#ifdef CUCKOO_HASH_LOCK_OPT
	#define READ_KEYVER( lock ) \
		__sync_fetch_and_add( &this->keyver_array[ lock & KEYVER_MASK ], 0 )

	#define INCR_KEYVER( lock ) \
		__sync_fetch_and_add( &this->keyver_array[ lock & KEYVER_MASK ], 1 )
#endif

/********** CuckooHash class definition **********/
class CuckooHash {
private:
	uint32_t size;
	uint32_t hashPower;
	uint32_t hashMask;
	struct Bucket *buckets;

	int kickCount;
	struct {
		size_t bucket;
		size_t slot;
		char *ptr;
	} cp[ MAX_CUCKOO_COUNT ][ CUCKOO_HASH_WIDTH ];

	#ifdef CUCKOO_HASH_LOCK_OPT
		uint32_t keyver_array[ KEYVER_COUNT ];
	#endif
	#ifdef CUCKOO_HASH_LOCK_FINEGRAIN
		pthread_spinlock_t fg_locks[ FG_LOCK_COUNT ];
	#endif

	void init( uint32_t power );

	size_t indexHash( uint32_t hashValue );
	uint8_t tagHash( uint32_t hashValue );
	size_t altIndex( size_t index, uint8_t tag );
	size_t lockIndex( size_t i1, size_t i2, uint8_t tag );

	char *tryRead( char *key, uint8_t keySize, uint8_t tag, size_t i );
	bool tryAdd( char *ptr, uint8_t tag, size_t i, size_t lock );
	bool tryDel( char *key, uint8_t keySize, uint8_t tag, size_t i, size_t lock );

	int cpSearch( size_t depthStart, size_t *cpIndex );
	int cpBackmove( size_t depthStart, size_t index );
	int cuckoo( int depth );

	#ifdef CUCKOO_HASH_LOCK_FINEGRAIN
		void fg_lock( uint32_t i1, uint32_t i2 ) {
			uint32_t j1, j2;
			j1 = i1 & FG_LOCK_MASK;
			j2 = i2 & FG_LOCK_MASK;

			if ( j1 < j2 ) {
				pthread_spin_lock( &fg_locks[ j1 ] );
				pthread_spin_lock( &fg_locks[ j2 ] );
			} else if ( j1 > j2 ) {
				pthread_spin_lock( &fg_locks[ j2 ] );
				pthread_spin_lock( &fg_locks[ j1 ] );
			} else {
				pthread_spin_lock( &fg_locks[ j1 ] );
			}
		}

		void fg_unlock( uint32_t i1, uint32_t i2 ) {
			uint32_t j1, j2;
			j1 = i1 & FG_LOCK_MASK;
			j2 = i2 & FG_LOCK_MASK;

			if ( j1 < j2 ) {
				pthread_spin_unlock( &fg_locks[ j2 ] );
				pthread_spin_unlock( &fg_locks[ j1 ] );
			} else if ( j1 > j2 ) {
				pthread_spin_unlock( &fg_locks[ j1 ] );
				pthread_spin_unlock( &fg_locks[ j2 ] );
			} else {
				pthread_spin_unlock( &fg_locks[ j1 ] );
			}
		}
	#endif

public:
	CuckooHash();
	CuckooHash( uint32_t power );
	~CuckooHash();

	char *find( char *key, uint8_t keySize, uint32_t hashValue );
	int insert( char *ptr, uint32_t hashValue );
	void del( char *key, uint8_t keySize, uint32_t hashValue );
};

#endif
