#ifndef __COMMON_DS_MEMORY_POOL_HH__
#define __COMMON_DS_MEMORY_POOL_HH__

#include <cstdlib>
#include <cassert>
#include <pthread.h>
#include "../lock/lock.hh"
#include "../util/debug.hh"

// Implemented the singleton pattern
template <class T> class MemoryPool {
private:
	MemoryPool() {}
	// No need to implement
	MemoryPool( MemoryPool<T> const& );
	void operator=( MemoryPool<T> const& );
	~MemoryPool() {
		LOCK( &this->mAccess );
		// while( this->count != 0 ) {
		// 	// Wait until all memory is returned
		// 	pthread_cond_wait( &this->cvEmpty, &this->mAccess );
		// }
		for ( uint64_t i = 0; i < this->capacity; i++ ) {
			delete poolBackup[ i ];
		}
		delete[] pool;
		delete[] poolBackup;
		UNLOCK( &this->mAccess );
	}

	volatile uint64_t readIndex;
	volatile uint64_t writeIndex;
	volatile uint64_t count; // current number of occupied elements
	volatile uint64_t capacity;

	LOCK_T mAccess;
	pthread_cond_t cvEmpty;
	T **pool;
	T **poolBackup;

public:
	static MemoryPool<T> *getInstance() {
		static MemoryPool<T> memoryPool;
		return &memoryPool;
	}

	inline uint64_t nextVal( uint64_t x ) {
		return ( ( x + 1 ) >= this->capacity ? 0 : x + 1 );
	}

	// Convert bytes to capacity of memory pool
	static uint64_t getCapacity( uint64_t space, uint64_t extra ) {
		uint64_t size = sizeof( T ) + extra;
		return ( space / size );
	}

	void init( uint64_t capacity, bool ( *initFn )( T *, void * ) = NULL, void *argv = NULL ) {
		this->readIndex = 0;
		this->writeIndex = 0;
		this->count = 0;
		this->capacity = capacity;

		LOCK_INIT( &this->mAccess, NULL );
		pthread_cond_init( &this->cvEmpty, NULL );

		this->pool = new T*[ capacity ];
		this->poolBackup = new T*[ capacity ];
		if ( ! this->pool || ! this->poolBackup ) {
			__ERROR__( "MemoryPool", "init", "Cannot allocate memory." );
			exit( 1 );
		}

		for ( uint64_t i = 0; i < capacity; i++ ) {
			this->pool[ i ] = new T();
			this->poolBackup[ i ] = this->pool[ i ];
			if ( ! this->pool[ i ] ) {
				__ERROR__( "MemoryPool", "init", "Cannot allocate memory." );
				exit( 1 );
			}
			if ( initFn ) {
				initFn( this->pool[ i ], argv );
			}
		}
	}

	T *malloc( T **buffer = 0, uint64_t count = 1 ) {
		T *ret = 0;

		LOCK( &this->mAccess );
		while( this->count + count > this->capacity ) {
			// Check whether the pool can provide the requested number of T
			// Doing this outside the loop to avoid deadlock!
			pthread_cond_wait( &this->cvEmpty, &this->mAccess );
		}

		if ( buffer && count ) {
			for ( uint64_t i = 0; i < count; i++ ) {
				buffer[ i ] = this->pool[ this->readIndex ];
				this->readIndex = nextVal( this->readIndex );
				this->count++;
			}
			ret = buffer[ 0 ];
		} else {
			ret = this->pool[ this->readIndex ];
			this->readIndex = nextVal( this->readIndex );
			this->count++;
		}
		UNLOCK( &this->mAccess );

		return ret;
	}

	uint64_t free( T *buffer ) {
		LOCK( &this->mAccess );
		assert( this->count != 0 );
		this->pool[ this->writeIndex ] = buffer;
		this->writeIndex = nextVal( this->writeIndex );
		this->count--;
		pthread_cond_signal( &this->cvEmpty );
		UNLOCK( &this->mAccess );
		return 1;
	}

	uint64_t free( T **buffer, uint64_t count ) {
		uint64_t ret = 0;

		LOCK( &this->mAccess );
		for ( uint64_t i = 0; i < count; i++ ) {
			assert( this->count != 0 );
			this->pool[ this->writeIndex ] = buffer[ i ];
			this->writeIndex = nextVal( this->writeIndex );
			this->count--;
			ret++;
		}
		pthread_cond_signal( &this->cvEmpty );
		UNLOCK( &this->mAccess );

		return ret;
	}

	uint64_t getCount() {
		return this->count;
	}
};

#endif
