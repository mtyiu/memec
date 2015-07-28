#ifndef __COMMON_DS_MEMORY_POOL_HH__
#define __COMMON_DS_MEMORY_POOL_HH__

#include <cstdlib>
#include <cassert>
#include <pthread.h>
#include "../util/debug.hh"

// Implemented the singleton pattern
template <class T> class MemoryPool {
private:
	MemoryPool() {}
	// No need to implement
	MemoryPool( MemoryPool<T> const& );
	void operator=( MemoryPool<T> const& );
	~MemoryPool() {
		pthread_mutex_lock( &this->mAccess );
		// while( this->count != 0 ) {
		// 	// Wait until all memory is returned
		// 	pthread_cond_wait( &this->cvEmpty, &this->mAccess );
		// }
		for ( size_t i = 0; i < this->capacity; i++ ) {
			delete poolBackup[ i ];
		}
		delete[] pool;
		delete[] poolBackup;
		pthread_mutex_unlock( &this->mAccess );
	}

	volatile size_t readIndex;
	volatile size_t writeIndex;
	volatile size_t count; // current number of occupied elements
	volatile size_t capacity;

	pthread_mutex_t mAccess;
	pthread_cond_t cvEmpty;
	T **pool;
	T **poolBackup;

public:
	static MemoryPool<T> *getInstance() {
		static MemoryPool<T> memoryPool;
		return &memoryPool;
	}

	inline size_t nextVal( size_t x ) {
		return ( ( x + 1 ) >= this->capacity ? 0 : x + 1 );
	}

	// Convert bytes to capacity of memory pool
	static size_t getCapacity( size_t bytes ) {
		
	}

	void init( size_t capacity, bool ( *initFn )( T *, void * ) = NULL, void *argv = NULL ) {
		this->readIndex = 0;
		this->writeIndex = 0;
		this->count = 0;
		this->capacity = capacity;

		pthread_mutex_init( &this->mAccess, NULL );
		pthread_cond_init( &this->cvEmpty, NULL );

		this->pool = new T*[ capacity ];
		this->poolBackup = new T*[ capacity ];
		if ( ! this->pool || ! this->poolBackup ) {
			__ERROR__( "MemoryPool", "init", "Cannot allocate memory." );
			exit( 1 );
		}

		for ( size_t i = 0; i < capacity; i++ ) {
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

	T *malloc( T **buffer = 0, size_t count = 0 ) {
		T *ret = 0;

		pthread_mutex_lock( &this->mAccess );
		while( this->count + count > this->capacity ) {
			// Check whether the pool can provide the requested number of T
			// Doing this outside the loop to avoid deadlock!
			pthread_cond_wait( &this->cvEmpty, &this->mAccess );
		}

		if ( buffer && count ) {
			for ( size_t i = 0; i < count; i++ ) {
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
		pthread_mutex_unlock( &this->mAccess );

		return ret;
	}

	size_t free( T *buffer ) {
		pthread_mutex_lock( &this->mAccess );
		assert( this->count != 0 );
		this->pool[ this->writeIndex ] = buffer;
		this->writeIndex = nextVal( this->writeIndex );
		this->count--;
		pthread_cond_signal( &this->cvEmpty );
		pthread_mutex_unlock( &this->mAccess );
		return 1;
	}

	size_t free( T **buffer, size_t count ) {
		size_t ret = 0;

		pthread_mutex_lock( &this->mAccess );
		for ( size_t i = 0; i < count; i++ ) {
			assert( this->count != 0 );
			this->pool[ this->writeIndex ] = buffer[ i ];
			this->writeIndex = nextVal( this->writeIndex );
			this->count--;
			ret++;
		}
		pthread_cond_signal( &this->cvEmpty );
		pthread_mutex_unlock( &this->mAccess );

		return ret;
	}
};

#endif
