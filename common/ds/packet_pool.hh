#ifndef __PACKET_POOL_HH__
#define __PACKET_POOL_HH__

#include "memory_pool.hh"

class Packet {
private:
	uint32_t referenceCount;
	uint32_t capacity;
	pthread_mutex_t lock;

public:
	uint32_t size;
	char *data;

	Packet() {
		this->referenceCount = 0;
		this->capacity = 0;
		this->size = 0;
		pthread_mutex_init( &this->lock, 0 );
		this->data = 0;
	}

	~Packet() {
		if ( this->data )
			delete[] this->data;
		this->data = 0;
	}

	bool init( uint32_t capacity ) {
		this->capacity = capacity;
		this->data = new char[ capacity ];
		return true;
	}

	bool read( char *&data, size_t &size ) {
		if ( this->size == 0 ) {
			data = 0;
			size = 0;
			return false;
		}

		data = this->data;
		size = this->size;

		return true;
	}

	bool write( char *data, size_t size ) {
		if ( size > this->capacity )
			return false;

		memcpy( this->data, data, size );
		this->size = size;

		return true;
	}

	void setReferenceCount( uint32_t count ) {
		fprintf( stderr, "{%lu} [%p] set referenceCount: %u\n", pthread_self(), this, count );
		this->referenceCount = count;
	}

	bool decrement() {
		bool ret;
		uint32_t prev;

		pthread_mutex_lock( &this->lock );
		prev = this->referenceCount;
		this->referenceCount--;
		fprintf( stderr, "{%lu} [%p] referenceCount = %u ---> %u\n", pthread_self(), this, prev, this->referenceCount );
		ret = this->referenceCount == 0;
		pthread_mutex_unlock( &this->lock );

		return ret;
	}

	static bool initFn( Packet *packet, void *argv ) {
		uint32_t size = *( ( uint32_t * ) argv );
		return packet->init( size );
	}
};

class PacketPool {
private:
	MemoryPool<Packet> *pool;

public:
	PacketPool() {
		this->pool = 0;
	}

	~PacketPool() {
		this->pool = 0;
	}

	void init( size_t capacity, size_t size ) {
		this->pool = MemoryPool<Packet>::getInstance();
		this->pool->init( capacity, Packet::initFn, ( void * ) &size );
	}

	Packet *malloc() {
		Packet *packet = this->pool->malloc();
		return packet;
	}

	void free( Packet *packet ) {
		if ( packet->decrement() ) { // Reference count == 0
			this->pool->free( packet );
		}
	}
};

#endif
