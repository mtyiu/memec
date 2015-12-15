#ifndef __BENCHMARK_HUAWEI_MEMEC_HH__
#define __BENCHMARK_HUAWEI_MEMEC_HH__

#include <unordered_set>
#include <unordered_map>
#include <pthread.h>
#include "protocol.hh"

class Buffer {
public:
	char *data;
	size_t len;
	size_t size;

	Buffer() {
		this->data = 0;
		this->len = 0;
		this->size = 0;
	}

	bool init( size_t size ) {
		this->size = size;
		this->data = ( char * ) malloc( sizeof( char ) * size );
		return ( this->data != 0 );
	}

	void free() {
		if ( this->data )
			::free( this->data );
		this->data = 0;
		this->len = 0;
		this->size = 0;
	}
};

struct GetResponse {
	pthread_mutex_t *lock;
	pthread_cond_t *cond;
	bool *completed;
	char **valuePtr;
	uint32_t *valueSizePtr;
};

class MemEC {
private:
	uint8_t keySize;
	uint32_t chunkSize;
	uint32_t batchSize;
	uint32_t id, fromId, toId;
	struct sockaddr_in addr;
	int sockfd;
	pthread_t tid;

	struct {
		Buffer recv;
		Buffer send;
	} buffer;

	struct {
		std::unordered_set<uint32_t> set, update, del;
		std::unordered_map<uint32_t, struct GetResponse> get;
		pthread_mutex_t setLock;
		pthread_mutex_t getLock;
		pthread_mutex_t updateLock;
		pthread_mutex_t delLock;
	} pending;

	Protocol protocol;

	inline uint32_t nextVal() {
		if ( this->id == this->toId - 1 )
			this->id = this->fromId;
		else
			this->id++;
		return this->id;
	}

	size_t read( size_t len, bool &connected );
	size_t write();
	size_t getSuggestedBufferSize( uint32_t keySize, uint32_t chunkSize );

public:
	MemEC( uint8_t keySize, uint32_t chunkSize, uint32_t batchSize, uint32_t addr, uint16_t port, uint32_t fromId, uint32_t toId );
	~MemEC();
	bool connect();
	bool disconnect();
	bool get( char *key, uint8_t keySize, char *&value, uint32_t &valueSize );
	bool set( char *key, uint8_t keySize, char *value, uint32_t valueSize );
	bool update( char *key, uint8_t keySize, char *valueUpdate, uint32_t valueUpdateSize, uint32_t valueUpdateOffset );
	bool del( char *key, uint8_t keySize );
	bool flush();
	void printPending( FILE *f = stdout );

	void recvThread();
	static void *recvThread( void *argv );
};

#endif
