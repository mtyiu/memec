#include <cstdio>
#include <cstdlib>
#include <cerrno>
#include <cstring>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include "memec.hh"

size_t MemEC::read( size_t len, bool &connected ) {
	size_t recvBytes = 0;
	ssize_t ret;
	char *buf = this->buffer.recv.data + this->buffer.recv.len;
	bool block = len > 0;
	len = len == 0 ? ( this->buffer.recv.size - this->buffer.recv.len ) : len;
	do {
		ret = ::read(
			this->sockfd,
			buf + recvBytes,
			len - recvBytes
		);
		if ( ret > 0 ) {
			recvBytes += ( size_t ) ret;
			connected = true;
		}
		if ( ret <= 0 )
			connected = false;
	} while ( connected && block && ret >= 0 && recvBytes < len );
	this->buffer.recv.len += recvBytes;
	return recvBytes;
}

size_t MemEC::write() {
	size_t sentBytes = 0;
	ssize_t ret = 0;
	ssize_t len = this->buffer.send.len;
	do {
		ret = ::send(
			this->sockfd,
			this->buffer.send.data + sentBytes,
			len - sentBytes,
			0
		);
		// printf( "Sent %ld bytes.\n", ret );
		if ( ret == -1 ) {
			if ( errno == EWOULDBLOCK )
				continue;
			fprintf( stderr, "MemEC::write(): [%d] %s.", this->sockfd, strerror( errno ) );
		} else if ( ret == 0 ) {
			break;
		} else {
			sentBytes += ( size_t ) ret;
		}
	} while ( ( ssize_t ) sentBytes < len );
	this->buffer.send.len -= sentBytes;
	return sentBytes;
}

size_t MemEC::getSuggestedBufferSize( uint32_t keySize, uint32_t chunkSize ) {
	size_t ret = (
		PROTO_HEADER_SIZE +
		PROTO_KEY_VALUE_SIZE +
		keySize +
		chunkSize
	);
	// Set ret = ceil( ret / 4096 ) * 4096
	if ( ret & 4095 ) { // 0xFFF
		ret >>= 12;
		ret += 1;
		ret <<= 12;
	}
	// Set ret = ret * 2
	ret <<= 1;
	if ( ret < PROTO_BUF_MIN_SIZE )
		ret = PROTO_BUF_MIN_SIZE;
	return ret;
}

MemEC::MemEC( uint8_t keySize, uint32_t chunkSize, uint32_t batchSize, uint32_t addr, uint16_t port, uint32_t fromId, uint32_t toId ) {
	this->keySize = keySize;
	this->chunkSize = chunkSize;
	this->batchSize = batchSize;
	this->id = fromId;
	this->fromId = fromId;
	this->toId = toId;
	this->instanceId = 0;
	// Set addr
	memset( &this->addr, 0, sizeof( this->addr ) );
	this->addr.sin_family = AF_INET;
	this->addr.sin_port = port;
	this->addr.sin_addr.s_addr = addr;
	// Initialize socket
	this->sockfd = socket( AF_INET, SOCK_STREAM, 0 );
	if ( this->sockfd < 0 ) {
		fprintf( stderr, "MemEC::MemEC(): Cannot create socket: %s.\n", strerror( errno ) );
		exit( 1 );
	}
	// Initialize buffer
	size_t bufSize = this->getSuggestedBufferSize( keySize, chunkSize );
	bool ret = true;
	ret &= this->buffer.recv.init( bufSize );
	ret &= this->buffer.send.init( batchSize > 0 ? batchSize : bufSize );
	if ( ! ret ) {
		fprintf( stderr, "MemEC::MemEC(): Cannot allocate memory.\n" );
		exit( 1 );
	}
	// Initialize lock
	pthread_mutex_init( &this->pending.setLock, 0 );
	pthread_mutex_init( &this->pending.getLock, 0 );
	pthread_mutex_init( &this->pending.updateLock, 0 );
	pthread_mutex_init( &this->pending.delLock, 0 );
	// Initialize condition
	pthread_cond_init( &this->pending.setCond, 0 );
	pthread_cond_init( &this->pending.getCond, 0 );
	pthread_cond_init( &this->pending.updateCond, 0 );
	pthread_cond_init( &this->pending.delCond, 0 );
}

MemEC::~MemEC() {
	this->buffer.recv.free();
	this->buffer.send.free();
}

bool MemEC::connect() {
	if ( ::connect( this->sockfd, ( struct sockaddr * ) &this->addr, sizeof( this->addr ) ) != 0 ) {
		fprintf( stderr, "MemEC::connect(): Cannot connect to the master: %s.\n", strerror( errno ) );
		return false;
	}

	// Send register message
	uint32_t id = this->nextVal();
	size_t size = this->protocol.generateHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_OPCODE_REGISTER,
		0, 0, id, this->buffer.send.data
	);
	bool connected;
	this->buffer.send.len = size;
	this->write();

	size = this->read( PROTO_HEADER_SIZE, connected );
	if ( size != PROTO_HEADER_SIZE ) {
		fprintf( stderr, "MemEC::connect(): Cannot register with the master.\n" );
		return false;
	}

	struct ProtocolHeader header;
	if ( ! this->protocol.parseHeader( header, this->buffer.recv.data, this->buffer.recv.len ) ) {
		fprintf( stderr, "MemEC::connect(): Cannot parse register response message from the master.\n" );
		return false;
	}
	this->instanceId = header.instanceId;
	if ( header.requestId != id ) {
		fprintf( stderr, "MemEC::connect(): The response does not match the request ID.\n" );
		return false;
	}
	this->buffer.recv.len -= PROTO_HEADER_SIZE;

	// Start receive thread
	if ( pthread_create( &this->tid, 0, MemEC::recvThread, ( void * ) this ) ) {
		fprintf( stderr, "MemEC::connect(): Cannot start receive thread: %s.\n", strerror( errno ) );
		return false;
	}

	return true;
}

bool MemEC::disconnect() {
	size_t pending;
	int retryCount = 2;

	do {
		pending = 0;

		pthread_mutex_lock( &this->pending.setLock );
		while ( this->pending.set.size() ) {
			pthread_cond_wait( &this->pending.setCond, &this->pending.setLock );
		}
		pending += this->pending.set.size();
		pthread_mutex_unlock( &this->pending.setLock );

		pthread_mutex_lock( &this->pending.getLock );
		while ( this->pending.get.size() ) {
			pthread_cond_wait( &this->pending.getCond, &this->pending.getLock );
		}
		pending += this->pending.get.size();
		pthread_mutex_unlock( &this->pending.getLock );

		pthread_mutex_lock( &this->pending.updateLock );
		while ( this->pending.update.size() ) {
			pthread_cond_wait( &this->pending.updateCond, &this->pending.updateLock
			);
		}
		pending += this->pending.update.size();
		pthread_mutex_unlock( &this->pending.updateLock );

		pthread_mutex_lock( &this->pending.delLock );
		while ( this->pending.del.size() ) {
			pthread_cond_wait( &this->pending.delCond, &this->pending.delLock );
		}
		pending += this->pending.del.size();
		pthread_mutex_unlock( &this->pending.delLock );

		retryCount--;
		// this->printPending();
	} while ( retryCount != 0 || pending > 0 );
	return ( ! ::close( this->sockfd ) );
}

bool MemEC::get( char *key, uint8_t keySize, char *&value, uint32_t &valueSize ) {
	// Flush send buffer if it is full
	uint32_t required = PROTO_HEADER_SIZE + PROTO_KEY_SIZE + keySize;
	if ( this->buffer.send.len + required > this->buffer.send.size )
		this->write();

	// Generate GET request
	uint32_t id = this->nextVal();
	size_t size = this->protocol.generateKeyHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_OPCODE_GET,
		this->instanceId, id, keySize, key,
		this->buffer.send.data + this->buffer.send.len
	);
	this->buffer.send.len += size;

	// Add to pending map for GET
#ifdef WAIT_GET_RESPONSE
	pthread_mutex_t lock;
	pthread_cond_t cond;
	bool completed = false;
	pthread_mutex_init( &lock, 0 );
	pthread_cond_init( &cond, 0 );
	value = 0;
	valueSize = 0;

	struct GetResponse getResponse = {
		.lock = &lock,
		.cond = &cond,
		.completed = &completed,
		.valuePtr = &value,
		.valueSizePtr = &valueSize
	};
	pthread_mutex_lock( &this->pending.getLock );
	this->pending.get[ id ] = getResponse;
	pthread_mutex_unlock( &this->pending.getLock );

	this->write();

	// Wait for the response
	pthread_mutex_lock( &lock );
	while ( ! completed )
		pthread_cond_wait( &cond, &lock );
	pthread_mutex_unlock( &lock );

	pthread_mutex_lock( &this->pending.getLock );
	this->pending.get.erase( id );
	if ( this->pending.get.size() == 0 )
		pthread_cond_signal( &this->pending.getCond );
	pthread_mutex_unlock( &this->pending.getLock );

	return ( value != 0 && valueSize != 0 );
#else
	pthread_mutex_lock( &this->pending.getLock );
	this->pending.get.insert( id );
	pthread_mutex_unlock( &this->pending.getLock );

	return this->batchSize > 0 ? true : this->write() >= 0;
#endif
}

bool MemEC::set( char *key, uint8_t keySize, char *value, uint32_t valueSize ) {
	// Flush send buffer if it is full
	uint32_t required = PROTO_HEADER_SIZE + PROTO_KEY_VALUE_SIZE + keySize + valueSize;
	if ( this->buffer.send.len + required > this->buffer.send.size )
		this->write();

	// Generate SET request
	uint32_t id = this->nextVal();
	size_t size = this->protocol.generateKeyValueHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_OPCODE_SET,
		this->instanceId, id, keySize, key,
		valueSize, value,
		this->buffer.send.data + this->buffer.send.len
	);
	this->buffer.send.len += size;

	// Add to pending map for SET
	pthread_mutex_lock( &this->pending.setLock );
	this->pending.set.insert( id );
	pthread_mutex_unlock( &this->pending.setLock );

	return this->batchSize > 0 ? true : this->write() >= 0;
}

bool MemEC::update( char *key, uint8_t keySize, char *valueUpdate, uint32_t valueUpdateSize, uint32_t valueUpdateOffset ) {
	// Flush send buffer if it is full
	uint32_t required = PROTO_HEADER_SIZE + PROTO_KEY_VALUE_UPDATE_SIZE + keySize + valueUpdateSize;
	if ( this->buffer.send.len + required > this->buffer.send.size )
		this->write();

	// Generate DELETE request
	uint32_t id = this->nextVal();
	size_t size = this->protocol.generateKeyValueUpdateHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_OPCODE_UPDATE,
		this->instanceId, id, keySize, key,
		valueUpdateOffset, valueUpdateSize, valueUpdate,
		this->buffer.send.data + this->buffer.send.len
	);
	this->buffer.send.len += size;

	// Add to pending map for DELETE
	pthread_mutex_lock( &this->pending.updateLock );
	this->pending.update.insert( id );
	pthread_mutex_unlock( &this->pending.updateLock );

	return this->batchSize > 0 ? true : this->write() >= 0;
}

bool MemEC::del( char *key, uint8_t keySize ) {
	// Flush send buffer if it is full
	uint32_t required = PROTO_HEADER_SIZE + PROTO_KEY_SIZE;
	if ( this->buffer.send.len + required > this->buffer.send.size )
		this->write();

	// Generate DELETE request
	uint32_t id = this->nextVal();
	size_t size = this->protocol.generateKeyHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_OPCODE_DELETE,
		this->instanceId, id, keySize, key,
		this->buffer.send.data + this->buffer.send.len
	);
	this->buffer.send.len += size;

	// Add to pending map for DELETE
	pthread_mutex_lock( &this->pending.delLock );
	this->pending.del.insert( id );
	pthread_mutex_unlock( &this->pending.delLock );

	return this->batchSize > 0 ? true : this->write() >= 0;
}

void MemEC::recvThread() {
	bool connected, ret;
	size_t recvBytes, ptr;
	ProtocolHeader common;
	union {
		KeyHeader key;
		KeyValueHeader keyValue;
		KeyValueUpdateHeader keyValueUpdate;
	} header;

	memset( &common, 0, sizeof( common ) );
	memset( &header, 0, sizeof( header ) );

	do {
		if ( ! this->read( 0, connected ) )
			break;

		recvBytes = this->buffer.recv.len;
		ptr = 0;
		while ( recvBytes - ptr > 0 ) {
			if ( common.opcode == 0 ) {
				if ( recvBytes - ptr < PROTO_HEADER_SIZE ) {
					memmove(
						this->buffer.recv.data,
						this->buffer.recv.data + ptr,
						recvBytes - ptr
					);
					this->buffer.recv.len = recvBytes - ptr;
					ptr = 0;
					common.opcode = 0;
					break;
				}

				ret = this->protocol.parseHeader(
					common,
					this->buffer.recv.data + ptr,
					recvBytes - ptr
				);
				if ( ! ret ) {
					fprintf( stderr, "MemEC::recvThread(): Protocol::parseHeader() failed.\n" );
					exit( 1 );
				}

				ptr += PROTO_HEADER_SIZE;
			} else {
				std::unordered_set<uint32_t>::iterator it;

				if ( recvBytes - ptr < common.length ) {
					memmove(
						this->buffer.recv.data,
						this->buffer.recv.data + ptr,
						recvBytes - ptr
					);
					this->buffer.recv.len = recvBytes - ptr;
					ptr = 0;
					break;
				}

				switch( common.opcode ) {
					case PROTO_OPCODE_GET:
					{
#ifdef WAIT_GET_RESPONSE
						std::unordered_map<uint32_t, struct GetResponse>::iterator it;
#endif
						if ( common.magic == PROTO_MAGIC_RESPONSE_SUCCESS ) {
							ret = this->protocol.parseKeyValueHeader(
								header.keyValue,
								this->buffer.recv.data + ptr,
								common.length
							);
							if ( ! ret ) {
								fprintf( stderr, "MemEC::recvThread(): Protocol::parseKeyValueHeader() failed.\n" );
								exit( 1 );
							}
						} else {
							ret = this->protocol.parseKeyHeader(
								header.key,
								this->buffer.recv.data + ptr,
								common.length
							);
							if ( ! ret )
								fprintf( stderr, "MemEC::recvThread(): Protocol::parseKeyHeader() failed.\n" );
						}

						pthread_mutex_lock( &this->pending.getLock );
						it = this->pending.get.find( common.requestId );
						if ( it == this->pending.get.end() ) {
							fprintf( stderr, "MemEC::recvThread(): Cannot find a pending GET request that matches the response. The message will be discarded (ID: (%u, %u)).\n", common.instanceId, common.requestId );
							pthread_mutex_unlock( &this->pending.getLock );
						} else {
#ifdef WAIT_GET_RESPONSE
							pthread_mutex_unlock( &this->pending.getLock );
							struct GetResponse getResponse = it->second;
							pthread_mutex_lock( getResponse.lock );
							*getResponse.completed = true;
							if ( common.magic == PROTO_MAGIC_RESPONSE_SUCCESS ) {
								*getResponse.valuePtr = ( char * ) malloc( sizeof( char ) * header.keyValue.valueSize );
								memcpy( *getResponse.valuePtr, header.keyValue.value, header.keyValue.valueSize );
								*getResponse.valueSizePtr = header.keyValue.valueSize;
							}
							pthread_cond_signal( getResponse.cond );
							pthread_mutex_unlock( getResponse.lock );
#else
							this->pending.get.erase( it );

							pthread_mutex_lock( this->pending.recvBytesLock );
							*this->pending.recvBytes += header.keyValue.keySize + header.keyValue.valueSize;
							pthread_mutex_unlock( this->pending.recvBytesLock );

							if ( this->pending.get.size() == 0 )
								pthread_cond_signal( &this->pending.getCond );

							pthread_mutex_unlock( &this->pending.getLock );
#endif
						}
					}
						break;
					case PROTO_OPCODE_SET:
					{
						ret = this->protocol.parseKeyHeader(
							header.key,
							this->buffer.recv.data + ptr,
							common.length
						);
						if ( ! ret )
							fprintf( stderr, "MemEC::recvThread(): Protocol::parseKeyHeader() failed.\n" );

						pthread_mutex_lock( &this->pending.setLock );
						it = this->pending.set.find( common.requestId );
						if ( it == this->pending.set.end() ) {
							fprintf( stderr, "MemEC::recvThread(): Cannot find a pending SET request that matches the response. The message will be discarded (ID: (%u, %u)).\n", common.instanceId, common.requestId );
						} else {
							this->pending.set.erase( it );
							if ( this->pending.set.size() == 0 )
								pthread_cond_signal( &this->pending.setCond );
						}
						pthread_mutex_unlock( &this->pending.setLock );
					}
						break;
					case PROTO_OPCODE_UPDATE:
					{
						ret = this->protocol.parseKeyValueUpdateHeader(
							header.keyValueUpdate,
							this->buffer.recv.data + ptr,
							common.length
						);
						if ( ! ret )
							fprintf( stderr, "MemEC::recvThread(): Protocol::parseKeyValueUpdateHeader() failed.\n" );

						pthread_mutex_lock( &this->pending.updateLock );
						it = this->pending.update.find( common.requestId );
						if ( it == this->pending.update.end() ) {
							fprintf( stderr, "MemEC::recvThread(): Cannot find a pending UPDATE request that matches the response. The message will be discarded (ID: (%u, %u)).\n", common.instanceId, common.requestId );
						} else {
							this->pending.update.erase( it );
							if ( this->pending.update.size() == 0 )
								pthread_cond_signal( &this->pending.updateCond );
						}
						pthread_mutex_unlock( &this->pending.updateLock );
					}
						break;
					case PROTO_OPCODE_DELETE:
					{
						ret = this->protocol.parseKeyHeader(
							header.key,
							this->buffer.recv.data + ptr,
							common.length
						);
						if ( ! ret )
							fprintf( stderr, "MemEC::recvThread(): Protocol::parseKeyHeader() failed.\n" );

						pthread_mutex_lock( &this->pending.delLock );
						it = this->pending.del.find( common.requestId );
						if ( it == this->pending.del.end() ) {
							fprintf( stderr, "MemEC::recvThread(): Cannot find a pending DELETE request that matches the response. The message will be discarded (ID: (%u, %u)).\n", common.instanceId, common.requestId );
						} else {
							this->pending.del.erase( it );
							if ( this->pending.del.size() == 0 )
								pthread_cond_signal( &this->pending.delCond );
						}
						pthread_mutex_unlock( &this->pending.delLock );
					}
						break;
					default:
						break;
				}

				common.opcode = 0;
				ptr += common.length;
			}
		}

		if ( ptr > 0 && this->buffer.recv.len > ptr ) {
			memmove(
				this->buffer.recv.data,
				this->buffer.recv.data + ptr,
				this->buffer.recv.len - ptr
			);
		}
		this->buffer.recv.len -= ptr;
	} while ( connected );
}

bool MemEC::flush() {
	if ( this->buffer.send.len ) {
		this->write();
		return true;
	}
	return false;
}

void MemEC::printPending( FILE *f ) {
	int width = 25;
	fprintf(
		f,
		"---------- Pending requests ----------\n"
		"%*s : %lu\n"
		"%*s : %lu\n"
		"%*s : %lu\n"
		"%*s : %lu\n",
		width, "Number of SET requests", this->pending.set.size(),
		width, "Number of GET requests", this->pending.get.size(),
		width, "Number of UPDATE requests", this->pending.update.size(),
		width, "Number of DELETE requests", this->pending.del.size()
	);
}

void MemEC::setRecvBytesVar( pthread_mutex_t *recvBytesLock, uint64_t *recvBytes ) {
	this->pending.recvBytesLock = recvBytesLock;
	this->pending.recvBytes = recvBytes;
}

void *MemEC::recvThread( void *argv ) {
	MemEC *self = ( MemEC * ) argv;
	self->recvThread();
	pthread_exit( 0 );
}
