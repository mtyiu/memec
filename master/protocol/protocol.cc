#include "protocol.hh"
#include "../../common/util/debug.hh"

bool MasterProtocol::init( size_t size, uint32_t parityChunkCount ) {
	this->status = new bool[ parityChunkCount ];
	return Protocol::init( size );
}

void MasterProtocol::free() {
	delete[] this->status;
	Protocol::free();
}

char *MasterProtocol::reqRegisterCoordinator( size_t &size, uint32_t id, uint32_t addr, uint16_t port ) {
	size = this->generateAddressHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_REGISTER,
		id,
		addr, port
	);
	return this->buffer.send;
}

char *MasterProtocol::reqPushLoadStats(
		size_t &size, uint32_t id,
		ArrayMap< struct sockaddr_in, Latency > *slaveGetLatency,
		ArrayMap< struct sockaddr_in, Latency > *slaveSetLatency )
{

	size = this->generateLoadStatsHeader(
		PROTO_MAGIC_LOADING_STATS,
		PROTO_MAGIC_TO_COORDINATOR,
		id,
		slaveGetLatency->size(),
		slaveSetLatency->size(),
		0,
		sizeof( uint32_t ) * 3 + sizeof( uint16_t ),
		sizeof( uint32_t ) + sizeof( uint16_t )
	);

	// TODO only send stats of most heavily loaded slave in case buffer overflows

	uint32_t addr, sec, nsec;
	uint16_t port;

#define SET_FIELDS_VAR( _SRC_ ) \
	addr = _SRC_->keys[ idx ].sin_addr.s_addr; \
	port = _SRC_->keys[ idx ].sin_port; \
	sec = _SRC_->values[ idx ]->sec; \
	nsec = _SRC_->values[ idx ]->nsec; \

	for ( uint32_t i = 0; i < slaveGetLatency->size() + slaveSetLatency->size(); i++ ) {
		uint32_t idx = i;
		// serialize the loading stats
		if ( i < slaveGetLatency->size() ) {
			SET_FIELDS_VAR( slaveGetLatency );
		} else {
			idx = i - slaveGetLatency->size();
			SET_FIELDS_VAR( slaveSetLatency );
		}

		//fprintf ( stderr, " stats send %d IP %u:%u time %us %unsec\n", i, ntohl( addr ), ntohs( port ), sec, nsec );
		*( ( uint32_t * )( this->buffer.send + size ) ) = addr; // htonl( addr );
		size += sizeof( addr );
		*( ( uint16_t * )( this->buffer.send + size ) ) = port; // htons( port );
		size += sizeof( port );
		*( ( uint32_t * )( this->buffer.send + size ) ) = htonl( sec );
		size += sizeof( sec );
		*( ( uint32_t * )( this->buffer.send + size ) ) = htonl( nsec );
		size += sizeof( nsec );
	}

#undef SET_FIELDS_VAR

	if ( size > PROTO_BUF_MIN_SIZE ) {
		__DEBUG__( CYAN, "MasterProtocol", "reqPushLoadStats", "Warning: Load stats exceeds minimum buffer size!\n" );
	}

	return this->buffer.send;
}

bool MasterProtocol::parseLoadingStats(
		const LoadStatsHeader& loadStatsHeader,
		ArrayMap< struct sockaddr_in, Latency > &slaveGetLatency,
		ArrayMap< struct sockaddr_in, Latency > &slaveSetLatency,
		std::set< struct sockaddr_in > &overloadedSlaveSet,
		char* buffer, uint32_t size )
{
	sockaddr_in addr;
	Latency *tempLatency = NULL;

	uint32_t recordSize = sizeof( uint32_t ) * 3 + sizeof( uint16_t );
	uint32_t slaveAddrSize = sizeof( uint32_t ) + sizeof ( uint16_t );

	// check if the all stats are received properly
	if ( size < ( loadStatsHeader.slaveGetCount + loadStatsHeader.slaveSetCount ) * recordSize +
			loadStatsHeader.slaveOverloadCount * slaveAddrSize )
		return false;

	for ( uint32_t i = 0; i < loadStatsHeader.slaveGetCount + loadStatsHeader.slaveSetCount; i++ ) {
		addr.sin_addr.s_addr = *( uint32_t * )( buffer );
		addr.sin_port = *( uint16_t * )( buffer + sizeof( uint32_t ) );
		tempLatency = new Latency();
		tempLatency->sec = ntohl( *( uint32_t * )( buffer + sizeof( uint32_t ) + sizeof( uint16_t ) ) );
		tempLatency->nsec = ntohl( *( uint32_t * )( buffer + sizeof( uint32_t ) * 2 + sizeof( uint16_t ) ) );

		if ( i < loadStatsHeader.slaveGetCount )
			slaveGetLatency.set( addr, tempLatency );
		else
			slaveSetLatency.set( addr, tempLatency );

		buffer += recordSize;
	}

	for  ( uint32_t i = 0; i < loadStatsHeader.slaveOverloadCount; i++ ) {
		addr.sin_addr.s_addr = *( uint32_t * )( buffer );
		addr.sin_port = *( uint16_t * )( buffer + sizeof( uint32_t ) );

		overloadedSlaveSet.insert( addr );

		buffer += slaveAddrSize;
	}

	return true;
}

char *MasterProtocol::reqDegradedLock( size_t &size, uint32_t id, uint32_t listId, uint32_t chunkId, char *key, uint8_t keySize ) {
	size = this->generateDegradedLockReqHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_DEGRADED_LOCK,
		id,
		listId,
		chunkId,
		keySize,
		key
	);
	return this->buffer.send;
}

char *MasterProtocol::reqRegisterSlave( size_t &size, uint32_t id, uint32_t addr, uint16_t port ) {
	size = this->generateAddressHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_REGISTER,
		id,
		addr,
		port
	);
	return this->buffer.send;
}

char *MasterProtocol::reqSet( size_t &size, uint32_t id, char *key, uint8_t keySize, char *value, uint32_t valueSize, char *buf ) {
	if ( ! buf ) buf = this->buffer.send;
	size = this->generateKeyValueHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_SET,
		id,
		keySize,
		key,
		valueSize,
		value,
		buf
	);
	return buf;
}

char *MasterProtocol::reqRemappingSetLock( size_t &size, uint32_t id, uint32_t listId, uint32_t chunkId, char *key, uint8_t keySize ) {
	size = this->generateRemappingLockHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_REMAPPING_LOCK,
		id,
		listId,
		chunkId,
		keySize,
		key
	);
	return this->buffer.send;
}

char *MasterProtocol::reqRemappingSet( size_t &size, uint32_t id, uint32_t listId, uint32_t chunkId, bool needsForwarding, char *key, uint8_t keySize, char *value, uint32_t valueSize, char *buf ) {
	if ( ! buf ) buf = this->buffer.send;
	size = this->generateRemappingSetHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_REMAPPING_SET,
		id,
		listId,
		chunkId,
		needsForwarding,
		keySize,
		key,
		valueSize,
		value,
		buf
	);
	return buf;
}

char *MasterProtocol::reqGet( size_t &size, uint32_t id, char *key, uint8_t keySize, bool isDegraded ) {
	size = this->generateKeyHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		isDegraded ? PROTO_OPCODE_DEGRADED_GET : PROTO_OPCODE_GET,
		id,
		keySize,
		key
	);
	return this->buffer.send;
}

char *MasterProtocol::reqUpdate( size_t &size, uint32_t id, char *key, uint8_t keySize, char *valueUpdate, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, bool isDegraded ) {
	size = this->generateKeyValueUpdateHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		isDegraded ? PROTO_OPCODE_DEGRADED_UPDATE : PROTO_OPCODE_UPDATE,
		id,
		keySize,
		key,
		valueUpdateOffset,
		valueUpdateSize,
		valueUpdate
	);
	return this->buffer.send;
}

char *MasterProtocol::reqDelete( size_t &size, uint32_t id, char *key, uint8_t keySize, bool isDegraded ) {
	size = this->generateKeyHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		isDegraded ? PROTO_OPCODE_DEGRADED_DELETE : PROTO_OPCODE_DELETE,
		id,
		keySize,
		key
	);
	return this->buffer.send;
}

char *MasterProtocol::resRegisterApplication( size_t &size, uint32_t id, bool success ) {
	size = this->generateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_APPLICATION,
		PROTO_OPCODE_REGISTER,
		0, // length
		id
	);
	return this->buffer.send;
}

char *MasterProtocol::resSet( size_t &size, uint32_t id, bool success, uint8_t keySize, char *key ) {
	size = this->generateKeyHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_APPLICATION,
		PROTO_OPCODE_SET,
		id,
		keySize,
		key
	);
	return this->buffer.send;
}

char *MasterProtocol::resGet( size_t &size, uint32_t id, bool success, uint8_t keySize, char *key, uint32_t valueSize, char *value ) {
	if ( success ) {
		size = this->generateKeyValueHeader(
			PROTO_MAGIC_RESPONSE_SUCCESS,
			PROTO_MAGIC_TO_APPLICATION,
			PROTO_OPCODE_GET,
			id,
			keySize,
			key,
			valueSize,
			value
		);
	} else {
		size = this->generateKeyHeader(
			PROTO_MAGIC_RESPONSE_FAILURE,
			PROTO_MAGIC_TO_APPLICATION,
			PROTO_OPCODE_GET,
			id,
			keySize,
			key
		);
	}
	return this->buffer.send;
}

char *MasterProtocol::resUpdate( size_t &size, uint32_t id, bool success, uint8_t keySize, char *key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize ) {
	size = this->generateKeyValueUpdateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_APPLICATION,
		PROTO_OPCODE_UPDATE,
		id,
		keySize, key,
		valueUpdateOffset, valueUpdateSize, 0
	);
	return this->buffer.send;
}

char *MasterProtocol::resDelete( size_t &size, uint32_t id, bool success, uint8_t keySize, char *key ) {
	size = this->generateKeyHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_APPLICATION,
		PROTO_OPCODE_DELETE,
		id,
		keySize,
		key
	);
	return this->buffer.send;
}
