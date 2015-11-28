#include "protocol.hh"
#include "../../common/ds/sockaddr_in.hh"

char *CoordinatorProtocol::resRegisterMaster( size_t &size, uint32_t id, bool success ) {
	size = this->generateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_REGISTER,
		0, // length
		id
	);
	return this->buffer.send;
}

/*
char *CoordinatorProtocol::resRegisterMaster( size_t &size, GlobalConfig &globalConfig, MasterConfig *masterConfig ) {
	size_t length[ 3 ];
	const char *serializedStrings[ 2 ];

	serializedStrings[ 0 ] = globalConfig.serialize( length[ 0 ] );
	if ( masterConfig )
		serializedStrings[ 1 ] = masterConfig->serialize( length[ 1 ] );
	else {
		serializedStrings[ 1 ] = 0;
		length[ 1 ] = 0;
	}
	length[ 2 ] = sizeof( uint32_t );

	size = this->generateHeader(
		PROTO_MAGIC_RESPONSE_SUCCESS,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_REGISTER,
		length[ 0 ] + length[ 1 ] + length[ 2 ] * 2
	);

	*( ( uint32_t * )( this->buffer.send + size ) ) = htonl( length[ 0 ] );
	size += length[ 2 ];

	*( ( uint32_t * )( this->buffer.send + size ) ) = htonl( length[ 1 ] );
	size += length[ 2 ];

	memcpy( this->buffer.send + size, serializedStrings[ 0 ], length[ 0 ] );
	size += length[ 0 ];

	if ( length[ 1 ] ) {
		memcpy( this->buffer.send + size, serializedStrings[ 1 ], length[ 1 ] );
		size += length[ 1 ];
	}

	return this->buffer.send;
}
*/

// TODO put into common/ as this is same as  MasterProtocol::reqPushLoadStats
char *CoordinatorProtocol::reqPushLoadStats(
		size_t &size, uint32_t id,
		ArrayMap< struct sockaddr_in, Latency > *slaveGetLatency,
		ArrayMap< struct sockaddr_in, Latency > *slaveSetLatency,
		std::set< struct sockaddr_in > *overloadedSlaveSet )
{

	size = this->generateLoadStatsHeader(
		PROTO_MAGIC_LOADING_STATS,
		PROTO_MAGIC_TO_MASTER,
		id,
		slaveGetLatency->size(),
		slaveSetLatency->size(),
		overloadedSlaveSet->size(),
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

		//fprintf( stderr, "stats send %u:%hu %usec %unsec\n", ntohl( addr ), ntohs( port ), sec, nsec );
		*( ( uint32_t * )( this->buffer.send + size ) ) = addr; // htonl( addr );
		size += sizeof( addr );
		*( ( uint16_t * )( this->buffer.send + size ) ) = port; //htons( port );
		size += sizeof( port );
		*( ( uint32_t * )( this->buffer.send + size ) ) = htonl( sec );
		size += sizeof( sec );
		*( ( uint32_t * )( this->buffer.send + size ) ) = htonl( nsec );
		size += sizeof( nsec );
	}

#undef SET_FIELDS_VAR

	for ( std::set<struct sockaddr_in>::iterator it = overloadedSlaveSet->begin(); it != overloadedSlaveSet->end(); it++ ) {
		addr = (*it).sin_addr.s_addr;
		port = (*it).sin_port;
		*( ( uint32_t * )( this->buffer.send + size ) ) = addr; // htonl( addr );
		size += sizeof( addr );
		*( ( uint16_t * )( this->buffer.send + size ) ) = port; //htons( port );
		size += sizeof( port );
	}

	if ( size > PROTO_BUF_MIN_SIZE ) {
		__DEBUG__( CYAN, "CoordinatorProtocol", "reqPushLoadStats", "Warning: Load stats exceeds minimum buffer size!\n" );
	}

	return this->buffer.send;
}

// TODO put into common/ as this is same as  MasterProtocol::parseLoadingStats
bool CoordinatorProtocol::parseLoadingStats(
		const LoadStatsHeader& loadStatsHeader,
		ArrayMap< struct sockaddr_in, Latency >& slaveGetLatency,
		ArrayMap< struct sockaddr_in, Latency >& slaveSetLatency,
		char* buffer, uint32_t size )
{
	struct sockaddr_in addr;
	Latency *tempLatency = NULL;

	uint32_t recordSize = sizeof( uint32_t ) * 3 + sizeof( uint16_t );

	// check if the all stats are received properly
	if ( size < ( loadStatsHeader.slaveGetCount + loadStatsHeader.slaveSetCount ) * recordSize )
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

	return true;
}

bool CoordinatorProtocol::parseRemappingLockHeader( struct RemappingLockHeader &header, char *buf, size_t size, std::vector<uint32_t> *remapList, size_t offset ) {
	bool ret = Protocol::parseRemappingLockHeader( header, buf, size, offset );
	char *payload = buf + offset + PROTO_REMAPPING_LOCK_SIZE + header.keySize;
	uint32_t listCount = payload[ 0 ];
	payload += 1;
	for ( uint32_t i = 0; i < listCount; i++, payload += 4 ) {
		remapList->push_back( ntohl( *( ( uint32_t * )( payload ) ) ) );
	}
	return ret;
}

char *CoordinatorProtocol::forwardRemappingRecords( size_t &size, uint32_t id, char* message ) {
	size_t headerSize = this->generateHeader(
		PROTO_MAGIC_REMAPPING,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_SYNC,
		size,
		id
	);
	memcpy( this->buffer.send + headerSize, message, size );
	size += headerSize;
	return this->buffer.send;
}

char *CoordinatorProtocol::reqSyncRemappingRecord( size_t &size, uint32_t id, std::unordered_map<Key, RemappingRecord> &remappingRecords, LOCK_T* lock, bool &isLast, char* buffer ) {
	size_t remapCount = 0;
	if ( ! buffer ) buffer = this->buffer.send;
	size = this->generateRemappingRecordMessage(
		PROTO_MAGIC_REMAPPING,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_SYNC,
		id,
		lock,
		remappingRecords,
		remapCount,
		buffer
	);
	isLast = ( remapCount == 0 );

	return buffer;
}

char *CoordinatorProtocol::reqSyncRemappedParity( size_t &size, uint32_t id, struct sockaddr_in target, char* buffer ) {
	if ( ! buffer ) buffer = this->buffer.send;
	size = this->generateAddressHeader(
		PROTO_MAGIC_REMAPPING,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_PARITY_MIGRATE,
		id,
		target.sin_addr.s_addr, 
		target.sin_port,
		buffer
	);
	return buffer;
}

char *CoordinatorProtocol::resDegradedLock( size_t &size, uint32_t id, bool isLocked, bool isSealed, uint8_t keySize, char *key, uint32_t srcListId, uint32_t srcStripeId, uint32_t srcChunkId, uint32_t dstListId, uint32_t dstChunkId ) {
	size = this->generateDegradedLockResHeader(
		isLocked ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_DEGRADED_LOCK,
		id,
		isLocked,
		isSealed,
		keySize, key,
		srcListId, srcStripeId, srcChunkId,
		dstListId, dstChunkId
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::resDegradedLock( size_t &size, uint32_t id, uint8_t keySize, char *key, uint32_t srcListId, uint32_t srcChunkId, uint32_t dstListId, uint32_t dstChunkId ) {
	size = this->generateDegradedLockResHeader(
		PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_DEGRADED_LOCK,
		id,
		keySize, key,
		srcListId, srcChunkId,
		dstListId, dstChunkId
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::resDegradedLock( size_t &size, uint32_t id, bool exist, uint8_t keySize, char *key, uint32_t listId, uint32_t chunkId ) {
	size = this->generateDegradedLockResHeader(
		PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_DEGRADED_LOCK,
		id,
		exist,
		keySize, key,
		listId, chunkId
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::resRegisterSlave( size_t &size, uint32_t id, bool success ) {
	size = this->generateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_REGISTER,
		0, // length
		id
	);
	return this->buffer.send;
}

/*
char *CoordinatorProtocol::resRegisterSlave( size_t &size, GlobalConfig &globalConfig, SlaveConfig *slaveConfig ) {
	size_t length[ 3 ];
	const char *serializedStrings[ 2 ];

	serializedStrings[ 0 ] = globalConfig.serialize( length[ 0 ] );
	if ( slaveConfig )
		serializedStrings[ 1 ] = slaveConfig->serialize( length[ 1 ] );
	else {
		serializedStrings[ 1 ] = 0;
		length[ 1 ] = 0;
	}
	length[ 2 ] = sizeof( uint32_t );

	size = this->generateHeader(
		PROTO_MAGIC_RESPONSE_SUCCESS,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_REGISTER,
		length[ 0 ] + length[ 1 ] + length[ 2 ] * 2
	);

	*( ( uint32_t * )( this->buffer.send + size ) ) = htonl( length[ 0 ] );
	size += length[ 2 ];

	*( ( uint32_t * )( this->buffer.send + size ) ) = htonl( length[ 1 ] );
	size += length[ 2 ];

	memcpy( this->buffer.send + size, serializedStrings[ 0 ], length[ 0 ] );
	size += length[ 0 ];

	if ( length[ 1 ] ) {
		memcpy( this->buffer.send + size, serializedStrings[ 1 ], length[ 1 ] );
		size += length[ 1 ];
	}

	return this->buffer.send;
}
*/

char *CoordinatorProtocol::announceSlaveConnected( size_t &size, uint32_t id, SlaveSocket *socket ) {
	ServerAddr addr = socket->getServerAddr();
	size = this->generateAddressHeader(
		PROTO_MAGIC_ANNOUNCEMENT,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_SLAVE_CONNECTED,
		id,
		addr.addr,
		addr.port
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::announceSlaveReconstructed( size_t &size, uint32_t id, SlaveSocket *srcSocket, SlaveSocket *dstSocket ) {
	ServerAddr srcAddr = srcSocket->getServerAddr(), dstAddr = dstSocket->getServerAddr();
	size = this->generateSrcDstAddressHeader(
		PROTO_MAGIC_ANNOUNCEMENT,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_SLAVE_RECONSTRUCTED,
		id,
		srcAddr.addr,
		srcAddr.port,
		dstAddr.addr,
		dstAddr.port
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::reqSealChunks( size_t &size, uint32_t id ) {
	size = this->generateHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_SEAL_CHUNKS,
		0, // length
		id
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::reqFlushChunks( size_t &size, uint32_t id ) {
	size = this->generateHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_FLUSH_CHUNKS,
		0, // length
		id
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::reqSyncMeta( size_t &size, uint32_t id ) {
	size = this->generateHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_SYNC_META,
		0, // length
		id
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::reqReleaseDegradedLock( size_t &size, uint32_t id, std::vector<Metadata> &chunks, bool &isCompleted ) {
	size = this->generateDegradedReleaseReqHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_RELEASE_DEGRADED_LOCKS,
		id,
		chunks,
		isCompleted
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::resRemappingSetLock( size_t &size, uint32_t id, bool success, uint32_t listId, uint32_t chunkId, bool isRemapped, uint8_t keySize, char *key, uint32_t sockfd ) {
	size = this->generateRemappingLockHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_REMAPPING_LOCK,
		id,
		listId,
		chunkId,
		isRemapped,
		keySize,
		key,
		sockfd
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::reqRecovery( size_t &size, uint32_t id, uint32_t listId, uint32_t chunkId, std::unordered_set<uint32_t> &stripeIds, std::unordered_set<uint32_t>::iterator &it, uint32_t numChunks, bool &isCompleted ) {
	size = this->generateRecoveryHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_RECOVERY,
		id,
		listId,
		chunkId,
		stripeIds,
		it,
		numChunks,
		isCompleted
	);
	return this->buffer.send;
}
