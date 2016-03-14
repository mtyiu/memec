#include "protocol.hh"
#include "../../common/util/debug.hh"

char *ClientProtocol::reqPushLoadStats(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		ArrayMap< struct sockaddr_in, Latency > *serverGetLatency,
		ArrayMap< struct sockaddr_in, Latency > *serverSetLatency )
{
	// -- common/protocol/load_protocol.cc --
	size = this->generateLoadStatsHeader(
		PROTO_MAGIC_LOADING_STATS,
		PROTO_MAGIC_TO_COORDINATOR,
		instanceId, requestId,
		serverGetLatency->size(),
		serverSetLatency->size(),
		0,
		sizeof( uint32_t ) * 3 + sizeof( uint16_t ),
		sizeof( uint32_t ) + sizeof( uint16_t )
	);

	// TODO only send stats of most heavily loaded server in case buffer overflows

	uint32_t addr, sec, nsec;
	uint16_t port;

#define SET_FIELDS_VAR( _SRC_ ) \
	addr = _SRC_->keys[ idx ].sin_addr.s_addr; \
	port = _SRC_->keys[ idx ].sin_port; \
	sec = _SRC_->values[ idx ]->sec; \
	nsec = _SRC_->values[ idx ]->nsec; \

	for ( uint32_t i = 0; i < serverGetLatency->size() + serverSetLatency->size(); i++ ) {
		uint32_t idx = i;
		// serialize the loading stats
		if ( i < serverGetLatency->size() ) {
			SET_FIELDS_VAR( serverGetLatency );
		} else {
			idx = i - serverGetLatency->size();
			SET_FIELDS_VAR( serverSetLatency );
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
		__DEBUG__( CYAN, "ClientProtocol", "reqPushLoadStats", "Warning: Load stats exceeds minimum buffer size!\n" );
	}

	return this->buffer.send;
}

bool ClientProtocol::parseLoadingStats(
		const LoadStatsHeader& loadStatsHeader,
		ArrayMap< struct sockaddr_in, Latency > &serverGetLatency,
		ArrayMap< struct sockaddr_in, Latency > &serverSetLatency,
		std::set< struct sockaddr_in > &overloadedServerSet,
		char* buffer, uint32_t size )
{
	sockaddr_in addr;
	Latency *tempLatency = NULL;

	uint32_t recordSize = sizeof( uint32_t ) * 3 + sizeof( uint16_t );
	uint32_t serverAddrSize = sizeof( uint32_t ) + sizeof ( uint16_t );

	// check if the all stats are received properly
	if ( size < ( loadStatsHeader.serverGetCount + loadStatsHeader.serverSetCount ) * recordSize +
			loadStatsHeader.serverOverloadCount * serverAddrSize )
		return false;

	for ( uint32_t i = 0; i < loadStatsHeader.serverGetCount + loadStatsHeader.serverSetCount; i++ ) {
		addr.sin_addr.s_addr = *( uint32_t * )( buffer );
		addr.sin_port = *( uint16_t * )( buffer + sizeof( uint32_t ) );
		tempLatency = new Latency();
		tempLatency->sec = ntohl( *( uint32_t * )( buffer + sizeof( uint32_t ) + sizeof( uint16_t ) ) );
		tempLatency->nsec = ntohl( *( uint32_t * )( buffer + sizeof( uint32_t ) * 2 + sizeof( uint16_t ) ) );

		if ( i < loadStatsHeader.serverGetCount )
			serverGetLatency.set( addr, tempLatency );
		else
			serverSetLatency.set( addr, tempLatency );

		buffer += recordSize;
	}

	for  ( uint32_t i = 0; i < loadStatsHeader.serverOverloadCount; i++ ) {
		addr.sin_addr.s_addr = *( uint32_t * )( buffer );
		addr.sin_port = *( uint16_t * )( buffer + sizeof( uint32_t ) );

		overloadedServerSet.insert( addr );

		buffer += serverAddrSize;
	}

	return true;
}
