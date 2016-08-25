#include "protocol.hh"
#include "../../common/util/debug.hh"
#include "../../common/ds/sockaddr_in.hh"

// TODO put into common/ as this is same as  ClientProtocol::reqPushLoadStats
char *CoordinatorProtocol::reqPushLoadStats(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		ArrayMap< struct sockaddr_in, Latency > *serverGetLatency,
		ArrayMap< struct sockaddr_in, Latency > *serverSetLatency,
		std::set< struct sockaddr_in > *overloadedServerSet )
{
	// -- common/protocol/load_protocol.cc --
	size = this->generateLoadStatsHeader(
		PROTO_MAGIC_LOADING_STATS,
		PROTO_MAGIC_TO_CLIENT,
		instanceId, requestId,
		serverGetLatency->size(),
		serverSetLatency->size(),
		overloadedServerSet->size(),
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

	for ( std::set<struct sockaddr_in>::iterator it = overloadedServerSet->begin(); it != overloadedServerSet->end(); it++ ) {
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

// TODO put into common/ as this is same as  ClientProtocol::parseLoadingStats
bool CoordinatorProtocol::parseLoadingStats(
		const LoadStatsHeader& loadStatsHeader,
		ArrayMap< struct sockaddr_in, Latency >& serverGetLatency,
		ArrayMap< struct sockaddr_in, Latency >& serverSetLatency,
		char* buffer, uint32_t size )
{
	struct sockaddr_in addr;
	Latency *tempLatency = NULL;

	uint32_t recordSize = sizeof( uint32_t ) * 3 + sizeof( uint16_t );

	// check if the all stats are received properly
	if ( size < ( loadStatsHeader.serverGetCount + loadStatsHeader.serverSetCount ) * recordSize )
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

	return true;
}

char *CoordinatorProtocol::announceServerReconstructed( size_t &size, uint16_t instanceId, uint32_t requestId, ServerSocket *srcSocket, ServerSocket *dstSocket, bool toServer ) {
	// -- common/protocol/address_protocol.cc --
	ServerAddr srcAddr = srcSocket->getServerAddr(), dstAddr = dstSocket->getServerAddr();
	size = this->generateSrcDstAddressHeader(
		PROTO_MAGIC_ANNOUNCEMENT,
		toServer ? PROTO_MAGIC_TO_SERVER : PROTO_MAGIC_TO_CLIENT,
		PROTO_OPCODE_SERVER_RECONSTRUCTED,
		instanceId, requestId,
		srcAddr.addr,
		srcAddr.port,
		dstAddr.addr,
		dstAddr.port
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::addNewServer( size_t &size, uint16_t instanceId, uint32_t requestId, char *name, uint8_t nameLen, ServerSocket *socket, bool toServer ) {
	struct sockaddr_in addr = socket->getAddr();
	size = this->generateNewServerHeader(
		PROTO_MAGIC_REQUEST,
		toServer ? PROTO_MAGIC_TO_SERVER : PROTO_MAGIC_TO_CLIENT,
		PROTO_OPCODE_ADD_NEW_SERVER,
		instanceId, requestId,
		nameLen, addr.sin_addr.s_addr, addr.sin_port, name
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::updateStripeList( size_t &size, uint16_t instanceId, uint32_t requestId, StripeList<ServerSocket> *stripeList, bool isMigrating, bool toServer ) {
	uint32_t numServers = 0, numLists = 0, n = 0, k = 0;
	std::vector<struct StripeListPartition> lists = stripeList->exportAll( numServers, numLists, n, k, isMigrating );
	size = this->generateStripeListScalingHeader(
		PROTO_MAGIC_REQUEST,
		toServer ? PROTO_MAGIC_TO_SERVER : PROTO_MAGIC_TO_CLIENT,
		PROTO_OPCODE_STRIPE_LIST_UPDATE,
		instanceId, requestId,
		isMigrating, numServers, numLists, n, k, lists
	);
	return this->buffer.send;
}
