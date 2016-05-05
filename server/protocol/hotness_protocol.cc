#include "protocol.hh"

char *ServerProtocol::sendHotnessStats(
	size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t timestamp,
	std::vector<Metadata> &getList, std::vector<Metadata> &updateList,
	bool &isCompleted
) {
	char *buffer = this->buffer.send;
	uint32_t getCount = 0, updateCount = 0, metadataSize = sizeof( Metadata );
	uint32_t i = 0;

	size = this->generateHotnessStatsHeader(
		PROTO_MAGIC_LOADING_STATS,
		PROTO_MAGIC_TO_COORDINATOR,
		instanceId, requestId, 
		timestamp,
		getList.size(), updateList.size(),
		metadataSize
	);

	buffer += size;

	for ( i = 0; i < getList.size() && size + metadataSize < this->buffer.size ; i++, getCount++ ) {
		*( ( uint32_t * ) buffer                      ) = htonl( getList[ i ].listId );
		*( ( uint32_t * ) buffer + sizeof( uint32_t ) ) = htonl( getList[ i ].stripeId );
		*( ( uint32_t * ) buffer + sizeof( uint32_t ) ) = htonl( getList[ i ].chunkId );
		size += 3 * sizeof( uint32_t );
		buffer += 3 * sizeof( uint32_t );

	}

	for ( i = 0; i < updateList.size() && size + metadataSize < this->buffer.size ; i++, updateCount++ ) {
		*( ( uint32_t * ) buffer                      ) = htonl( updateList[ i ].listId );
		*( ( uint32_t * ) buffer + sizeof( uint32_t ) ) = htonl( updateList[ i ].stripeId );
		*( ( uint32_t * ) buffer + sizeof( uint32_t ) ) = htonl( updateList[ i ].chunkId );
		size += 3 * sizeof( uint32_t );
		buffer += 3 * sizeof( uint32_t );
	}

	if ( getCount > 0 ) 
		getList.erase( getList.begin(), getList.begin() + getCount );

	if ( updateCount > 0 ) 
		updateList.erase( updateList.begin(), updateList.begin() + updateCount );

	isCompleted = ( ! getList.empty() || ! updateList.empty() );

	// avoid message receiver's buffer
	if ( ! isCompleted ) {
		this->generateHotnessStatsHeader(
			PROTO_MAGIC_LOADING_STATS,
			PROTO_MAGIC_TO_COORDINATOR,
			instanceId, requestId, 
			timestamp,
			getCount, updateCount,
			metadataSize
		);
	}
		

	return buffer - size;
}
