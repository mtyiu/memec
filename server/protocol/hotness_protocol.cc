#include "protocol.hh"

char *ServerProtocol::sendHotnessStats(
	size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t timestamp,
	std::vector<KeyMetadata> &getList, std::vector<KeyMetadata> &updateList,
	uint32_t &getCount, uint32_t &updateCount,
	bool &isCompleted
) {
	char *buffer = this->buffer.send;
	uint32_t i = 0;
	Key key;

	getCount = 0;
	updateCount = 0;

	buffer += PROTO_HEADER_SIZE + PROTO_HOTNESS_STATS_SIZE;
	size = PROTO_HEADER_SIZE + PROTO_HOTNESS_STATS_SIZE;

	for ( i = 0; i < getList.size(); i++, getCount++ ) {
		key.set( getList[ i ].length, getList[ i ].ptr );
		if ( size + 1 + key.size > this->buffer.size ) {
			break;
		}
		*( ( uint8_t * ) ( buffer ) ) = key.size;
		memcpy( buffer + 1, key.data, key.size );

		buffer += 1 + key.size;
		size += 1 + key.size;
	}

	for ( i = 0; i < updateList.size(); i++, updateCount++ ) {
		key.set( updateList[ i ].length, updateList[ i ].ptr );
		if ( size + 1 + key.size > this->buffer.size ) {
			break;
		}
		*( ( uint8_t * ) ( buffer ) ) = key.size;
		memcpy( buffer + 1, key.data, key.size );
		buffer += 1 + key.size;
		size += 1 + key.size;
	}

	isCompleted = ( getList.size() - getCount == 0 && updateList.size() - updateCount == 0 );

	// avoid message receiver's buffer
	this->generateHotnessStatsHeader(
		PROTO_MAGIC_LOADING_STATS,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_SYNC_HOTNESS_STATS,
		instanceId, requestId, 
		timestamp,
		getCount, updateCount,
		size - PROTO_HEADER_SIZE - PROTO_HOTNESS_STATS_SIZE
	);

	return buffer - size;
}
