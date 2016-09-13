#include "protocol.hh"

size_t Protocol::generateNewServerHeader(
	uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
	uint8_t length, uint32_t addr, uint16_t port, char *name,
	char *buf
) {
	if ( ! buf ) buf = this->buffer.send;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_NEW_SERVER_HEADER + length,
		instanceId, requestId, buf
	);
	buf += bytes;

	bytes += ProtocolUtil::write1Byte ( buf, length );
	bytes += ProtocolUtil::write4Bytes( buf, addr, false );
	bytes += ProtocolUtil::write2Bytes( buf, port, false );
	bytes += ProtocolUtil::write( buf, name, length );

	return bytes;
}

bool Protocol::parseNewServerHeader( struct NewServerHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_NEW_SERVER_HEADER ) return false;
	char *ptr = buf + offset;
	header.length = ProtocolUtil::read1Byte ( ptr );
	header.addr   = ProtocolUtil::read4Bytes( ptr, false );
	header.port   = ProtocolUtil::read2Bytes( ptr, false );

	if ( size - offset < ( size_t ) PROTO_NEW_SERVER_HEADER + header.length )
		return false;

	header.name = ptr;
	return true;
}

size_t Protocol::generateStripeListScalingHeader(
	uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
	bool isMigrating, uint32_t numServers, uint32_t numLists, uint32_t n, uint32_t k,
	std::vector<struct StripeListPartition> &lists,
	char* buf
) {
	if ( ! buf ) buf = this->buffer.send;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_STRIPE_LIST_SCALING_SIZE + ( PROTO_STRIPE_LIST_PARTITION_SIZE + n * sizeof( uint8_t ) ) * numLists,
		instanceId, requestId, buf
	);
	buf += bytes;

	bytes += ProtocolUtil::write1Byte ( buf, isMigrating );
	bytes += ProtocolUtil::write4Bytes( buf, numServers );
	bytes += ProtocolUtil::write4Bytes( buf, numLists );
	bytes += ProtocolUtil::write4Bytes( buf, n );
	bytes += ProtocolUtil::write4Bytes( buf, k );

	for ( uint32_t i = 0; i < numLists; i++ ) {
		bytes += ProtocolUtil::write1Byte( buf, lists[ i ].listId );
		bytes += ProtocolUtil::write4Bytes( buf, lists[ i ].partitionFrom );
		bytes += ProtocolUtil::write4Bytes( buf, lists[ i ].partitionTo );

		for ( uint32_t j = 0; j < n; j++ ) {
			bytes += ProtocolUtil::write1Byte( buf, lists[ i ].indices[ j ] );
		}
	}

	return bytes;
}

bool Protocol::parseStripeListScalingHeader( struct StripeListScalingHeader &header, size_t &next, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_STRIPE_LIST_SCALING_SIZE ) return false;
	char *ptr = buf + offset;
	header.isMigrating = ProtocolUtil::read1Byte ( ptr );
	header.numServers  = ProtocolUtil::read4Bytes( ptr );
	header.numLists    = ProtocolUtil::read4Bytes( ptr );
	header.n           = ProtocolUtil::read4Bytes( ptr );
	header.k           = ProtocolUtil::read4Bytes( ptr );

	next = offset + PROTO_STRIPE_LIST_SCALING_SIZE;

	return ( ! ( size - offset < ( size_t ) PROTO_STRIPE_LIST_SCALING_SIZE + ( PROTO_STRIPE_LIST_PARTITION_SIZE + header.n * sizeof( uint8_t ) ) * header.numLists ) );
}

bool Protocol::parseStripeListPartitionHeader( struct StripeListPartitionHeader &header, size_t &next, uint32_t n, uint32_t k, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_STRIPE_LIST_PARTITION_SIZE + n * sizeof( uint8_t ) ) return false;
	char *ptr = buf + offset;
	header.listId        = ProtocolUtil::read1Byte( ptr );
	header.partitionFrom = ProtocolUtil::read4Bytes( ptr );
	header.partitionTo   = ProtocolUtil::read4Bytes( ptr );
	header.data          = ( uint8_t * ) ptr;
	header.parity        = ( uint8_t * ) ptr + k;

	next = offset + PROTO_STRIPE_LIST_PARTITION_SIZE + n * sizeof( uint8_t );

	return true;
}

size_t Protocol::generateScalingMigrationHeader(
	uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
	uint32_t count, char *buf
) {
	if ( ! buf ) buf = this->buffer.send;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_SCALING_MIGRATION_SIZE,
		instanceId, requestId, buf
	);
	buf += bytes;
	bytes += ProtocolUtil::write4Bytes( buf, count );
	return bytes;
}

bool Protocol::parseScalingMigrationHeader( struct ScalingMigrationHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_SCALING_MIGRATION_SIZE ) return false;
	char *ptr = buf + offset;
	header.count = ProtocolUtil::read4Bytes( ptr );
	return true;
}
