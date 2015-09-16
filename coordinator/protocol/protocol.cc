#include "protocol.hh"

char *CoordinatorProtocol::resRegisterMaster( size_t &size, bool success ) {
	size = this->generateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_REGISTER,
		0
	);
	return this->buffer.send;
}

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

bool CoordinatorProtocol::parseLoadingStats( 
		const LoadStatsHeader& loadStatsHeader, 
		ArrayMap< ServerAddr, Latency >& slaveGetLatency,
		ArrayMap< ServerAddr, Latency >& slaveSetLatency,
		char* buffer, uint32_t size )
{
	ServerAddr addr; 
	Latency *tempLatency;

	uint32_t recordSize = sizeof( uint32_t ) * 3 + sizeof( uint16_t );

	// check if the all stats are received properly
	if ( size < ( loadStatsHeader.slaveGetCount + loadStatsHeader.slaveSetCount ) * recordSize )
		return false;

	for ( uint32_t i = 0; i < loadStatsHeader.slaveGetCount + loadStatsHeader.slaveSetCount; i++ ) {
		addr.addr = ntohl( *( uint32_t * )( buffer ) );
		addr.port = ntohs( *( uint16_t * )( buffer + sizeof( uint32_t ) ) );
		tempLatency = new Latency();
		tempLatency->sec = ntohl( *( uint32_t * )( buffer + sizeof( uint32_t ) + sizeof( uint16_t ) ) );
		tempLatency->usec = ntohl( *( uint32_t * )( buffer + sizeof( uint32_t ) * 2 + sizeof( uint16_t ) ) );

		if ( i < loadStatsHeader.slaveGetCount )
			slaveGetLatency.set( addr, tempLatency );
		else
			slaveSetLatency.set( addr, tempLatency );
		
		buffer += recordSize;
	}

	return true;
}

char *CoordinatorProtocol::resRegisterSlave( size_t &size, bool success ) {
	size = this->generateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_REGISTER,
		0
	);
	return this->buffer.send;
}

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

char *CoordinatorProtocol::announceSlaveConnected( size_t &size, SlaveSocket *socket ) {
	size = this->generateAddressHeader(
		PROTO_MAGIC_ANNOUNCEMENT,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_SLAVE_CONNECTED,
		socket->listenAddr.addr,
		socket->listenAddr.port
	);
	return this->buffer.send;
}
