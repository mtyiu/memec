#include "protocol.hh"

char *CoordinatorProtocol::resMasterRegister( size_t &size ) {
	size = this->generateHeader(
		PROTO_MAGIC_RESPONSE_SUCCESS,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_REGISTER,
		0
	);
	return this->buffer.data;
}

char *CoordinatorProtocol::resMasterRegister( size_t &size, GlobalConfig &globalConfig, MasterConfig *masterConfig ) {
	size_t length[ 3 ];
	const char *serializedStrings[ 2 ];

	serializedStrings[ 0 ] = globalConfig.serialize( length[ 0 ] );
	if ( masterConfig )
		serializedStrings[ 1 ] = masterConfig->serialize( length[ 1 ] );
	else
		length[ 1 ] = 0;
	length[ 2 ] = sizeof( uint32_t );

	size = this->generateHeader(
		PROTO_MAGIC_RESPONSE_SUCCESS,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_REGISTER,
		length[ 0 ] + length[ 1 ] + length[ 2 ] * 2
	);

	*( ( uint32_t * )( this->buffer.data + size ) ) = htonl( length[ 0 ] );
	size += length[ 2 ];

	*( ( uint32_t * )( this->buffer.data + size ) ) = htonl( length[ 1 ] );
	size += length[ 2 ];

	memcpy( this->buffer.data + size, serializedStrings[ 0 ], length[ 0 ] );
	size += length[ 0 ];

	if ( length[ 1 ] ) {
		memcpy( this->buffer.data + size, serializedStrings[ 1 ], length[ 1 ] );
		size += length[ 1 ];
	}

	return this->buffer.data;
}

char *CoordinatorProtocol::resSlaveRegister( size_t &size ) {
	size = this->generateHeader(
		PROTO_MAGIC_RESPONSE_SUCCESS,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_REGISTER,
		0
	);
	return this->buffer.data;
}

char *CoordinatorProtocol::resSlaveRegister( size_t &size, GlobalConfig &globalConfig, SlaveConfig *slaveConfig ) {
	size_t length[ 3 ];
	const char *serializedStrings[ 2 ];

	serializedStrings[ 0 ] = globalConfig.serialize( length[ 0 ] );
	if ( slaveConfig )
		serializedStrings[ 1 ] = slaveConfig->serialize( length[ 1 ] );
	else
		length[ 1 ] = 0;
	length[ 2 ] = sizeof( uint32_t );

	size = this->generateHeader(
		PROTO_MAGIC_RESPONSE_SUCCESS,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_REGISTER,
		length[ 0 ] + length[ 1 ] + length[ 2 ] * 2
	);

	*( ( uint32_t * )( this->buffer.data + size ) ) = htonl( length[ 0 ] );
	size += length[ 2 ];

	*( ( uint32_t * )( this->buffer.data + size ) ) = htonl( length[ 1 ] );
	size += length[ 2 ];

	memcpy( this->buffer.data + size, serializedStrings[ 0 ], length[ 0 ] );
	size += length[ 0 ];

	if ( length[ 1 ] ) {
		memcpy( this->buffer.data + size, serializedStrings[ 1 ], length[ 1 ] );
		size += length[ 1 ];
	}

	return this->buffer.data;
}
