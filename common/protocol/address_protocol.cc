#include "protocol.hh"

size_t Protocol::generateAddressHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t addr, uint16_t port, char* buf ) {
	if ( ! buf ) buf = this->buffer.send;

	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_ADDRESS_SIZE, instanceId, requestId, buf );

	buf += bytes;

	// Already in network-byte order
	*( ( uint32_t * )( buf     ) ) = addr;
	*( ( uint16_t * )( buf + 4 ) ) = port;

	bytes += PROTO_ADDRESS_SIZE;

	return bytes;
}

bool Protocol::parseAddressHeader( size_t offset, uint32_t &addr, uint16_t &port, char *buf, size_t size ) {
	if ( size - offset < PROTO_ADDRESS_SIZE )
		return false;

	char *ptr = buf + offset;
	addr = *( ( uint32_t * )( ptr     ) );
	port = *( ( uint16_t * )( ptr + 4 ) );

	return true;
}

bool Protocol::parseAddressHeader( struct AddressHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseAddressHeader(
		offset,
		header.addr,
		header.port,
		buf, size
	);
}

size_t Protocol::generateSrcDstAddressHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t srcAddr, uint16_t srcPort, uint32_t dstAddr, uint16_t dstPort ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_ADDRESS_SIZE * 2, instanceId, requestId );

	// Already in network-byte order
	*( ( uint32_t * )( buf     ) ) = srcAddr;
	*( ( uint16_t * )( buf + 4 ) ) = srcPort;
	*( ( uint32_t * )( buf + 6 ) ) = dstAddr;
	*( ( uint16_t * )( buf + 10 ) ) = dstPort;

	bytes += PROTO_ADDRESS_SIZE * 2;

	return bytes;
}

bool Protocol::parseSrcDstAddressHeader( struct AddressHeader &srcHeader, struct AddressHeader &dstHeader, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	bool ret = this->parseAddressHeader(
		offset,
		srcHeader.addr,
		srcHeader.port,
		buf, size
	);
	return ret ? this->parseAddressHeader(
		offset + PROTO_ADDRESS_SIZE,
		dstHeader.addr,
		dstHeader.port,
		buf, size
	) : false;
}
