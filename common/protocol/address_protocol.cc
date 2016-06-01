#include "protocol.hh"

size_t Protocol::generateAddressHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t addr, uint16_t port, char* buf ) {
	if ( ! buf ) buf = this->buffer.send;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_ADDRESS_SIZE, instanceId, requestId, buf );
	buf += bytes;
	// Already in network-byte order
	bytes += ProtocolUtil::write4Bytes( buf, addr, false );
	bytes += ProtocolUtil::write2Bytes( buf, port, false );
	return bytes;
}

bool Protocol::parseAddressHeader( struct AddressHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_ADDRESS_SIZE ) return false;
	char *ptr = buf + offset;
	header.addr = ProtocolUtil::read4Bytes( ptr, false );
	header.port = ProtocolUtil::read2Bytes( ptr, false );
	return true;
}

size_t Protocol::generateSrcDstAddressHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t srcAddr, uint16_t srcPort, uint32_t dstAddr, uint16_t dstPort ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_ADDRESS_SIZE * 2, instanceId, requestId );
	// Already in network-byte order
	bytes += ProtocolUtil::write4Bytes( buf, srcAddr, false );
	bytes += ProtocolUtil::write2Bytes( buf, srcPort, false );
	bytes += ProtocolUtil::write4Bytes( buf, dstAddr, false );
	bytes += ProtocolUtil::write2Bytes( buf, dstPort, false );
	return bytes;
}

bool Protocol::parseSrcDstAddressHeader( struct AddressHeader &srcHeader, struct AddressHeader &dstHeader, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_ADDRESS_SIZE * 2 ) return false;
	char *ptr = buf + offset;
	srcHeader.addr = ProtocolUtil::read4Bytes( ptr, false );
	srcHeader.port = ProtocolUtil::read2Bytes( ptr, false );
	dstHeader.addr = ProtocolUtil::read4Bytes( ptr, false );
	dstHeader.port = ProtocolUtil::read2Bytes( ptr, false );
	return true;
}
