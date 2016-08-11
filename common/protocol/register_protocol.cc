#include "protocol.hh"

size_t Protocol::generateNamedPipeHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint8_t length, char *pathname, char* buf ) {
	if ( ! buf ) buf = this->buffer.send;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_NAMED_PIPE_SIZE + length, instanceId, requestId, buf );
	buf += bytes;
	// Already in network-byte order
	bytes += ProtocolUtil::write1Byte( buf, length );
	bytes += ProtocolUtil::write( buf, pathname, length );
	return bytes;
}

bool Protocol::parseNamedPipeHeader( struct NamedPipeHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_NAMED_PIPE_SIZE ) return false;
	char *ptr = buf + offset;
	header.length = ProtocolUtil::read1Byte( ptr );
	header.pathname = ptr;
	return true;
}
