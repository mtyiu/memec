#include "protocol.hh"

size_t Protocol::generateNamedPipeHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint8_t readLength, uint8_t writeLength, char *readPathname, char *writePathname, char* buf ) {
	if ( ! buf ) buf = this->buffer.send;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_NAMED_PIPE_SIZE + readLength + writeLength, instanceId, requestId, buf );
	buf += bytes;
	// Already in network-byte order
	bytes += ProtocolUtil::write1Byte( buf, readLength );
	bytes += ProtocolUtil::write1Byte( buf, writeLength );
	bytes += ProtocolUtil::write( buf, readPathname, readLength );
	bytes += ProtocolUtil::write( buf, writePathname, writeLength );
	return bytes;
}

bool Protocol::parseNamedPipeHeader( struct NamedPipeHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_NAMED_PIPE_SIZE ) return false;
	char *ptr = buf + offset;
	header.readLength = ProtocolUtil::read1Byte( ptr );
	header.writeLength = ProtocolUtil::read1Byte( ptr );
	header.readPathname = ptr;
	header.writePathname = ptr + header.readLength;
	return true;
}
