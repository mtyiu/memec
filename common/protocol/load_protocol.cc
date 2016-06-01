#include "protocol.hh"

size_t Protocol::generateLoadStatsHeader( uint8_t magic, uint8_t to, uint16_t instanceId, uint32_t requestId, uint32_t serverGetCount, uint32_t serverSetCount, uint32_t serverOverloadCount, uint32_t recordSize, uint32_t serverAddrSize ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, 0, PROTO_LOAD_STATS_SIZE + ( serverGetCount + serverSetCount ) * recordSize + ( serverOverloadCount * serverAddrSize ), instanceId, requestId );
	bytes += ProtocolUtil::write4Bytes( buf, serverGetCount      );
	bytes += ProtocolUtil::write4Bytes( buf, serverSetCount      );
	bytes += ProtocolUtil::write4Bytes( buf, serverOverloadCount );
	return bytes;
}

bool Protocol::parseLoadStatsHeader( struct LoadStatsHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_LOAD_STATS_SIZE ) return false;
	char *ptr = buf + offset;
	header.serverGetCount      = ProtocolUtil::read4Bytes( ptr );
	header.serverSetCount      = ProtocolUtil::read4Bytes( ptr );
	header.serverOverloadCount = ProtocolUtil::read4Bytes( ptr );
	return true;
}
