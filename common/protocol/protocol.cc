#include <cstdlib>
#include <sys/types.h>
#include <signal.h>
#include <pthread.h>
#include "protocol.hh"
#include "../util/debug.hh"

Protocol::Protocol( Role role ) {
	this->buffer.size = 0;
	this->buffer.send = 0;
	this->buffer.recv = 0;
	switch( role ) {
		case ROLE_APPLICATION:
			this->from = PROTO_MAGIC_FROM_APPLICATION;
			this->to = PROTO_MAGIC_TO_APPLICATION;
			break;
		case ROLE_COORDINATOR:
			this->from = PROTO_MAGIC_FROM_COORDINATOR;
			this->to = PROTO_MAGIC_TO_COORDINATOR;
			break;
		case ROLE_CLIENT:
			this->from = PROTO_MAGIC_FROM_CLIENT;
			this->to = PROTO_MAGIC_TO_CLIENT;
			break;
		case ROLE_SERVER:
			this->from = PROTO_MAGIC_FROM_SERVER;
			this->to = PROTO_MAGIC_TO_SERVER;
			break;
		default:
			__ERROR__( "Protocol", "Protocol", "Unknown server role." );
	}
}

bool Protocol::init( size_t size ) {
	this->buffer.size = size;
	this->buffer.send = ( char * ) ::malloc( size );
	if ( ! this->buffer.send ) {
		__ERROR__( "Protocol", "init", "Cannot allocate memory." );
		return false;
	}

	this->buffer.recv = ( char * ) ::malloc( size );
	if ( ! this->buffer.recv ) {
		__ERROR__( "Protocol", "init", "Cannot allocate memory." );
		return false;
	}
	return true;
}

void Protocol::free() {
	if ( this->buffer.send )
		::free( this->buffer.send );
	if ( this->buffer.recv )
		::free( this->buffer.recv );
	this->buffer.size = 0;
	this->buffer.send = 0;
	this->buffer.recv = 0;
}

size_t Protocol::generateHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t length, uint16_t instanceId, uint32_t requestId, char *sendBuf, uint32_t requestTimestamp ) {
	size_t bytes = 0;
	if ( ! sendBuf ) sendBuf = this->buffer.send;

	sendBuf[ 0 ] = ( ( magic & 0x07 ) | ( this->from & 0x18 ) | ( to & 0x60 ) );
	sendBuf[ 1 ] = opcode & 0xFF;
	bytes = 2;
	sendBuf += 2;

	bytes += ProtocolUtil::write4Bytes( sendBuf, length );
	bytes += ProtocolUtil::write2Bytes( sendBuf, instanceId );
	bytes += ProtocolUtil::write4Bytes( sendBuf, requestId );
	bytes += ProtocolUtil::write4Bytes( sendBuf, requestTimestamp );
	return bytes;
}

bool Protocol::parseHeader( struct ProtocolHeader &header, char *buf, size_t size ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size < PROTO_HEADER_SIZE ) return false;

	header.magic  = buf[ 0 ] & 0x07;
	header.from   = buf[ 0 ] & 0x18;
	header.to     = buf[ 0 ] & 0x60;
	header.opcode = buf[ 1 ] & 0xFF;
	buf += 2;

	header.length     = ProtocolUtil::read4Bytes( buf );
	header.instanceId = ProtocolUtil::read2Bytes( buf );
	header.requestId  = ProtocolUtil::read4Bytes( buf );
	header.timestamp  = ProtocolUtil::read4Bytes( buf );

	switch( header.magic ) {
		case PROTO_MAGIC_HEARTBEAT:
		case PROTO_MAGIC_REQUEST:
		case PROTO_MAGIC_RESPONSE_SUCCESS:
		case PROTO_MAGIC_RESPONSE_FAILURE:
		case PROTO_MAGIC_ANNOUNCEMENT:
		case PROTO_MAGIC_LOADING_STATS:
		case PROTO_MAGIC_REMAPPING:
		case PROTO_MAGIC_ACKNOWLEDGEMENT:
			break;
		default:
			fprintf( stderr, "Error #1: (magic, from, to, opcode, length, instanceId, requestId) = (%x, %x, %x, %x, %u, %u, %u)\n", header.magic, header.from, header.to, header.opcode, header.length, header.instanceId, header.requestId );
			return false;
	}

	switch( header.from ) {
		case PROTO_MAGIC_FROM_APPLICATION:
		case PROTO_MAGIC_FROM_COORDINATOR:
		case PROTO_MAGIC_FROM_CLIENT:
		case PROTO_MAGIC_FROM_SERVER:
			break;
		default:
			fprintf( stderr, "Error #2: (magic, from, to, opcode, length, instanceId, requestId) = (%x, %x, %x, %x, %u, %u, %u)\n", header.magic, header.from, header.to, header.opcode, header.length, header.instanceId, header.requestId );
			return false;
	}

	if ( header.to != this->to ) {
		fprintf( stderr, "Error #3: (magic, from, to, opcode, length, instanceId, requestId) = (%x, %x, %x, %x, %u, %u, %u)\n", header.magic, header.from, header.to, header.opcode, header.length, header.instanceId, header.requestId );
		return false;
	}

	switch( header.opcode ) {
		case PROTO_OPCODE_REGISTER:
		case PROTO_OPCODE_SYNC:
		case PROTO_OPCODE_SERVER_CONNECTED:
		case PROTO_OPCODE_SEAL_CHUNKS:
		case PROTO_OPCODE_FLUSH_CHUNKS:
		case PROTO_OPCODE_RECONSTRUCTION:
		case PROTO_OPCODE_RECONSTRUCTION_UNSEALED:
		case PROTO_OPCODE_SYNC_META:
		case PROTO_OPCODE_RELEASE_DEGRADED_LOCKS:
		case PROTO_OPCODE_SERVER_RECONSTRUCTED:
		case PROTO_OPCODE_BACKUP_SERVER_PROMOTED:
		case PROTO_OPCODE_PARITY_MIGRATE:
		case PROTO_OPCODE_GET:
		case PROTO_OPCODE_SET:
		case PROTO_OPCODE_UPDATE:
		case PROTO_OPCODE_DELETE:
		case PROTO_OPCODE_UPDATE_CHECK:
		case PROTO_OPCODE_DELETE_CHECK:
		case PROTO_OPCODE_DEGRADED_GET:
		case PROTO_OPCODE_DEGRADED_UPDATE:
		case PROTO_OPCODE_DEGRADED_DELETE:
		case PROTO_OPCODE_DEGRADED_SET:
		case PROTO_OPCODE_DEGRADED_LOCK:
		case PROTO_OPCODE_ACK_METADATA:
		case PROTO_OPCODE_ACK_PARITY_DELTA:
		case PROTO_OPCODE_REVERT_DELTA:
		case PROTO_OPCODE_REMAPPING_LOCK:
		case PROTO_OPCODE_SEAL_CHUNK:
		case PROTO_OPCODE_UPDATE_CHUNK:
		case PROTO_OPCODE_DELETE_CHUNK:
		case PROTO_OPCODE_GET_CHUNK:
		case PROTO_OPCODE_SET_CHUNK:
		case PROTO_OPCODE_SET_CHUNK_UNSEALED:
		case PROTO_OPCODE_FORWARD_CHUNK:
		case PROTO_OPCODE_FORWARD_KEY:
		case PROTO_OPCODE_REMAPPED_UPDATE:
		case PROTO_OPCODE_REMAPPED_DELETE:
		case PROTO_OPCODE_BATCH_CHUNKS:
		case PROTO_OPCODE_BATCH_KEY_VALUES:
		case PROTO_OPCODE_UPDATE_CHUNK_CHECK:
		case PROTO_OPCODE_DELETE_CHUNK_CHECK:
			break;
		default:
			fprintf( stderr, "Error #4: (magic, from, to, opcode, length, instanceId, requestId) = (%x, %x, %x, %x, %u, %u, %u)\n", header.magic, header.from, header.to, header.opcode, header.length, header.instanceId, header.requestId );
			return false;
	}

	return true;
}

size_t Protocol::getSuggestedBufferSize( uint32_t keySize, uint32_t chunkSize, bool supportLargeObject ) {
	size_t ret = (
		PROTO_HEADER_SIZE +
		PROTO_KEY_VALUE_SIZE +
		keySize +
		( supportLargeObject ? 16777216 : chunkSize )
	);
	// Set ret = ceil( ret / 4096 ) * 4096
	if ( ret & 4095 ) { // 0xFFF
		ret >>= 12;
		ret += 1;
		ret <<= 12;
	}
	// Set ret = ret * 2
	ret <<= 1;
	if ( ret < PROTO_BUF_MIN_SIZE )
		ret = PROTO_BUF_MIN_SIZE;
	return ret;
}
