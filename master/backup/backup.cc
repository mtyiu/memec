#include "backup.hh"
#include "../../common/protocol/protocol.hh"
#include "../../common/util/debug.hh"

Backup::Backup() {
	LOCK_INIT( &this->lock );
}

void Backup::insert( uint8_t keySize, char *keyStr, uint8_t opcode, uint32_t listId, uint32_t stripeId, uint32_t chunkId ) {
	Key key;
	std::unordered_map<Key, OpMetadata>::iterator it;

	key.set( keySize, keyStr );

	LOCK( &this->lock );
	it = this->ops.find( key );
	if ( it == this->ops.end() ) {
		OpMetadata opMetadata;
		opMetadata.opcode = opcode;
		opMetadata.set( listId, stripeId, chunkId );
		key.dup();
		this->ops[ key ] = opMetadata;
	} else {
		OpMetadata &opMetadata = it->second;
		switch( opcode ) {
			case PROTO_OPCODE_SET:
				if ( listId   == opMetadata.listId &&
				     stripeId == opMetadata.stripeId &&
				     chunkId  == opMetadata.chunkId ) {
					// Nothing to do
				} else {
					__ERROR__( "Backup", "insert", "[SET] Metadata mismatch: (%u, %u, %u) vs. (%u, %u, %u).", listId, stripeId, chunkId, opMetadata.listId, opMetadata.stripeId, opMetadata.chunkId );
				}
				break;
			case PROTO_OPCODE_DELETE:
				if ( listId   == opMetadata.listId &&
				     stripeId == opMetadata.stripeId &&
				     chunkId  == opMetadata.chunkId ) {
					// Remove this entry
				} else {
					__ERROR__( "Backup", "insert", "[DELETE] Metadata mismatch: (%u, %u, %u) vs. (%u, %u, %u).", listId, stripeId, chunkId, opMetadata.listId, opMetadata.stripeId, opMetadata.chunkId );
				}
				break;
		}
	}
	UNLOCK( &this->lock );
}

Backup::~Backup() {
	std::unordered_map<Key, OpMetadata>::iterator it;
	Key key;

	it = this->ops.begin();
	while ( it != this->ops.end() ) {
		key = it->first;
		key.free();
		it = this->ops.begin();
	}
}
