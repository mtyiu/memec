#include "backup.hh"
#include "../../common/protocol/protocol.hh"
#include "../../common/util/debug.hh"

Backup::Backup() {
	LOCK_INIT( &this->lock );
}

void Backup::insert( uint8_t keySize, char *keyStr, uint8_t opcode, uint32_t timestamp, uint32_t listId, uint32_t stripeId, uint32_t chunkId ) {
	Key key;
	MetadataBackup metadataBackup;
	std::unordered_multimap<Key, MetadataBackup>::iterator it;

	key.dup( keySize, keyStr );

	metadataBackup.opcode = opcode;
	metadataBackup.timestamp = timestamp;
	metadataBackup.set( listId, stripeId, chunkId );

	std::pair<Key, MetadataBackup> p( key, metadataBackup );

	LOCK( &this->lock );
	this->ops.insert( p );
	UNLOCK( &this->lock );
}

size_t Backup::erase( uint32_t fromTimestamp, uint32_t toTimestamp ) {
	std::unordered_multimap<Key, MetadataBackup>::iterator it;
	bool isWrappedAround, selected;
	size_t ret = 0;

	LOCK( &this->lock );
	isWrappedAround = fromTimestamp > toTimestamp;
	for ( it = this->ops.begin(); it != this->ops.end();  ) {
		const MetadataBackup &metadataBackup = it->second;
		if ( isWrappedAround ) {
			selected = (
				metadataBackup.timestamp > fromTimestamp ||
				metadataBackup.timestamp < toTimestamp
			);
		} else {
			selected = (
				metadataBackup.timestamp > fromTimestamp &&
				metadataBackup.timestamp < toTimestamp
			);
		}
		if ( selected ) {
			Key key = it->first;
			it = this->ops.erase( it );
			key.free();
			ret++;
		} else {
			it++;
		}
	}
	// printf( "Number of metadata backup: (released) %lu; (remaining) %lu\n", ret, this->ops.size() );
	UNLOCK( &this->lock );

	return ret;
}

void Backup::print( FILE *f ) {
	std::unordered_multimap<Key, MetadataBackup>::iterator it;
	fprintf( f, "--------------------\n" );
	fprintf( f, "Number of metadata backup: %lu\n", this->ops.size() );
	for ( it = this->ops.begin(); it != this->ops.end(); it++ ) {
		const Key &key = it->first;
		MetadataBackup &metadataBackup = it->second;

		fprintf(
			f, "%.*s: [%10u: %s] (%u, %u, %u)\n",
			key.size, key.data,
			metadataBackup.timestamp,
			metadataBackup.opcode == PROTO_OPCODE_SET ? "SET" : "DELETE",
			metadataBackup.listId,
			metadataBackup.stripeId,
			metadataBackup.chunkId
		);
	}
	fprintf( f, "--------------------\n" );
}

Backup::~Backup() {
	std::unordered_multimap<Key, MetadataBackup>::iterator it;
	Key key;

	it = this->ops.begin();
	while ( it != this->ops.end() ) {
		key = it->first;
		key.free();
		it = this->ops.begin();
	}
}
