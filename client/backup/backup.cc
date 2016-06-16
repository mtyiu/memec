#include "backup.hh"
#include "../../common/protocol/protocol.hh"
#include "../../common/util/debug.hh"

Backup::Backup() {
	LOCK_INIT( &this->lock );
}

void Backup::insert( uint8_t keySize, char *keyStr, bool isLarge, uint8_t opcode, uint32_t timestamp, uint32_t listId, uint32_t stripeId, uint32_t chunkId ) {
	Key key;
	MetadataBackup metadataBackup;
	std::unordered_map<Key, MetadataBackup>::iterator it;

	key.set( keySize, keyStr, 0, isLarge );
	key.dup( 0, 0, 0, isLarge );

	metadataBackup.opcode = opcode;
	metadataBackup.timestamp = timestamp;
	metadataBackup.set( listId, stripeId, chunkId );

	std::pair<Key, MetadataBackup> p( key, metadataBackup );

	LOCK( &this->lock );
	it = this->ops.find( key );
	if ( it == this->ops.end() ) {
		this->ops.insert( p );
	} else {
		key.free();

		if ( opcode == PROTO_OPCODE_SET ) {
			if ( timestamp > it->second.timestamp ) {
				// Replace with the current entry
				it->second = metadataBackup;
			} else {
				// TODO: Handle wrap around case
			}
		} else if ( opcode == PROTO_OPCODE_DELETE ) {
			if ( timestamp > it->second.timestamp ) {
				// Remove the previous entry
				key = it->first;
				this->ops.erase( it );
				key.free();
			} else {
				// TODO: Handle wrap around case
			}
		}
	}
	UNLOCK( &this->lock );
}

void Backup::insert( uint8_t keySize, char *keyStr, bool isLarge, uint8_t opcode, uint32_t timestamp, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint8_t sealedCount, Metadata *sealed ) {
	Key key;
	MetadataBackup metadataBackup;
	std::unordered_map<Key, MetadataBackup>::iterator opsIt;

	key.set( keySize, keyStr, 0, isLarge );
	key.dup( 0, 0, 0, isLarge );

	metadataBackup.opcode = opcode;
	metadataBackup.timestamp = timestamp;
	metadataBackup.set( listId, stripeId, chunkId );

	std::pair<Key, MetadataBackup> p1( key, metadataBackup );

	LOCK( &this->lock );

	opsIt = this->ops.find( key );
	if ( opsIt == this->ops.end() ) {
		this->ops.insert( p1 );
	} else {
		key.free();

		if ( opcode == PROTO_OPCODE_SET ) {
			if ( timestamp > opsIt->second.timestamp ) {
				// Replace with the current entry
				opsIt->second = metadataBackup;
			} else {
				// TODO: Handle wrap around case
			}
		} else if ( opcode == PROTO_OPCODE_DELETE ) {
			if ( timestamp > opsIt->second.timestamp ) {
				// Remove the previous entry
				key = opsIt->first;
				this->ops.erase( opsIt );
				key.free();
			} else {
				// TODO: Handle wrap around case
			}
		}
	}

	for ( uint8_t i = 0; i < sealedCount; i++ ) {
		std::pair<uint32_t, Metadata> p2( timestamp, sealed[ i ] );
		this->sealed.insert( p2 );
	}

	UNLOCK( &this->lock );
}

size_t Backup::erase( uint32_t fromTimestamp, uint32_t toTimestamp ) {
	std::unordered_map<Key, MetadataBackup>::iterator opsIt;
	std::unordered_multimap<uint32_t, Metadata>::iterator sealedIt;
	bool isWrappedAround, selected;
	size_t opsCount = 0, sealedCount = 0;

	LOCK( &this->lock );
	isWrappedAround = fromTimestamp > toTimestamp;
	for ( opsIt = this->ops.begin(); opsIt != this->ops.end(); ) {
		const MetadataBackup &metadataBackup = opsIt->second;
		if ( isWrappedAround ) {
			selected = (
				metadataBackup.timestamp >= fromTimestamp ||
				metadataBackup.timestamp < toTimestamp
			);
		} else {
			selected = (
				metadataBackup.timestamp >= fromTimestamp &&
				metadataBackup.timestamp < toTimestamp
			);
		}
		if ( selected ) {
			Key key = opsIt->first;
			opsIt = this->ops.erase( opsIt );
			key.free();
			opsCount++;
		} else {
			opsIt++;
		}
	}

	for ( sealedIt = this->sealed.begin(); sealedIt != this->sealed.end(); ) {
		if ( isWrappedAround ) {
			selected = (
				sealedIt->first >= fromTimestamp ||
				sealedIt->first < toTimestamp
			);
		} else {
			selected = (
				sealedIt->first >= fromTimestamp &&
				sealedIt->first < toTimestamp
			);
		}
		if ( selected ) {
			sealedIt = this->sealed.erase( sealedIt );
			sealedCount++;
		} else {
			sealedIt++;
		}
	}
	// printf(
	// 	"Erased backup from %u to %u:\n"
	// 	"- Number of metadata backup: (released) %lu; (remaining) %lu;\n"
	// 	"- Number of sealed chunk backup: (released) %lu; (remaining) %lu.\n",
	// 	fromTimestamp, toTimestamp,
	// 	opsCount, this->ops.size(),
	// 	sealedCount, this->sealed.size()
	// );
	UNLOCK( &this->lock );

	return ( opsCount + sealedCount );
}

void Backup::print( FILE *f ) {
	std::unordered_map<Key, MetadataBackup>::iterator opsIt;
	std::unordered_multimap<uint32_t, Metadata>::iterator sealedIt;
	fprintf( f, "--------------------\n" );
	fprintf( f, "Number of metadata backup: %lu\n", this->ops.size() );
	for ( opsIt = this->ops.begin(); opsIt != this->ops.end(); opsIt++ ) {
		const Key &key = opsIt->first;
		MetadataBackup &metadataBackup = opsIt->second;

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
	fprintf( f, "********************\n" );
	fprintf( f, "Number of sealed chunk backup: %lu\n", this->sealed.size() );
	for ( sealedIt = this->sealed.begin(); sealedIt != this->sealed.end(); sealedIt++ ) {
		uint32_t timestamp = sealedIt->first;
		Metadata &metadata = sealedIt->second;

		fprintf(
			f, "[%10u] (%u, %u, %u)\n",
			timestamp,
			metadata.listId,
			metadata.stripeId,
			metadata.chunkId
		);
	}
	fprintf( f, "--------------------\n" );
}

Backup::~Backup() {
	std::unordered_map<Key, MetadataBackup>::iterator it;
	Key key;

	it = this->ops.begin();
	while ( it != this->ops.end() ) {
		key = it->first;
		key.free();
		it = this->ops.begin();
	}
}
