#ifndef __MASTER_DS_PENDING_HH__
#define __MASTER_DS_PENDING_HH__

#include <set>
#include <cstring>
#include "../../common/ds/key.hh"
#include "../../common/ds/metadata.hh"

class ChunkUpdate : public Metadata {
public:
	uint8_t offset, length, keySize;
	char *key;
	void *ptr;

	bool operator<( const ChunkUpdate &m ) const {
		int ret;
		if ( ! Metadata::operator<( m ) )
			return false;

		if ( this->offset < m.offset )
			return true;
		if ( this->offset > m.offset )
			return false;

		if ( this->length < m.length )
			return true;
		if ( this->length > m.length )
			return false;

		if ( this->keySize < m.keySize )
			return true;
		if ( this->keySize > m.keySize )
			return false;

		ret = strncmp( this->key, m.key, this->keySize );
		if ( ret < 0 )
			return true;
		if ( ret > 0 )
			return false;

		return this->ptr < m.ptr;
	}
};

typedef struct {
	struct {
		std::set<Key> get;
		std::set<Key> set;
		std::set<Key> update;
		std::set<Key> del;
	} applications;
	struct {
		std::set<Key> get;
		std::set<Key> set;
		std::set<Key> update;
		std::set<ChunkUpdate> updateChunk;
		std::set<Key> del;
		std::set<ChunkUpdate> deleteChunk;
	} slaves;
} Pending;

#endif
