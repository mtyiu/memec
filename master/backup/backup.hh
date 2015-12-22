#ifndef __MASTER_BACKUP_BACKUP_HH__
#define __MASTER_BACKUP_BACKUP_HH__

#include <unordered_map>
#include "../../common/ds/key.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/lock/lock.hh"

class MetadataBackup : public OpMetadata {
public:
	uint32_t timestamp;
};

class Backup {
private:
	LOCK_T lock;
	std::unordered_multimap<Key, MetadataBackup> ops;

public:
	Backup();
	void insert( uint8_t keySize, char *keyStr, uint8_t opcode, uint32_t timestamp, uint32_t listId, uint32_t stripeId, uint32_t chunkId );
	void print( FILE *f = stdout );
	~Backup();
};

#endif
