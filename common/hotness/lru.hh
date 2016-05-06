#ifndef __SERVER_HOTNESS_LRU_HH__
#define __SERVER_HOTNESS_LRU_HH__

#include <unordered_map>
#include "hotness.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/lock/lock.hh"

extern "C" {
#include "../../common/ds/list.h"
}

struct LruListRecord {
	Metadata metadata;
	Key key;
	struct list_head listPtr;
};

class LruHotness : public Hotness<KeyMetadata> {
public:

	LruHotness();
	LruHotness( size_t maxItems );
	~LruHotness();

	bool insert( KeyMetadata newItem );
	void reset();

	std::vector<KeyMetadata> getItems();
	std::vector<KeyMetadata> getTopNItems( size_t n );

	size_t getItemCount();
	size_t getFreeItemCount();

	size_t getAndReset( std::vector<KeyMetadata> &dest, size_t n = 0 );

	void print( FILE *output = stdout );

private:
	std::unordered_map<Metadata, LruListRecord*> existingRecords;
	struct LruListRecord *slots;
	struct list_head freeRecords;
	struct list_head lruList;

	LOCK_T lock;

	void init( size_t maxItems );
};

#endif
