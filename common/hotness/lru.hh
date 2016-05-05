#ifndef __SERVER_HOTNESS_LRU_HH__
#define __SERVER_HOTNESS_LRU_HH__

#include <unordered_map>
#include "hotness.hh"
#include "../../common/lock/lock.hh"

extern "C" {
#include "../../common/ds/list.h"
}

struct LruListRecord {
	Metadata metadata;
	struct list_head listPtr;
};

class LruHotness : public Hotness {
public:

	LruHotness();
	LruHotness( size_t maxItems );
	~LruHotness();

	bool insert( Metadata newItem );
	void reset();

	std::vector<Metadata> getItems();
	std::vector<Metadata> getTopNItems( size_t n );

	size_t getItemCount();
	size_t getFreeItemCount();

	size_t getAndReset( std::vector<Metadata> &dest, size_t n = 0 );

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
