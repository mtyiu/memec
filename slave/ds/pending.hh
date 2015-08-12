#ifndef __SLAVE_DS_PENDING_HH__
#define __SLAVE_DS_PENDING_HH__

#include "../../common/ds/pending.hh"

typedef struct {
	struct {
		std::set<Key> get;
		std::set<KeyValueUpdate> update;
		std::set<Key> del;
	} masters;
   struct {
		// std::set<ChunkUpdate> getChunk;
		std::set<ChunkUpdate> updateChunk;
		std::set<ChunkUpdate> deleteChunk;
	} slavePeers;
} Pending;

#endif
