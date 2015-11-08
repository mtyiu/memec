#ifndef __SLAVE_MAP_MAP_HH__
#define __SLAVE_MAP_MAP_HH__

#include <unordered_map>
#include <unordered_set>
#include "../../common/ds/key.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/lock/lock.hh"
#include "../../common/protocol/protocol.hh"

class Map {
public:
	/**
	 * Store the set of sealed chunks
	 * (list ID, stripe ID, chunk ID)
	 */
	std::unordered_set<Metadata> chunks;
	LOCK_T chunksLock;
	/**
	 * Store the mapping between keys and chunks
	 * Key |-> (list ID, stripe ID, chunk ID)
	 */
	static std::unordered_map<Key, Metadata> keys;
	static LOCK_T keysLock;
	/**
	 * Store the set of keys with lock acquired 
	 */
	static std::unordered_set<Key> lockedKeys;

	static uint32_t *stripes;
	static LOCK_T stripesLock;

	static void init( uint32_t numStripeList );
	static void free();

	Map();

	static bool updateMaxStripeId( uint32_t listId, uint32_t stripeId );
	bool seal( uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool needsLock = true, bool needsUnlock = true );
	static bool setKey( char *keyStr, uint8_t keySize, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint8_t opcode, bool needsLock = true, bool needsUnlock = true );
	void dump( FILE *f = stdout );
	void persist( FILE *f );
};

#endif
