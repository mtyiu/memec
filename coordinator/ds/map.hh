#ifndef __SLAVE_MAP_MAP_HH__
#define __SLAVE_MAP_MAP_HH__

#include <unordered_map>
#include <unordered_set>
#include "../../common/ds/key.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/lock/lock.hh"
#include "../../common/protocol/protocol.hh"

class Map {
private:
	bool updateMaxStripeId( uint32_t listId, uint32_t stripeId );

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
	std::unordered_map<Key, Metadata> keys;
	LOCK_T keysLock;
	/**
	 * Store the degraded locks
	 * (list ID, stripe ID, chunk ID) |-> (list ID, chunk ID)
	 */
	std::unordered_map<Metadata, Metadata> degradedLocks;
	LOCK_T degradedLocksLock;
	/**
	 * Store the current stripe ID of each list.
	 */
	static uint32_t *stripes;
	static LOCK_T stripesLock;

	static void init( uint32_t numStripeList );
	static void free();

	Map();
	// Insertion //
	bool insertChunk(
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		bool needsLock = true, bool needsUnlock = true
	);
	bool insertKey(
		char *keyStr, uint8_t keySize,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint8_t opcode, bool needsLock = true, bool needsUnlock = true
	);
	bool insertDegradedLock(
		Metadata srcMetadata, Metadata &dstMetadata,
		bool needsLock = true, bool needsUnlock = true
	);

	// Find //
	bool findMetadataByKey( char *keyStr, uint8_t keySize, Metadata &metadata );
	bool findDegradedLock( uint32_t srcListId, uint32_t srcStripeId, uint32_t srcChunkId, Metadata &dstMetadata );
	bool isSealed( Metadata metadata );

	// Debug //
	void dump( FILE *f = stdout );
	void persist( FILE *f );
};

#endif
