#ifndef __SERVER_MAP_MAP_HH__
#define __SERVER_MAP_MAP_HH__

#include <unordered_map>
#include <unordered_set>
#include "../../common/ds/chunk.hh"
#include "../../common/ds/chunk_pool.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/hash/cuckoo_hash.hh"
#include "../../common/lock/lock.hh"
#include "../../common/protocol/protocol.hh"
#include "../../common/timestamp/timestamp.hh"
#include "../../common/util/debug.hh"

class Map {
private:
	Timestamp *timestamp;
	/**
	 * Store the mapping between keys and chunks
	 * Key |-> (list ID, stripe ID, chunk ID, offset, length, isPartyRemapped)
	 */
	CuckooHash keys;
	LOCK_T keysLock;
	/**
	 * Store the cached chunks
	 * (list ID, stripe ID, chunk ID) |-> Chunk *
	 */
	CuckooHash chunks;
	LOCK_T chunksLock;

	/**
	 * Store the forwarded, reconstructed chunks
	 * (list ID, stripe ID, chunk ID) (src) |-> (list ID, stripe ID, chunk ID) (dst)
	 */
	struct {
		std::unordered_map<Key, Metadata> keys;
		std::unordered_map<Metadata, Metadata> chunks;
		LOCK_T lock;
	} forwarded;

	struct {
		CuckooHash keys;
		LOCK_T keysLock;

		CuckooHash chunks;
		LOCK_T chunksLock;
	} migrated;

public:
	/**
	 * Store the keys to be synchronized with coordinator
	 * Key |-> (list ID, stripe ID, chunk ID, opcode)
	 */
	std::unordered_map<Key, OpMetadata> ops;
	LOCK_T opsLock;
	/**
	 * Store the metadata of the sealed chunks to be synchronized with coordinator
	 * (list ID, stripe ID, chunk ID)
	 */
	std::unordered_set<Metadata> sealed;
	LOCK_T sealedLock;

	/**
	 * Store the used stripe IDs
	 * (list ID) |-> std::unordered_set<(stripe ID)>
	 */
	std::unordered_map<uint32_t, std::unordered_set<uint32_t>> stripeIds;
	LOCK_T stripeIdsLock;

	Map();
	void setTimestamp( Timestamp *timestamp );

	// Object hash table
	bool insertKey(
		Key key, uint8_t opcode, uint32_t &timestamp,
		KeyMetadata &keyMetadata,
		bool needsLock = true, bool needsUnlock = true,
		bool needsUpdateOpMetadata = true,
		bool isLarge = false
	);
	char *findObject(
		char *keyStr, uint8_t keySize,
		KeyValue *keyValuePtr = 0,
		Key *keyPtr = 0,
		bool needsLock = true, bool needsUnlock = true
	);
	char *findLargeObject(
		char *keyStr, uint8_t keySize,
		KeyValue *keyValuePtr = 0,
		Key *keyPtr = 0,
		bool needsLock = true, bool needsUnlock = true
	);
	bool deleteKey(
		Key key, uint8_t opcode, uint32_t &timestamp,
		KeyMetadata &keyMetadata,
		bool needsLock, bool needsUnlock,
		bool needsUpdateOpMetadata = true
	);
	void getKeysMap( CuckooHash **keys, LOCK_T **lock );

	// Chunk hash table
	void setChunk(
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		Chunk *chunk, bool isParity = false,
		bool needsLock = true, bool needsUnlock = true
	);
	Chunk *findChunkById(
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		Metadata *metadataPtr = 0,
		bool needsLock = true, bool needsUnlock = true, LOCK_T **lock = 0
	);
	bool seal( uint32_t listId, uint32_t stripeId, uint32_t chunkId );
	uint32_t nextStripeID( uint32_t listId, uint32_t from = 0 );
	void getChunksMap( CuckooHash **chunks, LOCK_T **lock );

	// Operator & metadata
	bool insertOpMetadata(
		uint8_t opcode, uint32_t &timestamp,
		Key key, KeyMetadata keyMetadata,
		bool dup = true
	);

	// Forwarded chunks
	bool insertForwardedChunk(
		uint32_t srcListId, uint32_t srcStripeId, uint32_t srcChunkId,
		uint32_t dstListId, uint32_t dstStripeId, uint32_t dstChunkId
	);
	bool findForwardedChunk(
		uint32_t srcListId, uint32_t srcStripeId, uint32_t srcChunkId,
		Metadata &dstMetadata
	);
	bool eraseForwardedChunk( uint32_t srcListId, uint32_t srcStripeId, uint32_t srcChunkId );

	// Forwarded keys
	bool insertForwardedKey(
		uint8_t keySize, char *keyStr, bool isLarge,
		uint32_t dstListId, uint32_t dstChunkId
	);
	bool findForwardedKey( uint8_t keySize, char *keyStr, bool isLarge, Metadata &dstMetadata );
	bool eraseForwardedKey( uint8_t keySize, char *keyStr, bool isLarge );

	// Migrate chunks and keys
	Chunk *migrateChunk( uint32_t listId, uint32_t stripeId, uint32_t chunkId );
};

#endif
