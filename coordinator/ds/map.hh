#ifndef __SLAVE_MAP_MAP_HH__
#define __SLAVE_MAP_MAP_HH__

#include <unordered_map>
#include <unordered_set>
#include <cassert>
#include "../../common/ds/key.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/lock/lock.hh"
#include "../../common/protocol/protocol.hh"

class ListStripe {
public:
	uint32_t listId;
	uint32_t stripeId;

	void clear() {
		this->listId = 0;
		this->stripeId = 0;
	}

	void set( uint32_t listId, uint32_t stripeId ) {
		this->listId = listId;
		this->stripeId = stripeId;
	}

	void clone( const ListStripe &l ) {
		this->listId = l.listId;
		this->stripeId = l.stripeId;
	}

	bool operator<( const ListStripe &l ) const {
		if ( this->listId < l.listId )
			return true;
		if ( this->listId > l.listId )
			return false;

		return this->stripeId < l.stripeId;
	}

	bool operator==( const ListStripe &l ) const {
		return (
			this->listId == l.listId &&
			this->stripeId == l.stripeId
		);
	}
};

namespace std {
	template<> struct hash<ListStripe> {
		size_t operator()( const ListStripe &listStripe ) const {
			size_t ret = 0;
			char *ptr = ( char * ) &ret, *tmp;
			tmp = ( char * ) &listStripe.stripeId;
			ptr[ 0 ] = tmp[ 0 ];
			ptr[ 1 ] = tmp[ 1 ];
			ptr[ 2 ] = tmp[ 2 ];
			ptr[ 3 ] = tmp[ 3 ];
			tmp = ( char * ) &listStripe.listId;
			ptr[ 4 ] = tmp[ 0 ];
			ptr[ 5 ] = tmp[ 1 ];
			ptr[ 6 ] = tmp[ 2 ];
			ptr[ 7 ] = tmp[ 3 ];
			return ret;
		}
	};
}

struct DegradedLock {
	uint32_t *original, *reconstructed;
	uint32_t reconstructedCount;
	uint32_t ongoingAtChunk;

	DegradedLock( uint32_t *original = 0, uint32_t *reconstructed = 0, uint32_t reconstructedCount = 0, uint32_t ongoingAtChunk = 0 ) {
		this->set( original, reconstructed, reconstructedCount, ongoingAtChunk );
	}

	void set( uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount, uint32_t ongoingAtChunk ) {
		if ( ! reconstructedCount ) {
			this->original = 0;
			this->reconstructed = 0;
			this->ongoingAtChunk = -1;
		} else {
			this->original = original;
			this->reconstructed = reconstructed;
			this->reconstructedCount = reconstructedCount;
			this->ongoingAtChunk = ongoingAtChunk;
		}
	}

	void dup( uint8_t numSurvivingChunkIds, uint32_t *survivingChunkIds, uint32_t chunkCount ) {
		if ( this->reconstructedCount ) {
			uint32_t listId = this->original[ 0 ];
			uint32_t newReconstructedCount = chunkCount - numSurvivingChunkIds - this->reconstructedCount;
			assert( newReconstructedCount < chunkCount );

			uint32_t *_original = new uint32_t[ reconstructedCount * 2 + newReconstructedCount ];
			uint32_t *_reconstructed = new uint32_t[ reconstructedCount * 2 + newReconstructedCount ];
			for ( uint32_t i = 0; i < this->reconstructedCount; i++ ) {
				_original[ i * 2     ] = this->original[ i * 2     ];
				_original[ i * 2 + 1 ] = this->original[ i * 2 + 1 ];
				_reconstructed[ i * 2     ] = this->reconstructed[ i * 2     ];
				_reconstructed[ i * 2 + 1 ] = this->reconstructed[ i * 2 + 1 ];
			}

			if ( newReconstructedCount ) {
				std::unordered_set<uint32_t> redirectedChunkIds;
				std::unordered_set<uint32_t> handledChunkIds;
				std::unordered_set<uint32_t>::iterator it;
				uint32_t ptr = this->reconstructedCount;

				for ( uint32_t i = 0; i < this->reconstructedCount; i++ ) {
					handledChunkIds.insert( this->original[ i * 2 + 1 ] );
					redirectedChunkIds.insert( this->reconstructed[ i * 2 + 1 ] );
				}
				for ( uint32_t i = 0; i < numSurvivingChunkIds; i++ )
					handledChunkIds.insert( survivingChunkIds[ i ] );

				for ( uint32_t i = 0; i < chunkCount; i++ ) {
					if ( ! handledChunkIds.count( i ) ) {
						// Not handled (neither surviving or reconstructed) - add new entries
						_original[ ptr * 2     ] = listId;
						_original[ ptr * 2 + 1 ] = i;
						_reconstructed[ ptr * 2     ] = listId;
						for ( uint32_t j = 0; j < numSurvivingChunkIds; j++ ) {
							if ( ! redirectedChunkIds.count( survivingChunkIds[ j ] ) ) {
								// Redirect to a not-yet-used server
								_reconstructed[ ptr * 2 + 1 ] = survivingChunkIds[ j ];
								break;
							}
						}
						ptr++;
					}
				}

				this->reconstructedCount += newReconstructedCount;
			}

			this->original = _original;
			this->reconstructed = _reconstructed;

			if ( newReconstructedCount ) {
				this->sort();
				// this->print();
			}
		}
	}

	void expand( uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount, uint32_t ongoingAtChunk, uint8_t numSurvivingChunkIds, uint32_t *survivingChunkIds, uint32_t chunkCount ) {
		if ( ! this->reconstructedCount ) {
			this->set( original, reconstructed, reconstructedCount, ongoingAtChunk );
			this->dup( numSurvivingChunkIds, survivingChunkIds, chunkCount );
		}
		if ( ! reconstructedCount )
			return;


		// Count the number of new entries
		uint32_t newReconstructedCount = 0;
		for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
			uint32_t newListId  = original[ i * 2     ];
			uint32_t newChunkId = original[ i * 2 + 1 ];
			bool found = false;
			for ( uint32_t j = 0; j < this->reconstructedCount; j++ ) {
				if ( this->original[ j * 2 ] == newListId && this->original[ j * 2 + 1 ] == newChunkId ) {
					found = true;
					break;
				}
			}
			if ( ! found )
				newReconstructedCount++;
		}

		if ( newReconstructedCount == 0 )
			return;

		newReconstructedCount += this->reconstructedCount;
		uint32_t *_original = new uint32_t[ newReconstructedCount ];
		uint32_t *_reconstructed = new uint32_t[ newReconstructedCount ];

		// Copy the original entries
		memcpy( _original, this->original, this->reconstructedCount * 2 * sizeof( uint32_t ) );
		memcpy( _reconstructed, this->reconstructed, this->reconstructedCount * 2 * sizeof( uint32_t ) );

		// Copy the new entries
		uint32_t tmp = this->reconstructedCount;
		for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
			uint32_t newListId  = original[ i * 2     ];
			uint32_t newChunkId = original[ i * 2 + 1 ];
			bool found = false;
			for ( uint32_t j = 0; j < this->reconstructedCount; j++ ) {
				if ( this->original[ j * 2 ] == newListId && this->original[ j * 2 + 1 ] == newChunkId ) {
					found = true;
					break;
				}
			}
			if ( ! found ) {
				_original[ tmp * 2     ] = original[ i * 2     ];
				_original[ tmp * 2 + 1 ] = original[ i * 2 + 1 ];
				_reconstructed[ tmp * 2     ] = reconstructed[ i * 2     ];
				_reconstructed[ tmp * 2 + 1 ] = reconstructed[ i * 2 + 1 ];
				tmp++;
			}
		}

		delete[] this->original;
		delete[] this->reconstructed;
		this->reconstructedCount = newReconstructedCount;
		this->original = _original;
		this->reconstructed = _reconstructed;

		this->sort();
	}

	void sort() {
		// Sort by original chunk ID (selection sort implemented)
		for ( uint32_t i = 0; i < this->reconstructedCount; i++ ) {
			uint32_t tmpOriginalListId       = this->original[ i * 2     ],
			         tmpOriginalChunkId      = this->original[ i * 2 + 1 ],
			         tmpReconstructedListId  = this->reconstructed[ i * 2     ],
			         tmpReconstructedChunkId = this->reconstructed[ i * 2 + 1 ],
			         minIndex = i;
			for ( uint32_t j = i + 1; j < this->reconstructedCount; j++ ) {
				if ( this->original[ j * 2 + 1 ] < this->original[ minIndex * 2 + 1 ] )
					minIndex = j;
			}
			// Swap minIndex <-> i
			this->original[ i * 2     ] = this->original[ minIndex * 2     ];
			this->original[ i * 2 + 1 ] = this->original[ minIndex * 2 + 1 ];
			this->reconstructed[ i * 2     ] = this->reconstructed[ minIndex * 2     ];
			this->reconstructed[ i * 2 + 1 ] = this->reconstructed[ minIndex * 2 + 1 ];

			this->original[ minIndex * 2     ] = tmpOriginalListId;
			this->original[ minIndex * 2 + 1 ] = tmpOriginalChunkId;
			this->reconstructed[ minIndex * 2     ] = tmpReconstructedListId;
			this->reconstructed[ minIndex * 2 + 1 ] = tmpReconstructedChunkId;
		}
	}

	void print() {
		for ( uint32_t i = 0; i < this->reconstructedCount; i++ ) {
			printf(
				"%s(%u, %u) |-> (%u, %u)",
				i == 0 ? "" : "; ",
				this->original[ i * 2     ],
				this->original[ i * 2 + 1 ],
				this->reconstructed[ i * 2     ],
				this->reconstructed[ i * 2 + 1 ]
			);
		}
		printf( "; ongoingAtChunk: %u\n", this->ongoingAtChunk );
	}

	void free() {
		if ( this->original ) delete[] this->original;
		if ( this->reconstructed ) delete[] this->reconstructed;
		this->original = 0;
		this->reconstructed = 0;
	}
};

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
	std::unordered_map<Key, OpMetadata> keys;
	/**
	 * Store the set of keys with lock acquired
	 */
	std::unordered_set<Key> lockedKeys;
	LOCK_T keysLock;

	/**
	 * Store the degraded locks
	 * (list ID, stripe ID) |-> (original, reconstructed, reconstructedCount)
	 */
	static std::unordered_map<ListStripe, DegradedLock> degradedLocks;
	/**
	 * Store the to-be-released degraded locks
	 * (list ID, stripe ID, chunk ID) |-> (list ID, chunk ID)
	 */
	static std::unordered_map<ListStripe, DegradedLock> releasingDegradedLocks;
	static LOCK_T degradedLocksLock;

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
		uint8_t opcode, uint32_t timestamp, bool needsLock = true, bool needsUnlock = true
	);
	bool insertDegradedLock(
		uint32_t listId, uint32_t stripeId,
		uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
		uint32_t ongoingAtChunk, uint8_t numSurvivingChunkIds, uint32_t *survivingChunkIds, uint32_t chunkCount,
		bool needsLock = true, bool needsUnlock = true
	);
	bool expandDegradedLock(
		uint32_t listId, uint32_t stripeId,
		uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
		uint32_t ongoingAtChunk, uint8_t numSurvivingChunkIds, uint32_t *survivingChunkIds, uint32_t chunkCount,
		DegradedLock &degradedLock,
		bool needsLock = true, bool needsUnlock = true
	);

	// Find //
	bool findMetadataByKey( char *keyStr, uint8_t keySize, Metadata &metadata );
	bool findDegradedLock(
		uint32_t listId, uint32_t stripeId, DegradedLock &degradedLock,
		bool needsLock = true, bool needsUnlock = true, LOCK_T **lock = 0
	);
	bool isSealed( Metadata metadata );

	// Debug //
	size_t dump( FILE *f = stdout );
	static size_t dumpDegradedLocks( FILE *f = stdout );
	void persist( FILE *f );
};

#endif
