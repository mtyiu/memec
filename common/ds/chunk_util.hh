#ifndef __COMMON_DS_CHUNK_UTIL_HH__
#define __COMMON_DS_CHUNK_UTIL_HH__

#include <stdint.h>
#include <arpa/inet.h>
#include "metadata.hh"
#include "chunk.hh"
#include "key.hh"
#include "key_value.hh"
#include "../lock/lock.hh"
#include "../coding/coding.hh"

class ChunkUtil {
public:
	static uint32_t chunkSize;
	static uint32_t dataChunkCount;
	static LOCK_T lock;

	static inline void init( uint32_t chunkSize, uint32_t dataChunkCount ) {
		ChunkUtil::chunkSize = chunkSize;
		ChunkUtil::dataChunkCount = dataChunkCount;
		LOCK_INIT( &ChunkUtil::lock );
	}

	// Getters
	static inline void get( Chunk *chunk, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId ) {
		struct ChunkIdentifier *chunkIdentifier = ( struct ChunkIdentifier * ) chunk;
		listId   = chunkIdentifier->listId;
		stripeId = chunkIdentifier->stripeId;
		chunkId  = chunkIdentifier->chunkId;
	}
	static inline Metadata getMetadata( Chunk *chunk ) {
		struct ChunkIdentifier *chunkIdentifier = ( struct ChunkIdentifier * ) chunk;
		Metadata metadata;
		metadata.set(
			chunkIdentifier->listId,
			chunkIdentifier->stripeId,
			chunkIdentifier->chunkId
		);
		return metadata;
	}
	static inline uint32_t getListId( Chunk *chunk ) {
		return ( ( struct ChunkIdentifier * ) chunk )->listId;
	}
	static inline uint32_t getStripeId( Chunk *chunk ) {
		return ( ( struct ChunkIdentifier * ) chunk )->stripeId;
	}
	static inline uint32_t getChunkId( Chunk *chunk ) {
		return ( ( struct ChunkIdentifier * ) chunk )->chunkId;
	}
	static inline uint32_t getSize( Chunk *chunk, bool needsLock = true, bool needsUnlock = true ) {
		if ( chunk == Coding::zeros )
			return 0;

		if ( ChunkUtil::isParity( chunk ) )
			return ChunkUtil::chunkSize;

		if ( needsLock ) LOCK( &ChunkUtil::lock );

		// Scan the whole data chunk
		uint8_t keySize;
		uint32_t valueSize, tmp, splitOffset, splitSize;
		char *key, *value;
		char *data, *ptr;
		uint32_t _chunkSize = 0;
		bool isLarge;

		data = ptr = ChunkUtil::getData( chunk );

		while ( ptr + KEY_VALUE_METADATA_SIZE < data + ChunkUtil::chunkSize ) {
			KeyValue::deserialize( ptr, key, keySize, value, valueSize, splitOffset );
			if ( keySize == 0 && valueSize == 0 )
				break;

			isLarge = LargeObjectUtil::isLarge( keySize, valueSize, 0, &splitSize );
			if ( isLarge ) {
				if ( splitOffset + splitSize > valueSize )
					splitSize = valueSize - splitOffset;
				tmp = KEY_VALUE_METADATA_SIZE + SPLIT_OFFSET_SIZE + keySize + splitSize;
			} else {
				tmp = KEY_VALUE_METADATA_SIZE + keySize + valueSize;
			}

			ptr += tmp;
			_chunkSize += tmp;
		}

		if ( needsUnlock ) UNLOCK( &ChunkUtil::lock );

		return _chunkSize;
	}
	static inline uint32_t getCount( Chunk *chunk ) {
		if ( ChunkUtil::isParity( chunk ) ) {
			return 0;
		} else {
			LOCK( &ChunkUtil::lock );

			// Scan whole chunk
			uint8_t keySize;
			uint32_t valueSize, tmp, splitOffset, splitSize;
			char *key, *value;
			char *data, *ptr;
			uint32_t count = 0;
			bool isLarge;

			data = ptr = ChunkUtil::getData( chunk );

			while ( ptr + KEY_VALUE_METADATA_SIZE < data + ChunkUtil::chunkSize ) {
				KeyValue::deserialize( ptr, key, keySize, value, valueSize, splitOffset );
				if ( keySize == 0 && valueSize == 0 )
					break;

				isLarge = LargeObjectUtil::isLarge( keySize, valueSize, 0, &splitSize );
				if ( isLarge ) {
					if ( splitOffset + splitSize > valueSize )
						splitSize = valueSize - splitOffset;
					tmp = KEY_VALUE_METADATA_SIZE + SPLIT_OFFSET_SIZE + keySize + splitSize;
				} else {
					tmp = KEY_VALUE_METADATA_SIZE + keySize + valueSize;
				}

				ptr += tmp;
				count++;
			}

			UNLOCK( &ChunkUtil::lock );

			return count;
		}
	}
	static inline char *getData( Chunk *chunk ) {
		return ( ( char * ) chunk ) + CHUNK_IDENTIFIER_SIZE;
	}
	static inline char *getData( Chunk *chunk, uint32_t &offset, uint32_t &size ) {
		uint32_t _chunkSize = ChunkUtil::getSize( chunk );
		char *data = ChunkUtil::getData( chunk );

		if ( _chunkSize > 0 ) {
			for ( offset = 0; offset < _chunkSize; offset++ ) {
				if ( data[ offset ] != 0 )
					break;
			}

			for ( size = _chunkSize - 1; size > offset; size-- ) {
				if ( data[ size ] != 0 )
					break;
			}

			size = size + 1 - offset;
		} else {
			offset = 0;
			size = 0;
		}

		return data + offset;
	}
	static inline bool isParity( Chunk *chunk ) {
		return ( ChunkUtil::getChunkId( chunk ) >= ChunkUtil::dataChunkCount );
	}
	static inline KeyValue getObject( Chunk *chunk, uint32_t offset ) {
		KeyValue keyValue;
		keyValue.data = ( char * ) chunk + CHUNK_IDENTIFIER_SIZE + offset;
		return keyValue;
	}
	static inline int next( Chunk *chunk, uint32_t offset, char *&key, uint8_t &keySize ) {
		char *data = ChunkUtil::getData( chunk );
		char *ptr = data + offset, *value;
		int ret = -1;
		uint32_t valueSize, splitOffset, splitSize, tmp;

		if ( ptr + KEY_VALUE_METADATA_SIZE < data + ChunkUtil::chunkSize ) {
			KeyValue::deserialize( ptr, key, keySize, value, valueSize, splitOffset );

			bool isLarge = LargeObjectUtil::isLarge( keySize, valueSize, 0, &splitSize );
			if ( isLarge ) {
				if ( splitOffset + splitSize > valueSize )
					splitSize = valueSize - splitOffset;
				tmp = KEY_VALUE_METADATA_SIZE + SPLIT_OFFSET_SIZE + keySize + splitSize;
			} else {
				tmp = KEY_VALUE_METADATA_SIZE + keySize + valueSize;
			}

			if ( keySize )
				ret = offset + tmp;
			else
				key = 0;
		}
		return ret;
	}

	// Setters
	static inline void set( Chunk *chunk, uint32_t listId, uint32_t stripeId, uint32_t chunkId ) {
		struct ChunkIdentifier *chunkIdentifier = ( struct ChunkIdentifier * ) chunk;
		chunkIdentifier->listId   = listId;
		chunkIdentifier->stripeId = stripeId;
		chunkIdentifier->chunkId  = chunkId;
	}
	static inline void setListId( Chunk *chunk, uint32_t listId ) {
		( ( struct ChunkIdentifier * ) chunk )->listId = listId;
	}
	static inline void setStripeId( Chunk *chunk, uint32_t stripeId ) {
		( ( struct ChunkIdentifier * ) chunk )->stripeId = stripeId;
	}
	static inline void setChunkId( Chunk *chunk, uint32_t chunkId ) {
		( ( struct ChunkIdentifier * ) chunk )->chunkId = chunkId;
	}

	// Memory allocator for objects
	static inline char *alloc( Chunk *chunk, uint32_t size, uint32_t &offset ) {
		LOCK( &ChunkUtil::lock );

		offset = ChunkUtil::getSize( chunk, false, false );
		char *ptr = ( ( char * ) chunk ) + CHUNK_IDENTIFIER_SIZE + offset;

		KeyValue::serialize( ptr, 0, 0, 0, size - CHUNK_IDENTIFIER_SIZE, 0 );

		UNLOCK( &ChunkUtil::lock );
		return ptr;
	}

	// Update
	static inline void computeDelta(
		Chunk *chunk,
		char *delta, char *newData,
		uint32_t offset, uint32_t length,
		bool applyUpdate = true
	) {
		char *data = getData( chunk ) + offset;
		Coding::bitwiseXOR(
			delta,
			data,    // original data
			newData, // new data
			length
		);
		if ( applyUpdate ) {
			Coding::bitwiseXOR(
				data,
				data,  // original data
				delta, // new data
				length
			);
		}
	}

	// Delete (return actual delta size)
	static inline uint32_t deleteObject( Chunk *chunk, uint32_t offset, char *delta = 0 ) {
		char *data = getData( chunk ) + offset;
		uint8_t keySize;
		uint32_t valueSize, length, splitOffset;
		char *key, *value;

		KeyValue::deserialize( data, key, keySize, value, valueSize, splitOffset );
		length = keySize + valueSize;

		if ( delta ) {
			// Calculate updated chunk data (delta)
			memset( delta, 0, KEY_VALUE_METADATA_SIZE + length );
			KeyValue::setSize( delta, 0, length );

			// Compute delta' := data XOR delta
			Coding::bitwiseXOR(
				delta,
				data,  // original data
				delta, // new data
				length
			);

			// Apply delta by setting data := data XOR delta' = data XOR ( data XOR delta ) = delta
			Coding::bitwiseXOR(
				data,
				data,  // original data
				delta, // new data
				length
			);
		} else {
			memset( data, 0, KEY_VALUE_METADATA_SIZE + length );
		}

		return length;
	}

	// Utilities
	static inline void dup( Chunk *dst, Chunk *src ) {
		memcpy(
			( char * ) dst,
			( char * ) src,
			CHUNK_IDENTIFIER_SIZE + ChunkUtil::chunkSize
		);
	}

	static inline void copy( Chunk *chunk, uint32_t offset, char *src, uint32_t n ) {
		char *dst = ChunkUtil::getData( chunk ) + offset;
		memcpy( dst, src, n );
	}

	static inline void load( Chunk *chunk, uint32_t offset, char *src, uint32_t n ) {
		char *dst = ChunkUtil::getData( chunk );
		if ( offset > 0 )
			memset( dst, 0, offset );
		memcpy( dst + offset, src, n );
		if ( offset + n < ChunkUtil::chunkSize )
			memset( dst + offset + n, 0, ChunkUtil::chunkSize - offset - n );
	}

	static inline void clear( Chunk *chunk ) {
		memset( ( char * ) chunk, 0, CHUNK_IDENTIFIER_SIZE + ChunkUtil::chunkSize );
	}

	static inline void print( Chunk *chunk, FILE *f = stdout ) {
		int width = 21;
		char *data = ChunkUtil::getData( chunk );
		unsigned int hash = HashFunc::hash( data, ChunkUtil::chunkSize );

		fprintf(
			f,
			"---------- %s Chunk (%u, %u, %u) ----------\n"
			"%-*s : 0x%p\n"
			"%-*s : %u\n"
			"%-*s : %u\n"
			"%-*s : %u\n",
			ChunkUtil::isParity( chunk ) ? "Parity" : "Data",
			ChunkUtil::getListId( chunk ),
			ChunkUtil::getStripeId( chunk ),
			ChunkUtil::getChunkId( chunk ),
			width, "Address", ( void * ) chunk,
			width, "Data (Hash)", hash,
			width, "Size", ChunkUtil::getSize( chunk ),
			width, "Count", ChunkUtil::getCount( chunk )
		);

		if ( ! ChunkUtil::isParity( chunk ) ) {
			uint8_t keySize;
			uint32_t valueSize, tmp, splitOffset;
			char *key, *value;
			char *ptr;

			ptr = data;

			while ( ptr + KEY_VALUE_METADATA_SIZE < data + ChunkUtil::chunkSize ) {
				KeyValue::deserialize( ptr, key, keySize, value, valueSize, splitOffset );
				if ( keySize == 0 && valueSize == 0 )
					break;

				fprintf(
					stderr, "[%u, %u, %u] Object: (k: %u, v: %u) at offset: %lu\n",
					ChunkUtil::getListId( chunk ),
					ChunkUtil::getStripeId( chunk ),
					ChunkUtil::getChunkId( chunk ),
					keySize, valueSize, ptr - data
				);

				tmp = KEY_VALUE_METADATA_SIZE + keySize + valueSize;
				ptr += tmp;
			}
		} else {
			uint32_t i;
			for ( i = ChunkUtil::chunkSize - 1; i >= 0; i-- ) {
				if ( data[ i ] != 0 )
					break;
			}
			if ( i == ChunkUtil::chunkSize )
				fprintf(
					stderr, "[%u, %u, %u] No zeros at the end.\n",
					ChunkUtil::getListId( chunk ),
					ChunkUtil::getStripeId( chunk ),
					ChunkUtil::getChunkId( chunk )
				);
			else
				fprintf(
					stderr, "[%u, %u, %u] Zeros at: %u-%u.\n",
					ChunkUtil::getListId( chunk ),
					ChunkUtil::getStripeId( chunk ),
					ChunkUtil::getChunkId( chunk ),
					i + 1, ChunkUtil::chunkSize - 1
				);
		}
	}
};

#endif
