#ifndef __COMMON_DS_CHUNK_POOL_HH__
#define __COMMON_DS_CHUNK_POOL_HH__

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cassert>
#include "../../common/coding/coding.hh"
#include "../../common/ds/chunk.hh"
#include "../../common/hash/hash_func.hh"

struct ChunkMetadata {
	uint32_t listId;
	uint32_t stripeId;
	uint32_t chunkId;
	uint32_t size;
} __attribute__((__packed__));

#define CHUNK_METADATA_SIZE sizeof( struct ChunkMetadata )

class ChunkUtil {
public:
	static uint32_t chunkSize;
	static uint32_t dataChunkCount;

	static inline void init( uint32_t chunkSize, uint32_t dataChunkCount ) {
		ChunkUtil::chunkSize = chunkSize;
		ChunkUtil::dataChunkCount = dataChunkCount;
	}

	// Getters
	static inline void get( Chunk *chunk, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, uint32_t &size ) {
		struct ChunkMetadata *chunkMetadata = ( struct ChunkMetadata * ) chunk;
		listId   = chunkMetadata->listId;
		stripeId = chunkMetadata->stripeId;
		chunkId  = chunkMetadata->chunkId;
		size     = chunkMetadata->size;
	}
	static inline Metadata getMetadata( Chunk *chunk ) {
		struct ChunkMetadata *chunkMetadata = ( struct ChunkMetadata * ) chunk;
		Metadata metadata;
		metadata.set(
			chunkMetadata->listId,
			chunkMetadata->stripeId,
			chunkMetadata->chunkId
		);
		return metadata;
	}
	static inline uint32_t getListId( Chunk *chunk ) {
		return ( ( struct ChunkMetadata * ) chunk )->listId;
	}
	static inline uint32_t getStripeId( Chunk *chunk ) {
		return ( ( struct ChunkMetadata * ) chunk )->stripeId;
	}
	static inline uint32_t getChunkId( Chunk *chunk ) {
		return ( ( struct ChunkMetadata * ) chunk )->chunkId;
	}
	static inline uint32_t getSize( Chunk *chunk ) {
		return ( ( struct ChunkMetadata * ) chunk )->size;
	}
	static inline uint32_t getCount( Chunk *chunk ) {
		if ( ChunkUtil::isParity( chunk ) ) {
			return 0;
		} else {
			// Scan whole chunk
			uint8_t keySize;
			uint32_t valueSize, tmp;
			char *key, *value;
			char *data, *ptr;
			uint32_t count = 0;

			data = ptr = ChunkUtil::getData( chunk );

			while ( ptr < data + ChunkUtil::chunkSize ) {
				KeyValue::deserialize( ptr, key, keySize, value, valueSize );
				if ( keySize == 0 && valueSize == 0 )
					break;

				tmp = KEY_VALUE_METADATA_SIZE + keySize + valueSize;
				ptr += tmp;

				count++;
			}

			return count;
		}
	}
	static inline char *getData( Chunk *chunk ) {
		return ( char * ) chunk + CHUNK_METADATA_SIZE;
	}
	static inline char *getData( Chunk *chunk, uint32_t &offset, uint32_t &size ) {
		uint32_t chunkSize = ChunkUtil::isParity( chunk ) ? ChunkUtil::chunkSize : ChunkUtil::getSize( chunk );
		char *data = ChunkUtil::getData( chunk );

		if ( chunkSize > 0 ) {
			for ( offset = 0; offset < chunkSize; offset++ ) {
				if ( data[ offset ] != 0 )
					break;
			}

			for ( size = chunkSize - 1; size > offset; size-- ) {
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
		keyValue.data = ( char * ) chunk + CHUNK_METADATA_SIZE + offset;
		return keyValue;
	}
	static inline int next( Chunk *chunk, uint32_t offset, char *&key, uint8_t &keySize ) {
		char *data = ChunkUtil::getData( chunk );
		char *ptr = data + offset, *value;
		int ret = -1;
		uint32_t valueSize;

		if ( ptr < data + ChunkUtil::chunkSize ) {
			KeyValue::deserialize( ptr, key, keySize, value, valueSize );
			if ( keySize )
				ret = offset + KEY_VALUE_METADATA_SIZE + keySize + valueSize;
			else
				key = 0;
		}
		return ret;
	}

	// Setters
	static inline void set( Chunk *chunk, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t size ) {
		struct ChunkMetadata *chunkMetadata = ( struct ChunkMetadata * ) chunk;
		chunkMetadata->listId   = listId;
		chunkMetadata->stripeId = stripeId;
		chunkMetadata->chunkId  = chunkId;
		chunkMetadata->size     = size;
	}
	static inline void setListId( Chunk *chunk, uint32_t listId ) {
		( ( struct ChunkMetadata * ) chunk )->listId = listId;
	}
	static inline void setStripeId( Chunk *chunk, uint32_t stripeId ) {
		( ( struct ChunkMetadata * ) chunk )->stripeId = stripeId;
	}
	static inline void setChunkId( Chunk *chunk, uint32_t chunkId ) {
		( ( struct ChunkMetadata * ) chunk )->chunkId = chunkId;
	}
	static inline void setSize( Chunk *chunk, uint32_t size ) {
		( ( struct ChunkMetadata * ) chunk )->size = size;
	}
	static inline uint32_t updateSize( Chunk *chunk ) {
		uint32_t size = 0;
		if ( ChunkUtil::isParity( chunk ) ) {
			size = ChunkUtil::chunkSize;
		} else {
			// Scan whole chunk
			uint8_t keySize;
			uint32_t valueSize, tmp;
			char *key, *value;
			char *data, *ptr;

			data = ptr = ChunkUtil::getData( chunk );

			while ( ptr < data + ChunkUtil::chunkSize ) {
				KeyValue::deserialize( ptr, key, keySize, value, valueSize );
				if ( keySize == 0 && valueSize == 0 )
					break;

				tmp = KEY_VALUE_METADATA_SIZE + keySize + valueSize;
				size += tmp;
				ptr += tmp;
			}
		}
		ChunkUtil::setSize( chunk, size );
		return size;
	}

	// Memory allocator for objects
	static inline char *alloc( Chunk *chunk, uint32_t size, uint32_t &offset ) {
		struct ChunkMetadata *chunkMetadata = ( struct ChunkMetadata * ) chunk;
		offset = chunkMetadata->size;
		chunkMetadata->size += size;
		return ( ( char * ) chunk ) + CHUNK_METADATA_SIZE + offset;
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
		uint32_t valueSize, length;
		char *key, *value;

		KeyValue::deserialize( data, key, keySize, value, valueSize );
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
			CHUNK_METADATA_SIZE + ChunkUtil::chunkSize
		);
	}

	static inline void copy( Chunk *chunk, uint32_t offset, char *src, uint32_t n ) {
		char *dst = ChunkUtil::getData( chunk ) + offset;
		memcpy( dst, src, n );
	}

	static inline void load( Chunk *chunk, uint32_t offset, char *src, uint32_t n ) {
		char *dst = ChunkUtil::getData( chunk );
		bool isParity = ChunkUtil::isParity( chunk );

		ChunkUtil::setSize( chunk, isParity ? ChunkUtil::chunkSize : n );

		if ( offset > 0 )
			memset( dst, 0, offset );
		memcpy( dst + offset, src, n );
		if ( offset + n < ChunkUtil::chunkSize )
			memset( dst + offset + n, 0, ChunkUtil::chunkSize - offset - n );
	}

	static inline void clear( Chunk *chunk ) {
		memset( ( char * ) chunk, 0, CHUNK_METADATA_SIZE + ChunkUtil::chunkSize );
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
	}
};

class ChunkPool {
private:
	uint32_t total;                  // Number of chunks allocated
	std::atomic<unsigned int> count; // Current index
	char *startAddress;

public:
	ChunkPool();
	~ChunkPool();

	void init( uint32_t chunkSize, uint64_t capacity );

	Chunk *alloc();
	Chunk *alloc( uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t size );

	// Translate object pointer to chunk pointer
	Chunk *getChunk( char *ptr, uint32_t &offset );

	// Check whether the chunk is allocated by this chunk pool
	bool isInChunkPool( Chunk *chunk );

	void print( FILE *f = stdout );
};

class TempChunkPool {
public:
	Chunk *alloc() {
		Chunk *ret = ( Chunk * ) malloc( CHUNK_METADATA_SIZE + ChunkUtil::chunkSize );
		if ( ret )
			ChunkUtil::clear( ret );
		return ret;
	}

	Chunk *alloc( uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t size ) {
		Chunk *ret = this->alloc();
		ChunkUtil::set( ret, listId, stripeId, chunkId, size );
		return ret;
	}

	void free( Chunk *chunk ) {
		::free( ( char * ) chunk );
	}
};

#endif
