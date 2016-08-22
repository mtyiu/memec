#ifndef __COMMON_STRIPE_LIST_STRIPE_LIST_HH__
#define __COMMON_STRIPE_LIST_STRIPE_LIST_HH__

#include <vector>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdint.h>
#include "../ds/array_map.hh"
#include "../ds/bitmask_array.hh"
#include "../hash/consistent_hash.hh"
#include "../hash/hash_func.hh"

// #define USE_CONSISTENT_HASHING

#ifndef UINT32_MAX
#define UINT32_MAX 0xFFFFFFFF
#endif

enum GenerationAlgorithm {
	ROUND_ROBIN,
	LOAD_AWARE,
	RANDOM
};

typedef struct {
	uint32_t from;
	uint32_t to;
} HashPartition;

typedef struct {
	uint32_t listId;
	uint32_t stripeId;
	uint32_t chunkId;
	bool isParity;
} StripeListIndex;

// Need to know n, k, number of stripe list requested, number of servers, mapped servers
template <class T> class StripeList {
protected:
	uint32_t n, k, numLists, numServers;
	bool generated;
	GenerationAlgorithm algo;
	BitmaskArray data, parity;
	unsigned int *load, *count;
	std::vector<T *> *servers;
#ifdef USE_CONSISTENT_HASHING
	ConsistentHash<uint32_t> listRing;
#endif
	std::vector<T **> lists, newLists;
	std::vector<HashPartition> partitions;

	inline uint32_t pickNext( int listId, int chunkId ) {
		return ( listId + chunkId ) % this->numServers;
	}

	inline uint32_t pickMin( int listId ) {
		int32_t index = -1;
		uint32_t minLoad = UINT32_MAX;
		uint32_t minCount = UINT32_MAX;
		for ( uint32_t i = 0; i < this->numServers; i++ ) {
			if (
				(
					( this->load[ i ] < minLoad ) ||
					( this->load[ i ] == minLoad && this->count[ i ] < minCount )
				) &&
				! this->data.check( listId, i ) && // The server should not be selected before
				! this->parity.check( listId, i ) // The server should not be selected before
			) {
				minLoad = this->load[ i ];
				minCount = this->count[ i ];
				index = i;
			}
		}
		if ( index == -1 ) {
			fprintf( stderr, "Cannot assign a server for stripe list #%d.", listId );
			return 0;
		}
		return ( uint32_t ) index;
	}

	inline uint32_t pickRand( int listId ) {
		int32_t index = -1;
		uint32_t i;
		 while( index == -1 ) {
			i = rand() % this->numServers;
			if (
				! this->data.check( listId, i ) && // The server should not be selected before
				! this->parity.check( listId, i ) // The server should not be selected before
			) {
				index = i;
			}
		}
		if ( index == -1 ) {
			fprintf( stderr, "Cannot assign a server for stripe list #%d.", listId );
			return 0;
		}
		return ( uint32_t ) index;
	}

	void generate( bool verbose = false, GenerationAlgorithm algo = ROUND_ROBIN ) {
		if ( generated )
			return;

		uint32_t i, j, index, dataCount, parityCount;

		for ( i = 0; i < this->numLists; i++ ) {
			T **list = this->lists[ i ];
			for ( j = 0; j < this->n - this->k; j++ ) {
				switch ( algo ) {
					case ROUND_ROBIN : index = pickNext( i, j + this->k ); break;
					case LOAD_AWARE  : index = pickMin( i );               break;
					case RANDOM      : index = pickRand( i );              break;
				}
				this->parity.set( i, index );
			}
			for ( j = 0; j < this->k; j++ ) {
				switch ( algo ) {
					case ROUND_ROBIN : index = pickNext( i, j ); break;
					case LOAD_AWARE  : index = pickMin( i );     break;
					case RANDOM      : index = pickRand( i );    break;
				}
				this->data.set( i, index );
			}

			// Implicitly sort the item
			dataCount = 0;
			parityCount = 0;
			for ( j = 0; j < this->numServers; j++ ) {
				if ( this->data.check( i, j ) ) {
					list[ dataCount++ ] = this->servers->at( j );
					this->load[ j ] += 1;
					this->count[ j ]++;
				} else if ( this->parity.check( i, j ) ) {
					list[ this->k + ( parityCount++ ) ] = this->servers->at( j );
					this->load[ j ] += this->k;
					this->count[ j ]++;
				}
			}

#ifdef USE_CONSISTENT_HASHING
			this->listRing.add( i );
#endif
		}

		this->generated = true;
		this->assign();
	}

	void assign() {
		uint32_t size = UINT32_MAX / this->numLists;
		for ( uint32_t i = 0; i < this->numLists; i++ ) {
			HashPartition partition = {
				.from = i * size,
				.to = ( i == this->numLists - 1 ) ? UINT32_MAX : ( ( i + 1 ) * size - 1 )
			};
			this->partitions.push_back( partition );
		}
	}

	uint32_t getListId( uint32_t hashValue ) {
		for ( uint32_t i = 0; i < this->numLists; i++ ) {
			if ( this->partitions[ i ].from <= hashValue && this->partitions[ i ].to > hashValue )
				return i;
		}
		fprintf( stderr, "StripeList::getListId(): Cannot get a partition for this hash value: %u.\n", hashValue );
		return -1;
	}

public:
	StripeList( uint32_t n, uint32_t k, uint32_t numLists, std::vector<T *> &servers, GenerationAlgorithm algo = ROUND_ROBIN ) : data( servers.size(), numLists ), parity( servers.size(), numLists ) {
		this->n = n;
		this->k = k;
		this->numLists = numLists;
		this->numServers = servers.size();
		this->generated = false;
		this->algo = algo;
		this->load = new unsigned int[ numServers ];
		this->count = new unsigned int[ numServers ];
		this->servers = &servers;
		this->lists.reserve( numLists );
		for ( uint32_t i = 0; i < numLists; i++ )
			this->lists.push_back( new T*[ n ] );

		memset( this->load, 0, sizeof( unsigned int ) * numServers );
		memset( this->count, 0, sizeof( unsigned int ) * numServers );

		this->generate( false /* verbose */, algo );
	}

	unsigned int get( const char *key, uint8_t keySize, T **data = 0, T **parity = 0, uint32_t *dataIndexPtr = 0, bool full = false ) {
		unsigned int hashValue = HashFunc::hash( key, keySize );
		uint32_t dataIndex = hashValue % this->k;
#ifdef USE_CONSISTENT_HASHING
		uint32_t listId = this->listRing.get( key, keySize );
#else
		uint32_t listId = this->getListId( HashFunc::hash( ( char * ) &hashValue, sizeof( hashValue ) ) );
#endif
		T **ret = this->lists[ listId ];

		if ( dataIndexPtr )
			*dataIndexPtr = dataIndex;

		if ( parity ) {
			for ( uint32_t i = 0; i < this->n - this->k; i++ ) {
				parity[ i ] = ret[ this->k + i ];
			}
		}
		if ( data ) {
			if ( full ) {
				for ( uint32_t i = 0; i < this->k; i++ ) {
					data[ i ] = ret[ i ];
				}
			} else {
				*data = ret[ dataIndex ];
			}
		}
		return listId;
	}

	unsigned int getByHash( unsigned int hashValue, T **data, T **parity ) {
#ifdef USE_CONSISTENT_HASHING
		uint32_t listId = this->listRing.get( hashValue );
#else
		uint32_t listId = this->getListId( HashFunc::hash( ( char * ) &hashValue, sizeof( hashValue ) ) );
#endif
		T **ret = this->lists[ listId ];

		for ( uint32_t i = 0; i < this->n - this->k; i++ )
			parity[ i ] = ret[ this->k + i ];
		for ( uint32_t i = 0; i < this->k; i++ )
			data[ i ] = ret[ i ];
		return listId;
	}

	T *get( uint32_t listId, uint32_t dataIndex, uint32_t jump, uint32_t *serverIndex = 0 ) {
		T **ret = this->lists[ listId ];
		unsigned int index = HashFunc::hash( ( char * ) &dataIndex, sizeof( dataIndex ) );
		index = ( index + jump ) % this->n;
		if ( serverIndex )
			*serverIndex = index;
		return ret[ index ];
	}

	T *get( uint32_t listId, uint32_t chunkId ) {
		T **ret = this->lists[ listId ];
		return ret[ chunkId ];
	}

	T **get( uint32_t listId, T **parity, T **data = 0 ) {
		T **ret = this->lists[ listId ];
		for ( uint32_t i = 0; i < this->n - this->k; i++ ) {
			parity[ i ] = ret[ this->k + i ];
		}
		if ( data ) {
			for ( uint32_t i = 0; i < this->k; i++ ) {
				data[ i ] = ret[ i ];
			}
		}
		return parity;
	}

	std::vector<StripeListIndex> list( uint32_t index ) {
		uint32_t i, j;
		std::vector<StripeListIndex> ret;
		for ( i = 0; i < this->numLists; i++ ) {
			if ( this->data.check( i, index ) ) {
				StripeListIndex s;
				s.listId = i;
				s.stripeId = 0;
				s.chunkId = 0;
				s.isParity = false;
				for ( j = 0; j < this->numServers; j++ ) {
					if ( j == index )
						break;
					if ( this->data.check( i, j ) )
						s.chunkId++;
				}
				ret.push_back( s );
			} else if ( this->parity.check( i, index ) ) {
				StripeListIndex s;
				s.listId = i;
				s.stripeId = 0;
				s.chunkId = this->k;
				s.isParity = true;
				for ( j = 0; j < this->numServers; j++ ) {
					if ( j == index )
						break;
					if ( this->parity.check( i, j ) )
						s.chunkId++;
				}
				ret.push_back( s );
			}
		}
		return ret;
	}

	void update() {
		if ( ! this->generated ) {
			this->generate();
			return;
		}

		uint32_t i, j, dataCount, parityCount;

		for ( i = 0; i < this->numLists; i++ ) {
			T **list = this->lists[ i ];
			dataCount = 0;
			parityCount = 0;
			for ( j = 0; j < this->numServers; j++ ) {
				if ( this->data.check( i, j ) ) {
					list[ dataCount++ ] = this->servers->at( j );
				} else if ( this->parity.check( i, j ) ) {
					list[ this->k + ( parityCount++ ) ] = this->servers->at( j );
				}
			}
		}
	}

	int32_t search( T *target ) {
		for ( uint32_t i = 0; i < this->numServers; i++ ) {
			if ( target == this->servers->at( i ) )
				return i;
		}
		return -1;
	}

	uint32_t getNumList() {
		return this->numLists;
	}

	void print( FILE *f = stdout ) {
		uint32_t i, j;
		bool first;

		if ( ! generated ) {
			fprintf( f, "The stripe lists are not generated yet.\n" );
			return;
		}

		fprintf( f, "### Stripe List (%s) ###\n", this->algo == ROUND_ROBIN ? "Round-robin" : ( this->algo == LOAD_AWARE ? "Load-aware" : "Random" ) );
		for ( i = 0; i < this->numLists; i++ ) {
			first = true;
			fprintf( f, "#%u [%10u-%10u]: ((", i, this->partitions[ i ].from, this->partitions[ i ].to );
			for ( j = 0; j < this->numServers; j++ ) {
				if ( this->data.check( i, j ) ) {
					fprintf( f, "%s%u", first ? "" : ", ", j );
					first = false;
				}
			}
			fprintf( f, "), (" );
			first = true;
			for ( j = 0; j < this->numServers; j++ ) {
				if ( this->parity.check( i, j ) ) {
					fprintf( f, "%s%u", first ? "" : ", ", j );
					first = false;
				}
			}
			fprintf( f, "))\n" );
		}

		fprintf( f, "\n- Weight vector :" );
		for ( uint32_t i = 0; i < this->numServers; i++ )
			fprintf( f, " %d", this->load[ i ] );
		fprintf( f, "\n- Cost vector   :" );
		for ( uint32_t i = 0; i < this->numServers; i++ )
			fprintf( f, " %d", this->count[ i ] );

		fprintf( f, "\n\n" );

#ifdef USE_CONSISTENT_HASHING
		this->listRing.print( f );

		fprintf( f, "\n" );
#endif
	}

	~StripeList() {
		delete[] this->load;
		delete[] this->count;
		for ( uint32_t i = 0; i < this->numLists; i++ )
			delete[] this->lists[ i ];
	}
};

#endif
