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

typedef struct {
	uint32_t listId;
	uint32_t stripeId;
	uint32_t chunkId;
	bool isParity;
} StripeListIndex;

// Need to know n, k, number of stripe list requested, number of servers, mapped servers
template <class T> class StripeList {
protected:
	uint32_t n, k, numLists, numSlaves;
	bool generated, useAlgo;
	BitmaskArray data, parity;
	unsigned int *load, *count;
	std::vector<T *> *servers;
#ifdef USE_CONSISTENT_HASHING
	ConsistentHash<uint32_t> listRing;
#endif
	std::vector<T **> lists;

	inline uint32_t pickMin( int listIndex ) {
		int32_t index = -1;
		uint32_t minLoad = UINT32_MAX;
		uint32_t minCount = UINT32_MAX;
		for ( uint32_t i = 0; i < this->numSlaves; i++ ) {
			if (
				(
					( this->load[ i ] < minLoad ) ||
					( this->load[ i ] == minLoad && this->count[ i ] < minCount )
				) &&
				! this->data.check( listIndex, i ) && // The server should not be selected before
				! this->parity.check( listIndex, i ) // The server should not be selected before
			) {
				minLoad = this->load[ i ];
				minCount = this->count[ i ];
				index = i;
			}
		}
		if ( index == -1 ) {
			fprintf( stderr, "Cannot assign a server for stripe list #%d.", listIndex );
			return 0;
		}
		return ( uint32_t ) index;
	}

	inline uint32_t pickRand( int listIndex ) {
		int32_t index = -1;
		uint32_t i;
		 while( index == -1 ) {
			i = rand() % this->numSlaves;
			if (
				! this->data.check( listIndex, i ) && // The server should not be selected before
				! this->parity.check( listIndex, i ) // The server should not be selected before
			) {
				index = i;
			}
		}
		if ( index == -1 ) {
			fprintf( stderr, "Cannot assign a server for stripe list #%d.", listIndex );
			return 0;
		}
		return ( uint32_t ) index;
	}

	void generate( bool verbose = false, bool useAlgo = true ) {
		if ( generated )
			return;

		uint32_t i, j, index, dataCount, parityCount;

		for ( i = 0; i < this->numLists; i++ ) {
			T **list = this->lists[ i ];
			for ( j = 0; j < this->n - this->k; j++ ) {
				index = useAlgo ? pickMin( i ) : pickRand( i );
				this->parity.set( i, index );
			}
			for ( j = 0; j < this->k; j++ ) {
				index = useAlgo ? pickMin( i ) : pickRand( i );
				this->data.set( i, index );
			}

			// Implicitly sort the item
			dataCount = 0;
			parityCount = 0;
			for ( j = 0; j < this->numSlaves; j++ ) {
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
	}

public:
	StripeList( uint32_t n, uint32_t k, uint32_t numLists, std::vector<T *> &servers, bool useAlgo = true ) : data( servers.size(), numLists ), parity( servers.size(), numLists ) {
		this->n = n;
		this->k = k;
		this->numLists = numLists;
		this->numSlaves = servers.size();
		this->generated = false;
		this->useAlgo = useAlgo;
		this->load = new unsigned int[ numSlaves ];
		this->count = new unsigned int[ numSlaves ];
		this->servers = &servers;
		this->lists.reserve( numLists );
		for ( uint32_t i = 0; i < numLists; i++ )
			this->lists.push_back( new T*[ n ] );

		memset( this->load, 0, sizeof( unsigned int ) * numSlaves );
		memset( this->count, 0, sizeof( unsigned int ) * numSlaves );

		this->generate( false /* verbose */, useAlgo );
	}

	unsigned int get( const char *key, uint8_t keySize, T **data = 0, T **parity = 0, uint32_t *dataIndexPtr = 0, bool full = false ) {
		unsigned int hashValue = HashFunc::hash( key, keySize );
		uint32_t dataIndex = hashValue % this->k;
#ifdef USE_CONSISTENT_HASHING
		uint32_t listIndex = this->listRing.get( key, keySize );
#else
		uint32_t listIndex = HashFunc::hash( ( char * ) &hashValue, sizeof( hashValue ) ) % this->numLists;
#endif
		T **ret = this->lists[ listIndex ];

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
		return listIndex;
	}

	unsigned int getByHash( unsigned int hashValue, T **data, T **parity ) {
#ifdef USE_CONSISTENT_HASHING
		uint32_t listIndex = this->listRing.get( hashValue );
#else
		uint32_t listIndex = HashFunc::hash( ( char * ) &hashValue, sizeof( hashValue ) ) % this->numLists;
#endif
		T **ret = this->lists[ listIndex ];

		for ( uint32_t i = 0; i < this->n - this->k; i++ )
			parity[ i ] = ret[ this->k + i ];
		for ( uint32_t i = 0; i < this->k; i++ )
			data[ i ] = ret[ i ];
		return listIndex;
	}

	T *get( uint32_t listIndex, uint32_t dataIndex, uint32_t jump, uint32_t *serverIndex = 0 ) {
		T **ret = this->lists[ listIndex ];
		unsigned int index = HashFunc::hash( ( char * ) &dataIndex, sizeof( dataIndex ) );
		index = ( index + jump ) % this->n;
		if ( serverIndex )
			*serverIndex = index;
		return ret[ index ];
	}

	T *get( uint32_t listIndex, uint32_t chunkIndex ) {
		T **ret = this->lists[ listIndex ];
		return ret[ chunkIndex ];
	}

	T **get( uint32_t listIndex, T **parity, T **data = 0 ) {
		T **ret = this->lists[ listIndex ];
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
				for ( j = 0; j < this->numSlaves; j++ ) {
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
				for ( j = 0; j < this->numSlaves; j++ ) {
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
			for ( j = 0; j < this->numSlaves; j++ ) {
				if ( this->data.check( i, j ) ) {
					list[ dataCount++ ] = this->servers->at( j );
				} else if ( this->parity.check( i, j ) ) {
					list[ this->k + ( parityCount++ ) ] = this->servers->at( j );
				}
			}
		}
	}

	int32_t search( T *target ) {
		for ( uint32_t i = 0; i < this->numSlaves; i++ ) {
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

		fprintf( f, "### Stripe List (%s) ###\n", this->useAlgo ? "Load-aware" : "Random" );
		for ( i = 0; i < this->numLists; i++ ) {
			first = true;
			fprintf( f, "#%u: ((", i );
			for ( j = 0; j < this->numSlaves; j++ ) {
				if ( this->data.check( i, j ) ) {
					fprintf( f, "%s%u", first ? "" : ", ", j );
					first = false;
				}
			}
			fprintf( f, "), (" );
			first = true;
			for ( j = 0; j < this->numSlaves; j++ ) {
				if ( this->parity.check( i, j ) ) {
					fprintf( f, "%s%u", first ? "" : ", ", j );
					first = false;
				}
			}
			fprintf( f, "))\n" );
		}

		fprintf( f, "\n- Weight vector :" );
		for ( uint32_t i = 0; i < this->numSlaves; i++ )
			fprintf( f, " %d", this->load[ i ] );
		fprintf( f, "\n- Cost vector   :" );
		for ( uint32_t i = 0; i < this->numSlaves; i++ )
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
