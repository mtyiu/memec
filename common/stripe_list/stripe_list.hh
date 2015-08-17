#ifndef __COMMON_STRIPE_LIST_STRIPE_LIST_HH__
#define __COMMON_STRIPE_LIST_STRIPE_LIST_HH__

#include <vector>
#include <cstdio>
#include <cstring>
#include <cassert>
#include "../ds/array_map.hh"
#include "../ds/bitmask_array.hh"
#include "../hash/consistent_hash.hh"
#include "../hash/hash_func.hh"

typedef struct {
	int listId;
	int stripeId;
	int chunkId;
	bool isParity;
} StripeListIndex;

// Need to know n, k, number of stripe list requested, number of slaves, mapped slaves
template <class T> class StripeList {
protected:
	uint32_t n, k, numLists, numSlaves;
	bool generated;
	BitmaskArray data, parity;
	unsigned int *weight, *cost;
	std::vector<T> *slaves;
	ConsistentHash<uint32_t> listRing;
	std::vector<T **> lists;

	inline uint32_t pickMin( int listIndex ) {
		uint32_t index = 0;
		unsigned int minWeight = this->weight[ 0 ];
		unsigned int minCost = this->cost[ 0 ];
		for ( uint32_t i = 0; i < this->numSlaves; i++ ) {
			if (
				(
					( this->weight[ i ] < minWeight ) ||
					( this->weight[ i ] == minWeight && this->cost[ i ] < minCost )
				) &&
				! this->data.check( listIndex, i ) && // The slave should not be selected before
				! this->parity.check( listIndex, i ) // The slave should not be selected before
			) {
				minWeight = this->weight[ i ];
				minCost = this->cost[ i ];
				index = i;
			}
		}
		return index;
	}

	void generate( bool verbose = false ) {
		if ( generated )
			return;

		uint32_t index;
		uint32_t i, j;

		for ( i = 0; i < this->numLists; i++ ) {
			T **list = this->lists[ i ];
			for ( j = 0; j < this->n - this->k; j++ ) {
				index = pickMin( i );
				this->parity.set( i, index );
				this->weight[ index ] += this->k;
				this->cost[ index ] += 1;

				list[ this->k + j ] = &this->slaves->at( index );
			}
			for ( j = 0; j < this->k; j++ ) {
				index = pickMin( i );
				this->data.set( i, index );
				this->weight[ index ] += 1;
				this->cost[ index ] += 1;

				list[ j ] = &this->slaves->at( index );
			}
			this->listRing.add( i );
		}

		this->generated = true;
	}

public:
	StripeList( uint32_t n, uint32_t k, uint32_t numLists, std::vector<T> &slaves ) : data( slaves.size(), numLists ), parity( slaves.size(), numLists ) {
		this->n = n;
		this->k = k;
		this->numLists = numLists;
		this->numSlaves = slaves.size();
		this->generated = false;
		this->weight = new unsigned int[ numSlaves ];
		this->cost = new unsigned int[ numSlaves ];
		this->slaves = &slaves;
		this->lists.reserve( numLists );
		for ( uint32_t i = 0; i < numLists; i++ )
			this->lists.push_back( new T*[ n ] );

		memset( this->weight, 0, sizeof( unsigned int ) * numSlaves );
		memset( this->cost, 0, sizeof( unsigned int ) * numSlaves );

		this->generate();
	}

	unsigned int get( const char *key, uint8_t keySize, T **data = 0, T **parity = 0, uint32_t *dataIndexPtr = 0, bool full = false ) {
		uint32_t dataIndex = HashFunc::hash( key, keySize ) % this->k;
		uint32_t listIndex = this->listRing.get( key, keySize );
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

	T *get( uint32_t listIndex, uint32_t dataIndex, uint32_t jump, uint32_t *slaveIndex = 0 ) {
		T **ret = this->lists[ listIndex ];
		unsigned int index = HashFunc::hash( ( char * ) &dataIndex, sizeof( dataIndex ) );
		index = ( index + jump ) % this->n;
		if ( slaveIndex )
			*slaveIndex = index;
		return ret[ index ];
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

	void print( FILE *f = stdout ) {
		uint32_t i, j;
		bool first;

		if ( ! generated )
			this->generate();

		fprintf( f, "### Stripe List ###\n" );
		for ( i = 0; i < this->numLists; i++ ) {
			first = true;
			fprintf( f, "#%u: ((", ( i + 1 ) );
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
			fprintf( f, " %d", this->weight[ i ] );
		fprintf( f, "\n- Cost vector   :" );
		for ( uint32_t i = 0; i < this->numSlaves; i++ )
			fprintf( f, " %d", this->cost[ i ] );

		fprintf( f, "\n" );
	}

	~StripeList() {
		delete[] this->weight;
		delete[] this->cost;
		for ( uint32_t i = 0; i < this->numLists; i++ )
			delete[] this->lists[ i ];
	}
};

#endif
