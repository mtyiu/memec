#ifndef __COMMON_STRIPE_LIST_STRIPE_LIST_HH__
#define __COMMON_STRIPE_LIST_STRIPE_LIST_HH__

#include <vector>
#include <cstdio>
#include <cstring>
#include "../ds/bitmask_array.hh"

// Need to know n, k, number of stripe list requested, number of slaves, mapped slaves
template <class T> class StripeList {
private:
	uint32_t n, k;
	size_t numLists, numSlaves;
	bool generated;
	BitmaskArray data;
	BitmaskArray parity;
	unsigned int *weight, *cost;
	std::vector<T> *slaves; // Mapping bitmask to slaves

	inline int pickMin( int listIndex ) {
		int index = 0;
		unsigned int minWeight = this->weight[ 0 ];
		unsigned int minCost = this->cost[ 0 ];
		for ( size_t i = 0; i < this->numSlaves; i++ ) {
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

		int index;
		size_t i, j;

		for ( i = 0; i < this->numLists; i++ ) {
			for ( j = 0; j < this->n - this->k; j++ ) {
				index = pickMin( i );
				printf( "%d ", index );
				this->parity.set( i, index );
				this->weight[ index ] += this->k;
				this->cost[ index ] += 1;
			}
			printf( "/ " );
			for ( j = 0; j < this->k; j++ ) {
				index = pickMin( i );
				printf( "%d ", index );
				this->data.set( i, index );
				this->weight[ index ] += 1;
				this->cost[ index ] += 1;
			}
			printf( "\n" );
		}
		this->generated = true;
	}

public:
	StripeList( uint32_t n, uint32_t k, uint32_t numLists, std::vector<T> &slaves ) : data( slaves.size(), numLists ), parity( slaves.size(), numLists ) {
		this->n = n;
		this->k = k;
		this->numLists = numLists;
		this->numSlaves = slaves.size();
		this->weight = new unsigned int[ numSlaves ];
		this->cost = new unsigned int[ numSlaves ];
		this->slaves = &slaves;

		memset( this->weight, 0, sizeof( unsigned int ) * numSlaves );
		memset( this->cost, 0, sizeof( unsigned int ) * numSlaves );

		this->generate();
	}

	void print( FILE *f = stdout ) {
		size_t i, j;
		bool first;

		if ( ! generated )
			this->generate();

		fprintf( f, "### Stripe List ###\n" );
		for ( i = 0; i < this->numLists; i++ ) {
			first = true;
			fprintf( f, "#%lu: ((", ( i + 1 ) );
			for ( j = 0; j < this->numSlaves; j++ ) {
				if ( this->data.check( i, j ) ) {
					fprintf( f, "%s%lu", first ? "" : ", ", j );
					first = false;
				}
			}
			fprintf( f, "), (" );
			first = true;
			for ( j = 0; j < this->numSlaves; j++ ) {
				if ( this->parity.check( i, j ) ) {
					fprintf( f, "%s%lu", first ? "" : ", ", j );
					first = false;
				}
			}
			fprintf( f, "))\n" );
		}

		fprintf( f, "\n- Weight vector :" );
		for ( size_t i = 0; i < this->numSlaves; i++ )
			fprintf( f, " %d", this->weight[ i ] );
		fprintf( f, "\n- Cost vector   :" );
		for ( size_t i = 0; i < this->numSlaves; i++ )
			fprintf( f, " %d", this->cost[ i ] );

		fprintf( f, "\n- Data Bitmask array:\n" );
		this->data.print( f );
		fprintf( f, "\n- Parity Bitmask array:\n" );
		this->parity.print( f );
	}

	~StripeList() {
		delete[] this->weight;
		delete[] this->cost;
	}
};

#endif
