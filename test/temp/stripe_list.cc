#include <map>
#include <string>
#include <sstream>
#include <vector>
#include <cstdio>
#include <cstdlib>
#include <cstring>

using namespace std;

class StripeList {
public:
	int n;
	int k;
	int *data;
	int *parity;

	StripeList( int n, int k ) {
		this->n = n;
		this->k = k;
		this->data = new int[ k ];
		this->parity = new int[ n - k ];
	}

	~StripeList() {
		delete[] this->data;
		delete[] this->parity;
	}

	void print() {
		int i;

		printf( "((" );
		for ( i = 0; i < this->k; i++ ) {
			printf( "%s%d", i > 0 ? ", " : "", this->data[ i ] + 1 );
		}
		printf( "), (" );
		for ( i = 0; i < this->n - this->k; i++ ) {
			printf( "%s%d", i > 0 ? ", " : "", this->parity[ i ] + 1 );
		}
		printf( "))" );
	}

	string serialize() {
		int i;
		stringstream ss;
		ss << "((";
		for ( i = 0; i < this->k; i++ )
			ss << ( i > 0 ? ", " : "" ) << ( this->data[ i ] + 1 );
		ss << "), (";
		for ( i = 0; i < this->n - this->k; i++ )
			ss << ( i > 0 ? ", " : "" ) << ( this->parity[ i ] + 1 );
		ss << "))";
		return ss.str();
	}
};

int pickMin( int *weight, int numSlaves, int *selected, int selectedCount ) {
	bool isSelected = false;
	int i, j, min = weight[ 0 ], index = 0;
	for ( i = 0; i < numSlaves; i++ ) {
		isSelected = false;
		if ( weight[ i ] < min ) {
			for ( j = 0; j < selectedCount; j++ ) {
				if ( i == selected[ j ] ) {
					isSelected = true;
					break;
				}
			}
			if ( ! isSelected ) {
				min = weight[ i ];
				index = i;
			}
		}
	}
	return index;
}

StripeList **generate( int numLists, int numSlaves, int n, int k, bool verbose = false ) {
	int i, j, min, max;
	double average;
	string serializedList;
	map<string, int> count;
	map<string, int>::iterator it;
	int **load = new int*[ numLists ];
	int **cost = new int*[ numLists ];
	StripeList **list = new StripeList*[ numLists ];

	for ( i = 0; i < numLists; i++ ) {
		load[ i ] = new int[ numSlaves ];
		cost[ i ] = new int[ numSlaves ];
		memset( load[ i ], 0, sizeof( int ) * numSlaves );
		memset( cost[ i ], 0, sizeof( int ) * numSlaves );
	}

	for ( i = 0; i < numLists; i++ ) {
		list[ i ] = new StripeList( n, k );
		if ( i > 0 ) {
			// Copy previous load and cost
			memcpy( load[ i ], load[ i - 1 ], sizeof( int ) * numSlaves );
			memcpy( cost[ i ], cost[ i - 1 ], sizeof( int ) * numSlaves );
		}
		for ( j = 0; j < n - k; j++ ) {
			list[ i ]->parity[ j ] = pickMin( load[ i ], numSlaves, list[ i ]->parity, j );
			load[ i ][ list[ i ]->parity[ j ] ] += k;
			cost[ i ][ list[ i ]->parity[ j ] ] += 1;
		}
		for ( j = 0; j < k; j++ ) {
			list[ i ]->data[ j ] = pickMin( load[ i ], numSlaves, list[ i ]->data, j );
			load[ i ][ list[ i ]->data[ j ] ] += 1;
			cost[ i ][ list[ i ]->data[ j ] ] += 1;
		}
		serializedList = list[ i ]->serialize();
		it = count.find( serializedList );
		if ( it != count.end() )
			it->second++;
		else
			count[ serializedList ] = 1;

		printf( "L%d: ", i + 1 );
		list[ i ]->print();
		printf( "\n" );
	}
	printf( "\n" );

	printf( "Load:\n" );
	for ( i = verbose ? 0 : numLists - 1; i < numLists; i++ ) {
		if ( verbose )
			printf( "#%d: ", i );
		min = load[ i ][ 0 ];
		max = load[ i ][ 0 ];
		average = 0.0;
		for( j = 0; j < numSlaves; j++ ) {
			printf( "%d ", load[ i ][ j ] );
			min = load[ i ][ j ] < min ? load[ i ][ j ] : min;
			max = load[ i ][ j ] > max ? load[ i ][ j ] : max;
			average += load[ i ][ j ];
		}
		average /= numSlaves;
		printf( " (min: %d, max: %d, average: %.1lf)\n", min, max, average );
		if ( max - min > k ) {
			printf( "*** Property violated: max - min > k ! ***\n" );
			goto terminate;
		}
	}
	printf( "\n" );

	/*
	printf( "Storage cost:\n" );
	for ( i = verbose ? 0 : numLists - 1; i < numLists; i++ ) {
		if ( verbose )
			printf( "#%d: ", i );
		min = cost[ i ][ 0 ];
		max = cost[ i ][ 0 ];
		average = 0.0;
		for( j = 0; j < numSlaves; j++ ) {
			printf( "%d ", cost[ i ][ j ] );
			min = cost[ i ][ j ] < min ? cost[ i ][ j ] : min;
			max = cost[ i ][ j ] > max ? cost[ i ][ j ] : max;
			average += cost[ i ][ j ];
		}
		average /= numSlaves;
		printf( " (min: %d, max: %d, average: %.1lf)\n", min, max, average );
	}
	printf( "\n" );
	*/

	printf( "Number of unique stripe list:\n" );
	printf( "%lu\n\n", count.size() );

	printf( "Number of repetitions:\n" );
	min = 1;
	max = 1;
	average = 0.0;
	for ( it = count.begin(); it != count.end(); it++ ) {
		min = it->second < min ? it->second : min;
		max = it->second > max ? it->second : max;
		average += it->second;
		printf( "%d ", it->second );
	}
	average /= ( int ) count.size();
	printf( " (min: %d, max: %d, average: %.1lf)\n", min, max, average );

terminate:
	for ( i = 0; i < numLists; i++ ) {
		delete[] load[ i ];
		delete[] cost[ i ];
	}
	delete[] load;
	delete[] cost;

	return list;
}

int main( int argc, char **argv ) {
	if ( argc <= 4 )
		goto usage;

	int i, numLists, numSlaves, n, k;
	StripeList **list;

	numLists = atoi( argv[ 1 ] );
	numSlaves = atoi( argv[ 2 ] );
	n = atoi( argv[ 3 ] );
	k = atoi( argv[ 4 ] );
	if ( numLists < 1 || numSlaves < n || n < k ) {
		fprintf( stderr, "Invalid parameters.\n" );
		goto usage;
	}
	
	list = generate( numLists, numSlaves, n, k, true );
	for ( i = 0; i < numLists; i++ )
		delete list[ i ];
	delete[] list;

	return 0;

usage:
	fprintf( stderr, "Usage: %s [Number of stripe lists] [Number of slaves] [n] [k]\n", argv[ 0 ] );
	return 1;
}
