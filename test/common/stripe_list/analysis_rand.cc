#include <vector>
#include <cstdio>
#include <cmath>
#include <cassert>
#include "../../../common/stripe_list/stripe_list.hh"

#define METRIC_LOAD         0
#define METRIC_STORAGE_COST 1
#define METRIC_NUM_KEYS     2
#define METRIC_COUNT        3

#define FAIRNESS_MAX_MIN_AVG 0
#define FAIRNESS_STDEV       1
#define FAIRNESS_JAINS       2
#define FAIRNESS_COUNT       3

////////////////////////////////////////////////////////////////////////////////

uint32_t n, k, M, cFrom, cTo, iters;
char *loadFilename, *rand10Filename, *rand50Filename, *rand90Filename;

////////////////////////////////////////////////////////////////////////////////

class ServerNode {
private:
	uint32_t id;
	uint32_t metric[ METRIC_COUNT ];
	uint32_t _metric[ METRIC_COUNT ]; // backup

public:
	ServerNode( uint32_t id ) {
		this->id = id;
		for ( uint32_t i = 0; i < METRIC_COUNT; i++ ) {
			this->_metric[ i ] = this->metric[ i ] = 0; // id < M / 8 ? ( i == 0 ? k : ( i == 1 ? 1 : 0 ) ) : 0;
		}
	}

	void reset() {
		for ( uint32_t i = 0; i < METRIC_COUNT; i++ ) {
			this->metric[ i ] = this->_metric[ i ];
		}
	}

	void increment( int metricType, bool isParity ) {
		switch( metricType ) {
			case METRIC_LOAD:
				this->metric[ metricType ] += isParity ? k : 1;
				break;
			case METRIC_STORAGE_COST:
				this->metric[ metricType ] += 1;
				break;
			default:
				return;
		}
	}

	void increment( bool isParity ) {
		for ( int metricType = 0; metricType < METRIC_COUNT; metricType++ ) {
			if ( metricType == METRIC_NUM_KEYS )
				continue;
			this->increment( metricType, isParity );
		}
	}

	void incrementNumKeys( uint32_t num, bool isParity ) {
		this->metric[ METRIC_NUM_KEYS ] += num; // isParity ? 0 : num;
	}

	uint32_t getMetric( int metricType ) {
		if ( metricType < 0 || metricType > METRIC_COUNT )
			return 0;
		return this->metric[ metricType ];
	}

	void print() {
		printf( "#%u:", this->id );
		for ( uint32_t metricType = 0; metricType < METRIC_COUNT; metricType++ )
			printf( "\t%u", this->metric[ metricType ] );
		printf( "\n" );
	}
};

////////////////////////////////////////////////////////////////////////////////

struct Fairness {
	double f[ FAIRNESS_COUNT ];

	static int compareByMaxMinAvg( const void *p1, const void *p2 ) {
		const struct Fairness *f1 = ( struct Fairness * ) p1;
		const struct Fairness *f2 = ( struct Fairness * ) p2;
		double diff = f1->f[ FAIRNESS_MAX_MIN_AVG ] - f2->f[ FAIRNESS_MAX_MIN_AVG ];
		return ( diff < 0.0 ) ? -1 : ( ( diff > 0.0 ) ? 1 : 0 );
	}

	static int compareByStDev( const void *p1, const void *p2 ) {
		const struct Fairness *f1 = ( struct Fairness * ) p1;
		const struct Fairness *f2 = ( struct Fairness * ) p2;
		double diff = f1->f[ FAIRNESS_STDEV ] - f2->f[ FAIRNESS_STDEV ];
		return ( diff < 0.0 ) ? -1 : ( ( diff > 0.0 ) ? 1 : 0 );
	}

	static int compareByJains( const void *p1, const void *p2 ) {
		const struct Fairness *f1 = ( struct Fairness * ) p1;
		const struct Fairness *f2 = ( struct Fairness * ) p2;
		double diff = f1->f[ FAIRNESS_JAINS ] - f2->f[ FAIRNESS_JAINS ];
		return ( diff < 0.0 ) ? -1 : ( ( diff > 0.0 ) ? 1 : 0 );
	}
};

////////////////////////////////////////////////////////////////////////////////

std::vector<ServerNode *> servers;

////////////////////////////////////////////////////////////////////////////////

double calculateFairness( int metricType, int fairnessType ) {
	uint32_t metric;
	double normalizedMetric, fairness = 0, totalSum = 0;

	for ( uint32_t i = 0; i < M; i++ )
		totalSum += servers[ i ]->getMetric( metricType );

	if ( fairnessType == FAIRNESS_MAX_MIN_AVG ) {
		double max = 0, min = 0;
		// uint32_t originalMax = 0, originalMin = 0;
		for ( uint32_t i = 0; i < M; i++ ) {
			metric = servers[ i ]->getMetric( metricType );
			normalizedMetric = ( double ) metric / totalSum;
			if ( i == 0 ) {
				max = normalizedMetric;
				min = normalizedMetric;
			} else {
				max = normalizedMetric > max ? normalizedMetric : max;
				min = normalizedMetric < min ? normalizedMetric : min;
			}
		}
		fairness = ( max - min ) / ( double ) M;
	} else if ( fairnessType == FAIRNESS_STDEV ) {
		double mean = 0;
		mean = 1.0 / ( double ) M;
		for ( uint32_t i = 0; i < M; i++ ) {
			metric = servers[ i ]->getMetric( metricType );
			normalizedMetric = ( double ) metric / totalSum;
			fairness += ( normalizedMetric - mean ) * ( normalizedMetric - mean );
		}
		fairness = sqrt( fairness / ( double ) M );
	} else if ( fairnessType == FAIRNESS_JAINS ) {
		double sum = 0, sumsq = 0;
		for ( uint32_t i = 0; i < M; i++ ) {
			metric = servers[ i ]->getMetric( metricType );
			normalizedMetric = ( double ) metric / totalSum;
			sum += normalizedMetric;
			sumsq += normalizedMetric * normalizedMetric;
		}
		fairness = ( double )( sum * sum ) / ( M * sumsq );
	}

	return fairness;
}

void generateStripeLists( uint32_t c, struct Fairness *fairnesses, bool useAlgo ) {
	ServerNode *serverNode;
	StripeList<ServerNode> stripeList( n, k, c, servers, useAlgo );

	// Reset metrics
	for ( uint32_t i = 0; i < M; i++ ) {
		servers[ i ]->reset();
	}

	// Calculate metrics (load and storage cost)
	for ( uint32_t listId = 0; listId < c; listId++ ) {
		for ( uint32_t chunkId = 0; chunkId < n; chunkId++ ) {
			serverNode = stripeList.get( listId, chunkId );
			serverNode->increment( chunkId >= k /* isParity */ );
		}
	}

	// Calculate metrics (key range distribution)
	ServerNode **dataServerNodes = new ServerNode*[ k ];
	ServerNode **parityServerNodes = new ServerNode*[ n - k ];
	std::map<unsigned int, uint32_t> ring = stripeList.getRing();
	std::map<unsigned int, uint32_t>::iterator it;
	unsigned int from = 0, to = 0, num, rem;
	for ( it = ring.begin(); it != ring.end(); it++ ) {
		to = it->first;
		num = ( to - from ) / k;
		rem = ( to - from ) % k;
		assert(
			stripeList.getByHash( from, dataServerNodes, parityServerNodes ) == stripeList.getByHash( to, dataServerNodes, parityServerNodes )
		);
		for ( uint32_t i = 0; i < k; i++ )
			dataServerNodes[ i ]->incrementNumKeys( num + ( ( rem <= i ) ? 1 : 0 ), false );
		for ( uint32_t i = 0; i < n - k; i++ )
			parityServerNodes[ i ]->incrementNumKeys( num + ( ( rem <= i ) ? 1 : 0 ), true );
		from = to + 1;
	}
	// Handle wrap-around list
	from = ring.rbegin()->first + 1;
	to = UINT32_MAX;
	num = ( to - from ) / k;
	rem = ( to - from ) % k;
	assert(
		stripeList.getByHash( from, dataServerNodes, parityServerNodes ) == stripeList.getByHash( to, dataServerNodes, parityServerNodes )
	);
	for ( uint32_t i = 0; i < k; i++ ) {
		dataServerNodes[ i ]->incrementNumKeys( num + ( ( rem <= i ) ? 1 : 0 ), false );
		for ( uint32_t j = 0; j < n - k; j++ )
			parityServerNodes[ j ]->incrementNumKeys( num + ( ( rem <= i ) ? 1 : 0 ), true );
	}

	delete[] dataServerNodes;
	delete[] parityServerNodes;

	// Calculate fairness
	for ( int metricType = 0; metricType < METRIC_COUNT; metricType++ ) {
		for ( int fairnessType = 0; fairnessType < FAIRNESS_COUNT; fairnessType++ ) {
			fairnesses[ metricType ].f[ fairnessType ] = calculateFairness( metricType, fairnessType );
		}
	}

	// Print each server node (for debugging only)
	// for ( uint32_t i = 0; i < M; i++ ) {
	// 	servers[ i ]->print();
	// }
	// stripeList.print();
}

////////////////////////////////////////////////////////////////////////////////

void printFairness( FILE *f, struct Fairness *fairnesses, uint32_t c ) {
	fprintf( f, "%u", c );
	for ( int metricType = 0; metricType < METRIC_COUNT; metricType++ ) {
		for ( int fairnessType = 0; fairnessType < FAIRNESS_COUNT; fairnessType++ ) {
			fprintf( f, "\t%10.8lf", fairnesses[ metricType ].f[ fairnessType ] );
		}
	}
	fprintf( f, "\n" );
}

////////////////////////////////////////////////////////////////////////////////

void runExperiments() {
	struct Fairness **fairnesses, *tmp;
	struct Fairness sortedFairness[ 3 ][ METRIC_COUNT ];
	FILE *f[ 4 ];
	int ( *compar[ FAIRNESS_COUNT ] )( const void *, const void * ) = {
		Fairness::compareByMaxMinAvg,
		Fairness::compareByStDev,
		Fairness::compareByJains
	};

	f[ 0 ] = fopen( loadFilename, "w" );
	f[ 1 ] = fopen( rand10Filename, "w" );
	f[ 2 ] = fopen( rand50Filename, "w" );
	f[ 3 ] = fopen( rand90Filename, "w" );

	fairnesses = new struct Fairness*[ iters ];
	for ( uint32_t iter = 0; iter < iters; iter++ )
		fairnesses[ iter ] = new struct Fairness[ METRIC_COUNT ];
	tmp = new struct Fairness[ iters ];

	for ( int i = 0; i < 4; i++ ) {
		if ( ! f[ i ] ) {
			fprintf( stderr, "Cannot create result file. Terminating...\n" );
			for ( int j = 0; j < i; j++ )
				fclose( f[ i ] );
			return;
		}
	}

	// Load-aware stripe list generation
	printf( "Performing load-aware stripe list generation..." );
	fflush( stdout );
	for ( uint32_t c = cFrom; c < cTo; c++ ) {
		generateStripeLists( c, fairnesses[ 0 ], true /* useAlgo */ );
		printFairness( f[ 0 ], fairnesses[ 0 ], c );
	}

	// Random stripe list generatoin
	printf( "Performing random stripe list generation (seed = %u)...\n", cFrom + cTo + iters );
	fflush( stdout );
	for ( uint32_t c = cFrom; c < cTo; c++ ) {
		for ( uint32_t iter = 0; iter < iters; iter++ ) {
			printf( "\r\tc = %u (%6d / %6d)", c, iter + 1, iters );
			generateStripeLists( c, fairnesses[ iter ], false /* useAlgo */ );
		}
		printf( "\n" );

		for ( uint32_t metricType = 0; metricType < METRIC_COUNT; metricType++ ) {
			// Sort by different fairnesses
			for ( uint32_t fairnessType = 0; fairnessType < FAIRNESS_COUNT; fairnessType++ ) {
				for ( uint32_t iter = 0; iter < iters; iter++ )
					tmp[ iter ] = fairnesses[ iter ][ metricType ];
				qsort( tmp, iters, sizeof( struct Fairness ), compar[ fairnessType ] );

				// Take the 10th-percentile, 50th-percentile and 90th-percentile
				sortedFairness[ 0 ][ metricType ].f[ fairnessType ] = tmp[ iters / 10 ].f[ fairnessType ];
				sortedFairness[ 1 ][ metricType ].f[ fairnessType ] = tmp[ iters / 2 ].f[ fairnessType ];
				sortedFairness[ 2 ][ metricType ].f[ fairnessType ] = tmp[ 9 * iters / 10 ].f[ fairnessType ];
			}
		}

		for ( int i = 0; i < 3; i++ )
			printFairness( f[ i + 1 ], sortedFairness[ i ], c );
	}
	printf( "Done.\n" );

	for ( int i = 0; i < 4; i++ )
		fclose( f[ i ] );
}

////////////////////////////////////////////////////////////////////////////////

int main( int argc, char **argv ) {
	assert( sizeof( unsigned int ) == sizeof( uint32_t ) );
	if ( argc <= 10 ) {
		fprintf(
			stderr,
			"Usage: %s [n] [k] [M] [c_from] [c_to] [iters] [laod_f] [rand10_f] [rand50_f] [rand90_f]\n\n"
			"n: Number of chunks\n"
			"k: Number of data chunks\n"
			"M: Number of servers\n"
			"c_from: Number of stripe lists (From)\n"
			"c_to: Number of stripe lists (To)\n"
			"iters: Number of iterations for random stripe list generation\n"
			"load_f: Output file for load-aware stripe list generation algorithm\n"
			"rand10_f: Output file for random stripe list generation (10th percentile)\n"
			"rand50_f: Output file for random stripe list generation (50th percentile)\n"
			"rand90_f: Output file for random stripe list generation (90th percentile)\n",
			argv[ 0 ]
		);
		return 1;
	}
	// Parse arguments
	n = atoi( argv[ 1 ] );
	k = atoi( argv[ 2 ] );
	M = atoi( argv[ 3 ] );
	cFrom = atoi( argv[ 4 ] );
	cTo = atoi( argv[ 5 ] );
	iters = atoi( argv[ 6 ] );
	loadFilename = argv[ 7 ];
	rand10Filename = argv[ 8 ];
	rand50Filename = argv[ 9 ];
	rand90Filename = argv[ 10 ];

	// Select a random seed
	srand( cFrom + cTo + iters );

	// Generate ServerNodes
	for ( uint32_t i = 0; i < M; i++ ) {
		ServerNode *serverNode = new ServerNode( i );
		servers.push_back( serverNode );
	}

	runExperiments();

	return 0;
}
