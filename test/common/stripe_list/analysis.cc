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
char *loadFilename, *rand25Filename, *rand50Filename, *rand75Filename;

////////////////////////////////////////////////////////////////////////////////

class ServerNode {
private:
	uint32_t id;
	uint32_t metric[ METRIC_COUNT ];

public:
	ServerNode( uint32_t id ) {
		this->id = id;
		this->reset();
	}

	void reset() {
		for ( uint32_t i = 0; i < METRIC_COUNT; i++ )
			this->metric[ i ] = 0;
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
		this->metric[ METRIC_NUM_KEYS ] += num;
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
		return f1->f[ FAIRNESS_MAX_MIN_AVG ] > f2->f[ FAIRNESS_MAX_MIN_AVG ];
	}

	static int compareByStDev( const void *p1, const void *p2 ) {
		const struct Fairness *f1 = ( struct Fairness * ) p1;
		const struct Fairness *f2 = ( struct Fairness * ) p2;
		return f1->f[ FAIRNESS_STDEV ] < f2->f[ FAIRNESS_STDEV ];
	}

	static int compareByJains( const void *p1, const void *p2 ) {
		const struct Fairness *f1 = ( struct Fairness * ) p1;
		const struct Fairness *f2 = ( struct Fairness * ) p2;
		return f1->f[ FAIRNESS_JAINS ] < f2->f[ FAIRNESS_JAINS ];
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

	f[ 0 ] = fopen( loadFilename, "w" );
	f[ 1 ] = fopen( rand25Filename, "w" );
	f[ 2 ] = fopen( rand50Filename, "w" );
	f[ 3 ] = fopen( rand75Filename, "w" );

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
	for ( uint32_t c = cFrom; c < cTo; c++ ) {
		generateStripeLists( c, fairnesses[ 0 ], true /* useAlgo */ );
		printFairness( f[ 0 ], fairnesses[ 0 ], c );
	}

	// Random stripe list generatoin
	srand( cFrom + cTo + iters ); // Select a random seed
	for ( uint32_t c = cFrom; c < cTo; c++ ) {
		for ( uint32_t iter = 0; iter < iters; iter++ ) {
			generateStripeLists( c, fairnesses[ iter ], false /* useAlgo */ );
			fairnesses[ iter ][ 0 ].f[ 0 ] = iter;
			fairnesses[ iter ][ 0 ].f[ 1 ] = 10 + iter;
			fairnesses[ iter ][ 0 ].f[ 2 ] = 20 + iter;
		}

		for ( uint32_t metricType = 0; metricType < METRIC_COUNT; metricType++ ) {
			printf( "Metric Type: %u\n", metricType );
			// Before sorting
			for ( uint32_t fairnessType = 0; fairnessType < FAIRNESS_COUNT; fairnessType++ ) {
				for ( uint32_t iter = 0; iter < iters; iter++ ) {
					printf( "%lf ", fairnesses[ iter ][ metricType ].f[ fairnessType ] );
				}
				printf( "\n" );
			}

			// Sort by different fairnesses
			printf( "\nSort by max-min average:\n" );
			for ( uint32_t iter = 0; iter < iters; iter++ ) {
				printf( "%p\n", fairnesses[ iter ][ metricType ].f );
				printf( "(%p)\n", tmp[ iter ].f );
				tmp[ iter ] = fairnesses[ iter ][ metricType ];
			}
			for ( uint32_t iter = 0; iter < iters; iter++ )
				printf( "%lf ", tmp[ iter ].f[ FAIRNESS_MAX_MIN_AVG ] );
			printf( "\n\n" );

			////////////////////////////////////////////////////////////////////

			printf( "Sort by standard deviation:\n" );
			for ( uint32_t iter = 0; iter < iters; iter++ ) {
				printf( "%p\n", fairnesses[ iter ][ metricType ].f );
				printf( "(%p)\n", tmp[ iter ].f );
				tmp[ iter ] = fairnesses[ iter ][ metricType ];
			}
			qsort( fairnesses, iters, sizeof( struct Fairness ), Fairness::compareByStDev );
			for ( uint32_t iter = 0; iter < iters; iter++ )
				printf( "%lf ", tmp[ iter ].f[ FAIRNESS_STDEV ] );
			printf( "\n\n" );
			
			////////////////////////////////////////////////////////////////////


			printf( "Sort by Jain's fairness:\n" );
			for ( uint32_t iter = 0; iter < iters; iter++ ) {
				printf( "%p\n", fairnesses[ iter ][ metricType ].f );
				printf( "(%p)\n", tmp[ iter ].f );
				tmp[ iter ] = fairnesses[ iter ][ metricType ];
			}
			qsort( fairnesses, iters, sizeof( struct Fairness ), Fairness::compareByJains );
			for ( uint32_t iter = 0; iter < iters; iter++ )
				printf( "%lf ", tmp[ iter ].f[ FAIRNESS_JAINS ] );
			printf( "\n\n" );
		}

		printf( "\n" );

		// for ( int i = 0; i < 3; i++ )
		// 	printFairness( f[ i + 1 ], sortedFairness[ i ], c );
	}

	for ( int i = 0; i < 4; i++ )
		fclose( f[ i ] );
}

////////////////////////////////////////////////////////////////////////////////

int main( int argc, char **argv ) {
	assert( sizeof( unsigned int ) == sizeof( uint32_t ) );
	if ( argc <= 10 ) {
		fprintf(
			stderr,
			"Usage: %s [n] [k] [M] [c_from] [c_to] [iters] [laod_f] [rand25_f] [rand50_f] [rand75_f]\n\n"
			"n: Number of chunks\n"
			"k: Number of data chunks\n"
			"M: Number of servers\n"
			"c_from: Number of stripe lists (From)\n"
			"c_to: Number of stripe lists (To)\n"
			"iters: Number of iterations for random stripe list generation\n"
			"load_f: Output file for load-aware stripe list generation algorithm\n"
			"rand25_f: Output file for random stripe list generation (25th percentile)\n"
			"rand50_f: Output file for random stripe list generation (50th percentile)\n"
			"rand75_f: Output file for random stripe list generation (75th percentile)\n",
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
	rand25Filename = argv[ 8 ];
	rand50Filename = argv[ 9 ];
	rand75Filename = argv[ 10 ];

	// Generate ServerNodes
	for ( uint32_t i = 0; i < M; i++ ) {
		ServerNode *serverNode = new ServerNode( i );
		servers.push_back( serverNode );
	}

	runExperiments();

	return 0;
}
