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

uint32_t n, k, M;

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

				// originalMax = metric;
				// originalMin = metric;
			} else {
				max = normalizedMetric > max ? normalizedMetric : max;
				min = normalizedMetric < min ? normalizedMetric : min;

				// originalMax = metric > originalMax ? metric : originalMax;
				// originalMin = metric < originalMin ? metric : originalMin;
			}
		}
		// if ( metricType == METRIC_LOAD ) {
		// 	fprintf( stderr, "%lf %lf (%lf)\t%u %u (%u)\n", max, min, max - min, originalMax, originalMin, originalMax - originalMin );
		// }
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

void generateStripeLists( uint32_t c, struct Fairness *fairnesses ) {
	ServerNode *serverNode;
	StripeList<ServerNode> stripeList( n, k, c, servers );

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

void runExperiments( uint32_t from, uint32_t to ) {
	struct Fairness fairnesses[ METRIC_COUNT ];
	for ( uint32_t c = from; c < to; c++ ) {
		generateStripeLists( c, fairnesses );
		printf( "%u", c );
		for ( int metricType = 0; metricType < METRIC_COUNT; metricType++ ) {
			for ( int fairnessType = 0; fairnessType < FAIRNESS_COUNT; fairnessType++ ) {
				printf( "\t%10.8lf", fairnesses[ metricType ].f[ fairnessType ] );
			}
		}
		printf( "\n" );
	}
}

////////////////////////////////////////////////////////////////////////////////

int main( int argc, char **argv ) {
	assert( sizeof( unsigned int ) == sizeof( uint32_t ) );
	if ( argc < 4 ) {
		fprintf( stderr, "Usage: %s [n: number of chunks] [k: number of data chunks] [M: number of servers]\n", argv[ 0 ] );
		return 1;
	}
	// Parse arguments
	n = atoi( argv[ 1 ] );
	k = atoi( argv[ 2 ] );
	M = atoi( argv[ 3 ] );

	// Generate ServerNodes
	for ( uint32_t i = 0; i < M; i++ ) {
		ServerNode *serverNode = new ServerNode( i );
		servers.push_back( serverNode );
	}

	runExperiments( 1, 100 );

	return 0;
}
