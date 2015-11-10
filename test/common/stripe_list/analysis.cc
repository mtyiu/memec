#include <vector>
#include <cstdio>
#include <cmath>
#include <cassert>
#include "../../../common/stripe_list/stripe_list.hh"

#define USE_LOAD_AS_METRIC
// #define USE_STORAGE_COST_AS_METRIC

////////////////////////////////////////////////////////////////////////////////

uint32_t n, k, M;

////////////////////////////////////////////////////////////////////////////////

class ServerNode {
private:
	uint32_t id;
	uint32_t load, initLoad;
	uint32_t numKeys;

public:
	ServerNode( uint32_t id, uint32_t load = 0 ) {
		this->id = id;
		this->load = load;
		this->initLoad = load;
		this->numKeys = 0;
	}

	uint32_t incrementLoad( bool isParity ) {
#ifdef USE_LOAD_AS_METRIC
		this->load += isParity ? k : 1;
#endif
#ifdef USE_STORAGE_COST_AS_METRIC
		this->load += 1;
#endif
		return this->load;
	}

	uint32_t incrementNumKeys( bool isParity = false ) {
		if ( ! isParity )
			this->numKeys++;
		return this->numKeys;
	}

	uint32_t getLoad() {
		return this->load;
	}

	uint32_t getNumKeys() {
		return this->numKeys;
	}

	void reset() {
		this->load = this->initLoad;
		this->numKeys = 0;
	}

	void print() {
		printf( "#%u: %u; %u\n", this->id, this->load, this->numKeys );
	}
};

struct Fairness {
	struct {
		double maxMinAverage;
		double stDev;
		double jains;
	} load;
	struct {
		double maxMinAverage;
		double stDev;
		double jains;
	} keyRange;
};

////////////////////////////////////////////////////////////////////////////////

std::vector<ServerNode *> servers;

////////////////////////////////////////////////////////////////////////////////

struct Fairness generateStripeLists( uint32_t c ) {
	ServerNode *serverNode;
	StripeList<ServerNode> stripeList( n, k, c, servers );

	// Reset metrics
	for ( uint32_t i = 0; i < M; i++ ) {
		servers[ i ]->reset();
	}

	// Calculate metrics
	for ( uint32_t listId = 0; listId < c; listId++ ) {
		for ( uint32_t chunkId = 0; chunkId < n; chunkId++ ) {
			serverNode = stripeList.get( listId, chunkId );
			serverNode->incrementLoad(
				chunkId >= k // isParity
			);
		}
	}

	// Calculate laod fairness
	struct Fairness fairness = { { 0, 0, 0 }, { 0, 0, 0 } };
	uint32_t metric;

	////////// Load //////////

	/* Max-Min Average */
	uint32_t max = 0, min = 0;
	for ( uint32_t i = 0; i < M; i++ ) {
		metric = servers[ i ]->getLoad();
		if ( i == 0 ) {
			max = metric;
			min = metric;
		} else {
			max = metric > max ? metric : max;
			min = metric < min ? metric : min;
		}
	}
	fairness.load.maxMinAverage = ( ( double )( max - min ) ) / ( double ) M;

	/* Standard Deviation */
	double mean = 0;
	for ( uint32_t i = 0; i < M; i++ )
		mean += servers[ i ]->getLoad();
	mean = mean / ( double ) M;
	for ( uint32_t i = 0; i < M; i++ ) {
		metric = servers[ i ]->getLoad();
		fairness.load.stDev += ( metric - mean ) * ( metric - mean );
	}
	fairness.load.stDev = sqrt( fairness.load.stDev / ( double ) M );

	/* Jain's Fairness */
	uint32_t sum = 0, sumsq = 0;
	for ( uint32_t i = 0; i < M; i++ ) {
		metric = servers[ i ]->getLoad();
		sum += metric;
		sumsq += metric * metric;
	}
	fairness.load.jains = ( double )( sum * sum ) / ( M * sumsq );

	////////// Key Range Distribution //////////

	// Calculate key range distribution
	ServerNode **dataServerNodes = new ServerNode*[ k ];
	ServerNode **parityServerNodes = new ServerNode*[ n - k ];
	for ( unsigned int val = 0; val < UINT32_MAX; val++ ) {
		stripeList.getByHash( val, dataServerNodes, parityServerNodes, 0, true );
		for ( uint32_t i = 0; i < k; i++ ) {
			dataServerNodes[ i ]->incrementNumKeys( false );
		}
		/*
		for ( uint32_t i = 0; i < n - k; i++ ) {
			parityServerNodes[ i ]->incrementNumKeys( true );
		}
		*/
	}

	/* Max-Min Average */
	max = 0, min = 0;
	for ( uint32_t i = 0; i < M; i++ ) {
		metric = servers[ i ]->getNumKeys();
		if ( i == 0 ) {
			max = metric;
			min = metric;
		} else {
			max = metric > max ? metric : max;
			min = metric < min ? metric : min;
		}
	}
	fairness.keyRange.maxMinAverage = ( ( double )( max - min ) ) / ( double ) M;

	/* Standard Deviation */
	mean = 0;
	for ( uint32_t i = 0; i < M; i++ )
		mean += servers[ i ]->getNumKeys();
	mean = mean / ( double ) M;
	for ( uint32_t i = 0; i < M; i++ ) {
		metric = servers[ i ]->getNumKeys();
		fairness.keyRange.stDev += ( metric - mean ) * ( metric - mean );
	}
	fairness.keyRange.stDev = sqrt( fairness.keyRange.stDev / ( double ) M );

	/* Jain's Fairness */
	sum = 0, sumsq = 0;
	for ( uint32_t i = 0; i < M; i++ ) {
		metric = servers[ i ]->getNumKeys();
		sum += metric;
		sumsq += metric * metric;
	}
	fairness.keyRange.jains = ( double )( sum * sum ) / ( M * sumsq );

	// Print each server node (for debugging only)
	// for ( uint32_t i = 0; i < M; i++ ) {
	// 	servers[ i ]->print();
	// }
	// stripeList.print();

	delete[] dataServerNodes;
	delete[] parityServerNodes;

	return fairness;
}

////////////////////////////////////////////////////////////////////////////////

void runExperiments( uint32_t from, uint32_t to ) {
	struct Fairness fairness;
	for ( uint32_t c = from; c < to; c++ ) {
		fairness = generateStripeLists( c );
		printf(
			"%u\t%6.4lf\t%6.4lf\t%6.4lf\t%6.4lf\t%6.4lf\t%6.4lf\n",
			c,
			fairness.load.maxMinAverage,
			fairness.load.stDev,
			fairness.load.jains,
			fairness.keyRange.maxMinAverage,
			fairness.keyRange.stDev,
			fairness.keyRange.jains
		);
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
