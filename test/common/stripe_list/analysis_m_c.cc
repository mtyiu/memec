#include <vector>
#include <cstdio>
#include <cmath>
#include <cassert>
#include "../../../common/stripe_list/stripe_list.hh"

////////////////////////////////////////////////////////////////////////////////

uint32_t n, k, M;
double epsilon;

////////////////////////////////////////////////////////////////////////////////

class ServerNode {
private:
	uint32_t id;
	uint32_t metric;
	uint32_t _metric; // backup

public:
	ServerNode( uint32_t id ) {
		this->id = id;
		this->_metric = this->metric = 0; // id < M / 8 ? ( i == 0 ? k : ( i == 1 ? 1 : 0 ) ) : 0;
	}

	void reset() {
		this->metric = this->_metric;
	}

	void increment( bool isParity ) {
		this->metric += isParity ? k : 1;
	}

	uint32_t getMetric() {
		return this->metric;
	}

	void print() {
		printf( "#%u:\t%u\n", this->id, this->metric );
	}
};

////////////////////////////////////////////////////////////////////////////////

struct Fairness {
	double f;

	static int compareByJains( const void *p1, const void *p2 ) {
		const struct Fairness *f1 = ( struct Fairness * ) p1;
		const struct Fairness *f2 = ( struct Fairness * ) p2;
		double diff = f1->f - f2->f;
		return ( diff < 0.0 ) ? -1 : ( ( diff > 0.0 ) ? 1 : 0 );
	}
};

////////////////////////////////////////////////////////////////////////////////

std::vector<ServerNode *> servers;

////////////////////////////////////////////////////////////////////////////////

double calculateFairness() {
	uint32_t metric;
	double normalizedMetric, fairness = 0, totalSum = 0;

	for ( uint32_t i = 0; i < M; i++ )
		totalSum += servers[ i ]->getMetric();

	double sum = 0, sumsq = 0;
	for ( uint32_t i = 0; i < M; i++ ) {
		metric = servers[ i ]->getMetric();
		normalizedMetric = ( double ) metric / totalSum;
		sum += normalizedMetric;
		sumsq += normalizedMetric * normalizedMetric;
	}
	fairness = ( double )( sum * sum ) / ( M * sumsq );

	return fairness;
}

double generateStripeLists( uint32_t c ) {
	ServerNode *serverNode;
	StripeList<ServerNode> stripeList( n, k, c, servers, true );

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

	// Calculate fairness
	return calculateFairness();
}

////////////////////////////////////////////////////////////////////////////////

void runExperiments() {
	double fairness = 0.0;
	uint32_t c = 0;
	do {
		c++;
		fairness = generateStripeLists( c );
	} while( 1.0 - fairness > epsilon );
	printf( "%u\t%u\n", M, c );
}

////////////////////////////////////////////////////////////////////////////////

int main( int argc, char **argv ) {
	assert( sizeof( unsigned int ) == sizeof( uint32_t ) );
	if ( argc <= 4 ) {
		fprintf(
			stderr,
			"Usage: %s [n] [k] [M] [epsilon]\n\n"
			"n: Number of chunks\n"
			"k: Number of data chunks\n"
			"M: Number of servers\n"
			"epsilon: Terminating condition\n",
			argv[ 0 ]
		);
		return 1;
	}
	// Parse arguments
	n = atoi( argv[ 1 ] );
	k = atoi( argv[ 2 ] );
	M = atoi( argv[ 3 ] );
	sscanf( argv[ 4 ], "%lf", &epsilon );

	// Generate ServerNodes
	for ( uint32_t i = 0; i < M; i++ ) {
		ServerNode *serverNode = new ServerNode( i );
		servers.push_back( serverNode );
	}

	runExperiments();

	return 0;
}
