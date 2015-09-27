#ifndef __COMMON_DS_LATENCY_H__
#define __COMMON_DS_LATENCY_H__


#include <stdint.h>
#include <set>
#include <ctime>

class Latency {
public:
	
	uint32_t sec;
	uint32_t nsec;

	Latency();
	Latency( double sec ) {
		this->set( sec );
	}
	Latency( double sec, double nsec ) {
		this->set( sec, nsec );
	}
	Latency( struct timespec latency ) {
		this->set( latency );
	}
	Latency( const Latency *latency ) {
		this->set( *latency );
	}
	~Latency() {} ;

	void set ( double sec );
	void set ( double sec, double nsec );
	void set ( struct timespec latency );
	void set ( Latency latency );
	void set ( std::set< Latency > latencies );
	double get();
	void aggregate ( Latency *l );
	void aggregate ( Latency l );

	bool operator< ( const Latency &l ) const;
	Latency operator+ ( const Latency &l ) const;
};

#endif
