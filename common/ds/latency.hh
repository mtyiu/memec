#ifndef __COMMON_DS_LATENCY_H__
#define __COMMON_DS_LATENCY_H__


#include <stdint.h>
#include <set>

class Latency {
public:
	
	uint32_t sec;
	uint32_t usec;

	Latency();
	Latency( double latency ) {
		this->set( latency );
	}
	~Latency() {} ;

	void set ( double latency );
	void set ( Latency latency );
	void set ( std::set< Latency > latencies );
	double get();
	void aggregate ( Latency *l );
	void aggregate ( Latency l );


	static void set ( Latency& target, double latency ) {
		target.sec = ( uint32_t ) latency;
		target.usec = ( uint32_t ) ( latency - target.sec ) * 1000000;
	}

	bool operator< ( const Latency &l ) const {
		if ( this->sec > l.sec ) {
			return true;
		} else if ( this->sec < l.sec ) {
			return false;
		}
		if ( this->usec < l.usec ) {
			return false;
		}
		return true;
	}
	
};

#endif
