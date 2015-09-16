#include "latency.hh"
#include <cstdio>

#define MILLION		( ( double ) 1000000 )
#define PRECENTILE	( ( double ) 0.9 )

#define SMOOTHING_FACTOR		( ( double ) 0.2 )

Latency::Latency() {
	this->sec = 0;
	this->usec = 0;
}

void Latency::set( double latency ) {
	this->sec = ( uint32_t ) latency;
	this->usec = ( uint32_t ) ( latency - this->sec ) * MILLION;
}

void Latency::set( Latency latency ) {
	this->sec = latency.sec;
	this->usec = latency.usec;
}

void Latency::set( std::set< Latency > latencies ) {
	// find and set the 90 percentile of latency
	uint32_t setSize = latencies.size();
	uint32_t precentileIndex = ( uint32_t ) setSize * PRECENTILE;
	std::set< Latency >::iterator it = latencies.begin();
	for ( uint32_t i = 0; i < precentileIndex; i++ ) 
		it++;
	this->set( *it );
}

double Latency::get() {
	return this->sec + this->usec / MILLION;
}

void Latency::aggregate( Latency *l ) {

	if ( this->sec == 0 && this->usec == 0 )
		this->set ( *l );

	double origin = this->get();
	double update = l->get();

	// EWMA
	this->set( origin * ( 1 - SMOOTHING_FACTOR ) + update * ( SMOOTHING_FACTOR ) );
}

void Latency::aggregate( Latency l ) {
	this->aggregate( &l );
}
