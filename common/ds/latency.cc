#include "latency.hh"
#include <cstdio>

#define KILO		( 1000 )
#define MILLION		( ( double ) 1000 * 1000 )
#define GIGA		( ( double ) 1000 * 1000 * 1000 )
#define PRECENTILE	( ( double ) 0.9 )

#define SMOOTHING_FACTOR		( ( double ) 0.2 )

Latency::Latency() {
	this->sec = 0;
	this->nsec = 0;
}

void Latency::set( double sec ) {
	this->sec = ( uint32_t ) sec ;
	this->nsec = ( uint32_t ) ( sec - this->sec ) * GIGA;
}

void Latency::set( double sec, double nsec ) {
	this->sec = ( uint32_t ) sec ;
	this->nsec = ( uint32_t ) ( sec - this->sec ) * GIGA + ( uint32_t ) nsec;
	if ( this->nsec > GIGA ) {
		this->sec += 1;
		this->nsec -= GIGA;
	} 
}

void Latency::set( struct timespec latency ) {
	this->sec = latency.tv_sec;
	this->nsec = latency.tv_nsec;
}

void Latency::set( Latency latency ) {
	this->sec = latency.sec;
	this->nsec = latency.nsec;
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
	return this->sec + this->nsec / GIGA;
}

void Latency::aggregate( Latency *l ) {

	if ( this->sec == 0 && this->nsec == 0 )
		this->set ( *l );

	// EWMA
	this->sec = this->sec * ( 1 - SMOOTHING_FACTOR ) + l->sec * ( SMOOTHING_FACTOR );
	this->nsec = this->nsec * ( 1 - SMOOTHING_FACTOR ) + l->nsec * ( SMOOTHING_FACTOR );
	if ( this->nsec > GIGA ) {
		this->sec += 1;
		this->nsec -= GIGA;
	}
}

void Latency::aggregate( Latency l ) {
	this->aggregate( &l );
}
