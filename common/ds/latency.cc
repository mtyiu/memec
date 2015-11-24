#include "latency.hh"
#include <cstdio>

#define KILO		( 1000 )
#define MILLION		( ( double ) 1000 * 1000 )
#define GIGA		( ( double ) 1000 * 1000 * 1000 )
#define PRECENTILE	( ( double ) 0.9 )

double Latency::smoothingFactor = 0.2;

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
	printf( "smoothingFactor = %f\n", Latency::smoothingFactor );
	this->sec = this->sec * ( 1 - Latency::smoothingFactor ) + l->sec * ( Latency::smoothingFactor );
	this->nsec = this->nsec * ( 1 - Latency::smoothingFactor ) + l->nsec * ( Latency::smoothingFactor );
	if ( this->nsec > GIGA ) {
		this->sec += 1;
		this->nsec -= GIGA;
	}
}

void Latency::aggregate( Latency l ) {
	this->aggregate( &l );
}

bool Latency::operator< ( const Latency &l ) const {
	if ( this->sec < l.sec ) {
		return true;
	} else if ( this->sec > l.sec ) {
		return false;
	}
	if ( this->nsec > l.nsec ) {
		return false;
	}
	return true;
}

Latency Latency::operator+ ( const Latency &l ) const {
	Latency ret;
	ret.sec = this->sec + l.sec;
	ret.nsec = this->nsec + l.nsec;
	if ( ret.nsec >= GIGA ) {
		ret.sec += 1;
		ret.nsec -= GIGA;
	}
	return ret;
}
