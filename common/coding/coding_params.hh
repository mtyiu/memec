#ifndef __CODING_PARAMS_HH__
#define __CODING_PARAMS_HH__

#include <stdint.h>
#include "coding_scheme.hh"

class CodingParams {
private:
	CodingScheme scheme;
	uint32_t params[ 3 ];

	inline void set( int index, uint32_t p ) {
		this->params[ index ] = p;
	}

	inline uint32_t get( int index ) {
		return this->params[ index ];
	}

public:
	CodingParams() {
		this->scheme = CS_UNDEFINED;
	}

	inline void setScheme( CodingScheme scheme ) {
		this->scheme = scheme;
	}

	inline void setN( uint32_t n ) {
		switch( this->scheme ) {
			case CS_RAID0:
			case CS_RAID1:
			case CS_RAID5:
			case CS_EMBR:
			case CS_RDP:
			case CS_EVENODD:
				return this->set( 0, n );
			default:
				return;
		}
	}

	inline void setK( uint32_t k ) {
		switch( this->scheme ) {
			case CS_RS:
			case CS_CAUCHY:
				return this->set( 0, k );
			case CS_EMBR:
				return this->set( 1, k );
			default:
				return;
		}
	}

	inline void setM( uint32_t m ) {
		switch( this->scheme ) {
			case CS_RS:
			case CS_CAUCHY:
				return this->set( 1, m );
			default:
				return;
		}
	}

	inline void setW( uint32_t w ) {
		switch( this->scheme ) {
			case CS_RS:
			case CS_EMBR:
			case CS_CAUCHY:
				return this->set( 2, w );
			default:
				return;
		}
	}

	inline uint32_t getN() {
		switch( this->scheme ) {
			case CS_RAID0:
			case CS_RAID1:
			case CS_RAID5:
			case CS_EMBR:
			case CS_RDP:
			case CS_EVENODD:
				return this->get( 0 );
			default:
				return 0;
		}
	}

	inline uint32_t getK() {
		switch( this->scheme ) {
			case CS_RS:
			case CS_CAUCHY:
				return this->get( 0 );
			case CS_EMBR:
				return this->get( 1 );
			default:
				return 0;
		}
	}

	inline uint32_t getM() {
		switch( this->scheme ) {
			case CS_RS:
			case CS_CAUCHY:
				return this->get( 1 );
			default:
				return 0;
		}
	}

	inline uint32_t getW() {
		switch( this->scheme ) {
			case CS_RS:
			case CS_EMBR:
			case CS_CAUCHY:
				return this->get( 2 );
			default:
				return 0;
		}
	}
};

#endif
