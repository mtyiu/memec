#ifndef __COMMON_DS_VALUE_HH__
#define __COMMON_DS_VALUE_HH__

#include <cstdlib>
#include <cstring>
#include <stdint.h>

class Value {
public:
	uint32_t size;
	char *data;

	inline void dup( uint32_t size = 0, char *data = 0 ) {
		if ( ! size )
			size = this->size;
		if ( ! data )
			data = this->data;
		this->size = size;
		this->data = ( char * ) malloc( size );
		memcpy( this->data, data, size );
	}

	inline void set( uint32_t size, char *data ) {
		this->size = size;
		this->data = data;
	}

	inline void free() {
		if ( this->data )
			::free( this->data );
		this->data = 0;
	}
};

#endif
