#ifndef __COMMON_DS_KEY_HH__
#define __COMMON_DS_KEY_HH__

#include <unordered_map>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctype.h>
#include <stdint.h>
#include "../hash/hash_func.hh"

#define SPLIT_OFFSET_SIZE       3

class Key {
public:
	uint8_t size;
	char *data;
	bool isLarge;
	void *ptr; // Extra data to be augmented to the object

	inline void dup( uint8_t size = 0, char *data = 0, void *ptr = 0, bool isLarge = false ) {
		if ( ! size )
			size = this->size;
		if ( ! data )
			data = this->data;
		this->size = size;
		this->isLarge = isLarge;

		if ( isLarge )
			size += SPLIT_OFFSET_SIZE;

		this->data = ( char * ) malloc( size );
		memcpy( this->data, data, size );
		this->ptr = ptr;
	}

	inline void set( uint8_t size, char *data, void *ptr = 0, bool isLarge = false ) {
		this->size = size;
		this->data = data;
		this->ptr = ptr;
		this->isLarge = isLarge;
	}

	inline void free() {
		return;
		if ( this->data )
			::free( this->data );
		this->data = 0;
	}

	bool equal( const Key &k ) const {
		return (
			this->size == k.size &&
			this->isLarge == k.isLarge &&
			strncmp( this->data, k.data, this->size + ( this->isLarge ? SPLIT_OFFSET_SIZE : 0 ) ) == 0
		);
	}

	bool operator==( const Key &k ) const {
		return (
			this->size == k.size &&
			this->isLarge == k.isLarge &&
			strncmp( this->data, k.data, this->size + ( this->isLarge ? SPLIT_OFFSET_SIZE : 0 ) ) == 0
		);
	}

	bool operator<( const Key &k ) const {
		int ret;
		if ( this->size < k.size )
			return true;
		if ( this->size > k.size )
			return false;

		if ( ! this->isLarge && k.isLarge )
			return true;
		if ( this->isLarge && ! k.isLarge )
			return false;

		ret = strncmp( this->data, k.data, this->size + ( this->isLarge ? SPLIT_OFFSET_SIZE : 0 ) );
		if ( ret < 0 )
			return true;
		if ( ret > 0 )
			return false;

		return this->ptr < k.ptr;
	}
};

namespace std {
	template<> struct hash<Key> {
		size_t operator()( const Key &key ) const {
			return HashFunc::hash( key.data, key.size + ( key.isLarge ? SPLIT_OFFSET_SIZE : 0 ) );
		}
	};
}

#endif
