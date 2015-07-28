#ifndef __COMMON_DS_KEY_HH__
#define __COMMON_DS_KEY_HH__

#include <cstring>

class Key {
public:
	uint8_t size;
	char *data;

	bool operator<( const Key &k ) const {
		return (
			this->size < k.size ||
			( this->size == k.size && strncmp( this->data, k.data, this->size ) )
		);
	}
};

#endif
