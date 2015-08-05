#ifndef __COMMON_DS_KEY_VALUE_HH__
#define __COMMON_DS_KEY_VALUE_HH__

#include <stdint.h>
#include <arpa/inet.h>
#include "key.hh"

#define KEY_VALUE_METADATA_SIZE	4

class KeyValue {
public:
	char *data;
	void *ptr; // Extra data to be augmented to the object

	Key key();
	
	void dup( char *key, uint8_t keySize, char *value, uint32_t valueSize, void *ptr = 0 );
	void free();

	void setSize( uint8_t keySize, uint32_t valueSize );

	char *serialize( char *key, uint8_t keySize, char *value, uint32_t valueSize );
	static char *serialize( char *data, char *key, uint8_t keySize, char *value, uint32_t valueSize );
	
	char *deserialize( char *&key, uint8_t &keySize, char *&value, uint32_t &valueSize ) const;
	static char *deserialize( char *data, char *&key, uint8_t &keySize, char *&value, uint32_t &valueSize );

	bool operator<( const KeyValue &kv ) const;
	static bool compare( const KeyValue *v1, const KeyValue *v2 );
};

#endif
