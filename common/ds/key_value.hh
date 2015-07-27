#ifndef __COMMON_DS_KEY_VALUE_HH__
#define __COMMON_DS_KEY_VALUE_HH__

#include <stdint.h>
#include <arpa/inet.h>

class KeyValue {
public:
	char *data;

	char *serialize( char *key, uint8_t keySize, char *value, uint32_t valueSize );
	static char *serialize( char *data, char *key, uint8_t keySize, char *value, uint32_t valueSize );
	
	char *deserialize( char *&key, uint8_t &keySize, char *&value, uint32_t &valueSize );
	static char *deserialize( char *data, char *&key, uint8_t &keySize, char *&value, uint32_t &valueSize );

	static bool compare( const KeyValue *v1, const KeyValue *v2 );
};

#endif
