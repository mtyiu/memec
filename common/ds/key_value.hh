#ifndef __COMMON_DS_KEY_VALUE_HH__
#define __COMMON_DS_KEY_VALUE_HH__

#include <stdint.h>
#include <arpa/inet.h>
#include "key.hh"

#define KEY_VALUE_METADATA_SIZE	4
#define SPLIT_OFFSET_SIZE       3

class KeyValue {
public:
	char *data;
	void *ptr; // Extra data to be augmented to the object

	Key key( bool enableSplit = false );

	void dup( char *key, uint8_t keySize, char *value, uint32_t valueSize, uint32_t splitOffset, void *ptr = 0 );
	void _dup( char *key, uint8_t keySize, char *value, uint32_t valueSize, void *ptr = 0 );
	void set( char *data, void *ptr = 0 );
	void clear();
	void free();

	void setSize( uint8_t keySize, uint32_t valueSize );
	static void setSize( char *data, uint8_t keySize, uint32_t valueSize );

	uint32_t getSize( uint8_t *keySizePtr = 0, uint32_t *valueSizePtr = 0 ) const;
	static uint32_t getSize( char *data, uint8_t *keySizePtr = 0, uint32_t *valueSizePtr = 0 );

	char *serialize( char *key, uint8_t keySize, char *value, uint32_t valueSize, uint32_t splitOffset );
	static char *serialize( char *data, char *key, uint8_t keySize, char *value, uint32_t valueSize, uint32_t splitOffset );

	char *_serialize( char *key, uint8_t keySize, char *value, uint32_t valueSize );
	static char *_serialize( char *data, char *key, uint8_t keySize, char *value, uint32_t valueSize );

	char *deserialize( char *&key, uint8_t &keySize, char *&value, uint32_t &valueSize, uint32_t &splitOffset ) const;
	static char *deserialize( char *data, char *&key, uint8_t &keySize, char *&value, uint32_t &valueSize, uint32_t &splitOffset );

	char *_deserialize( char *&key, uint8_t &keySize, char *&value, uint32_t &valueSize ) const;
	static char *_deserialize( char *data, char *&key, uint8_t &keySize, char *&value, uint32_t &valueSize );

	static uint32_t getChunkUpdateOffset( uint32_t chunkOffset, uint8_t keySize, uint32_t valueUpdateOffset );

	void print( FILE *f = stdout );
};

class LargeObjectUtil {
private:
	static uint32_t chunkSize;

public:
	static void init( uint32_t chunkSize );
	static bool isLarge( uint8_t keySize, uint32_t valueSize, uint32_t *numOfSplitPtr = 0, uint32_t *splitSizePtr = 0 );
	static uint32_t getValueOffsetAtSplit( uint8_t keySize, uint32_t valueSize, uint32_t index );
	static uint32_t getSplitIndex( uint8_t keySize, uint32_t valueSize, uint32_t offset, bool &isLarge );
	static void writeSplitOffset( char *buf, uint32_t splitOffset );
	static uint32_t readSplitOffset( char *buf );
};

#endif
