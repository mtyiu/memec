// //////////////////////////////////////////////////////////
// md5.h
// Copyright (c) 2014 Stephan Brumme. All rights reserved.
// see http://create.stephan-brumme.com/disclaimer.html
//

#pragma once

//#include "hash.h"
#define HASH_BUFFER_SIZE 4*8+1

// define fixed size integer types
#ifdef _MSC_VER
// Windows
typedef unsigned __int8	uint8_t;
typedef unsigned __int32 uint32_t;
typedef unsigned __int64 uint64_t;
#else
// GCC
#include <stdint.h>
#include <stddef.h>
#endif


/// compute MD5 hash
/** Usage:
		MD5 md5;
		std::string myHash	= md5("Hello World");		 // std::string
		std::string myHash2 = md5("How are you", 11); // arbitrary data, 11 bytes

		// or in a streaming fashion:

		MD5 md5;
		while (more data available)
			md5.add(pointer to fresh data, number of new bytes);
		std::string myHash3 = md5.getHash();
	*/
class MD5 //: public Hash
{
public:
	static char *toString( const uint32_t *, char * );
	static uint32_t *toArray( const char *, uint32_t * );

	/// same as reset()
	MD5();

	/// compute MD5 of a memory block
	char *operator()(const void *, size_t, char *);
	/// compute MD5 of a string, excluding final zero
	// char *operator()(const std::string& text);

	/// add arbitrary number of bytes
	void add(const void *, size_t);

	/// return latest hash as 16 hex characters
	char *getHash( char * );

	/// return latest hash as 4 uint32_t integers
	uint32_t *getHash( uint32_t * );

	/// restart
	void reset();

private:
	/// process 64 bytes
	void processBlock(const void* data);
	/// process everything left in the internal buffer
	void processBuffer();

	/// split into 64 byte blocks (=> 512 bits)
	enum { BlockSize = 512 / 8 };

	/// size of processed data in bytes
	uint64_t m_numBytes;
	/// valid bytes in m_buffer
	size_t	 m_bufferSize;
	/// bytes not processed yet
	uint8_t	m_buffer[BlockSize];
	/// hash, stored as integers
	uint32_t m_hash[4];
};
