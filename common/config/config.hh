#ifndef __COMMON_CONFIG_CONFIG_HH__
#define __COMMON_CONFIG_CONFIG_HH__

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include "../util/debug.hh"
#include "../util/option.hh"

#define STORAGE_PATH_MAX 256

#define CFG_MATCH( s, n ) strcmp( section, s ) == 0 && strcmp( name, n ) == 0

#define CFG_PARSE_ERROR(class_name, ...) \
	do { \
		__ERROR__( class_name, "parse", __VA_ARGS__ ); \
		return false; \
	} while( 0 )

class Config {
protected:
	size_t serializedStringLength;
	char *serializedString;

	Config() {
		this->serializedStringLength = 0;
		this->serializedString = 0;
	}
	~Config() {
		if ( this->serializedString )
			::free( this->serializedString );
	}
	bool parse( const char *path, const char *filename );
	inline bool match( const char *r, const char *s ) {
		return strcmp( r, s ) == 0;
	}

public:
	static int handler( void *data, const char *section, const char *name, const char *value );

	bool override( OptionList &options );
	virtual bool set( const char *section, const char *name, const char *value ) = 0;
	virtual bool validate() = 0;
	const char *serialize( size_t &serializedStringLength );
	bool deserialize( const char *serializedString, size_t serializedStringLength );
	virtual void print( FILE *f ) = 0;
};

#endif
