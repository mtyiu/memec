#ifndef __CONFIG_HH__
#define __CONFIG_HH__

#include <cstdio>
#include <cstring>
#include "../util/debug.hh"

#define CFG_MATCH( s, n ) strcmp( section, s ) == 0 && strcmp( name, n ) == 0

#define CFG_PARSE_ERROR(class_name, ...) \
	do { \
		__ERROR__( class_name, "parse", __VA_ARGS__ ); \
		return false; \
	} while( 0 )

class Config {
protected:
	inline bool match( const char *r, const char *s ) {
		return strcmp( r, s ) == 0;
	}

public:
	static int handler( void *data, const char *section, const char *name, const char *value );
	bool parse( const char *path, const char *filename );

	virtual bool set( const char *section, const char *name, const char *value ) = 0;
	virtual bool validate() = 0;
	virtual void print( FILE *f ) = 0;
};

#endif
