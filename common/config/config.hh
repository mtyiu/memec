#ifndef __CONFIG_HH__
#define __CONFIG_HH__

#include <cstdio>
#include <cstring>

#define CFG_MATCH( s, n ) strcmp( section, s ) == 0 && strcmp( name, n ) == 0

class Config {
protected:
	inline bool match( const char *r, const char *s ) {
		return strcmp( r, s ) == 0;
	}

public:
	static int handler( void *, const char *, const char *, const char * );
	bool parse( char * );

	virtual bool set( const char *, const char *, const char * ) = 0;
	virtual bool validate() = 0;
	virtual void print( FILE * ) = 0;
};

#endif
