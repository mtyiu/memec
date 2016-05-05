#ifndef __SERVER_HOTNESS_HOTNESS_HH__
#define __SERVER_HOTNESS_HOTNESS_HH__

#include <stdio.h>
#include <vector>
#include "../../common/ds/metadata.hh"

#define DEFAULT_MAX_HOTNESS_ITEMS		( 100 )

class Hotness {
public:

	Hotness() {};
	Hotness( size_t maxItems ) {};
	virtual ~Hotness() {};

	virtual bool insert( Metadata newItem ) = 0;
	virtual void reset() = 0;

	virtual std::vector<Metadata> getItems() = 0;
	virtual std::vector<Metadata> getTopNItems( size_t n ) = 0;

	virtual size_t getAndReset( std::vector<Metadata>& dest, size_t n = 0 ) = 0;

	virtual void print( FILE *output = stdout ) = 0;

protected:
	size_t maxItems;
};

#endif
