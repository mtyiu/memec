#ifndef __SERVER_HOTNESS_HOTNESS_HH__
#define __SERVER_HOTNESS_HOTNESS_HH__

#include <stdio.h>
#include <vector>

#define DEFAULT_MAX_HOTNESS_ITEMS		( 100 )

template<class T> class Hotness {
public:

	Hotness() {};
	Hotness( size_t maxItems ) {};
	virtual ~Hotness() {};

	virtual bool insert( T newItem ) = 0;
	virtual void reset() = 0;

	virtual std::vector<T> getItems() = 0;
	virtual std::vector<T> getTopNItems( size_t n ) = 0;

	virtual size_t getAndReset( std::vector<T>& dest, size_t n = 0 ) = 0;

	virtual void print( FILE *output = stdout ) = 0;

protected:
	size_t maxItems;
};

#endif
