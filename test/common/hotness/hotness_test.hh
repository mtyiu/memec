#ifndef __TEST_COMMON_HOTNESS_HOTNESS_TEST_HH__
#define __TEST_COMMON_HOTNESS_HOTNESS_TEST_HH__

#include <vector>
#include <stdio.h>
#include "../../../common/ds/metadata.hh"
#include "../../../common/hotness/hotness.hh"

class HotnessDataStructTest {
public:
	HotnessDataStructTest( Hotness *hotness ) {
		this->hotness = hotness;
	}

	void print() {
		hotness->print();
	}

	void insertItems( size_t count ) {
		Metadata metadata;
		for ( size_t i = 0; i < count; i++ ) {
			metadata.set( i, i*2, i );
			hotness->insert( metadata );
		}
	}

	void insertRepeatedItems( size_t count ) {
		Metadata metadata;
		metadata.set( 101, 101, 1 );
		for ( size_t i = 0; i < count; i++ ) {
			hotness->insert( metadata );
		}
	}

	void overflowList() {
		insertItems( DEFAULT_MAX_HOTNESS_ITEMS * 3 );
	}

	void resetList() {
		hotness->reset();
	}

	void run() {
		print();

		insertItems( DEFAULT_MAX_HOTNESS_ITEMS / 2 );
		print();

		insertRepeatedItems( DEFAULT_MAX_HOTNESS_ITEMS / 4 );
		print();

		overflowList();
		print();

		resetList();
		print();
	}

private: 
	Hotness *hotness;
};

#endif
