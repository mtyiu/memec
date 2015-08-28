#ifndef __COMMON_HASH_CONSISTENT_HASH_HH__
#define __COMMON_HASH_CONSISTENT_HASH_HH__

#include <map>
#include <cstring>
#include <cstdlib>
#include "hash_func.hh"

template <typename T> class ConsistentHash {
private:
	std::map<unsigned int, T> ring;
	unsigned int replicas;

public:
	ConsistentHash() {
		this->replicas = 1;
	}

	void setReplicas( unsigned int replicas ) {
		this->replicas = replicas;
	}

	unsigned int getReplicas() {
		return this->replicas;
	}

	void add( T item ) {
		for ( unsigned int i = 0; i < this->replicas; i++ ) {
			unsigned int val = HashFunc::hash(
				( char * ) &item, sizeof( T ),
				( char * ) &i, sizeof( i )
			);
			this->ring[ val ] = item;
		}
	}

	void remove( T item ) {
		for ( unsigned int i = 0; i < this->replicas; i++ ) {
			unsigned int val = HashFunc::hash(
				( char * ) &item, sizeof( T ),
				( char * ) &i, sizeof( i )
			);
			this->ring.erase( val );
		}
	}

	T get( const char *data, size_t n = 0 ) {
		typename std::map<unsigned int, T>::iterator it;
		unsigned int val = HashFunc::hash( data, n ? n : strlen( data ) );
		it = this->ring.lower_bound( val );
		if ( it == this->ring.end() )
			it = this->ring.begin();
		return it->second;
	}

	T get( const char *data1, size_t n1, const char *data2, size_t n2 ) {
		typename std::map<unsigned int, T>::iterator it;
		unsigned int val = HashFunc::hash( data1, n1, data2, n2 );
		it = this->ring.lower_bound( val );
		if ( it == this->ring.end() )
			it = this->ring.begin();
		return it->second;
	}

	void print( FILE *f = stdout ) {
		fprintf( f,
			"Consistent Hash Ring\n"
			"--------------------\n"
		);
		unsigned int i = 0;
		for (
			typename std::map<unsigned int, T>::iterator it = this->ring.begin();
			it != this->ring.end();
			it++
		) {
			fprintf( f,
				"%u. %u |--> %u\n",
				++i, it->first, it->second
			);
		}
	}
};

#endif
