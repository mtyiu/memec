#ifndef __COMMON_HASH_CONSISTENT_HASH_HH__
#define __COMMON_HASH_CONSISTENT_HASH_HH__

#include <map>
#include <cstring>
#include <cstdlib>
#include "hash_func.hh"

template <class T> class ConsistentHash {
private:
	std::map<unsigned int, T *> ring;
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

	void add( T *item ) {
		for ( unsigned int i = 0; i < this->replicas; i++ ) {
			unsigned int val = HashFunc::hash(
				( char * ) item, sizeof( T ),
				( char * ) &i, sizeof( i )
			);
			this->ring[ val ] = item;
		}
	}

	void remove( T *item ) {
		for ( unsigned int i = 0; i < this->replicas; i++ ) {
			unsigned int val = HashFunc::hash(
				( char * ) item, sizeof( T ),
				( char * ) &i, sizeof( i )
			);
			this->ring.erase( val );
		}
	}

	T *get( const char *data, size_t n = 0 ) {
		typename std::map<unsigned int, T *>::iterator it;
		unsigned int val = HashFunc::hash( data, n ? n : strlen( data ) );
		it = this->ring.lower_bound( val );
		if ( it == this->ring.end() )
			it = this->ring.begin();
		return it->second;
	}

	T *get( const char *data1, size_t n1, const char *data2, size_t n2 ) {
		typename std::map<unsigned int, T *>::iterator it;
		unsigned int val = HashFunc::hash( data1, n1, data2, n2 );
		it = this->ring.lower_bound( val );
		if ( it == this->ring.end() )
			it = this->ring.begin();
		return it->second;
	}
};

#endif
