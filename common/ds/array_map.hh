#ifndef __COMMON_DS_ARRAY_MAP_HH__
#define __COMMON_DS_ARRAY_MAP_HH__

#include <vector>
#include <cstdio>
#include <pthread.h>

template<typename KeyType, typename ValueType> class ArrayMap {
private:
	int indexOf( KeyType &key ) {
		int ret = -1;
		for ( int i = 0, len = this->keys.size(); i < len; i++ ) {
			if ( this->keys[ i ] == key ) {
				if ( ret != -1 ) {
					printf( "ArrayMap::indexOf(): Duplicated entry!\n" );
				}
				ret = i;
			}
		}
		return ret;
	}

public:
	std::vector<KeyType> keys;
	std::vector<ValueType *> values;
	pthread_mutex_t lock;

	ArrayMap() {
		pthread_mutex_init( &this->lock, 0 );
	}

	void reserve( int n ) {
		this->keys.reserve( n );
		this->values.reserve( n );
	}

	ValueType *get( KeyType &key, int *indexPtr = 0 ) {
		ValueType *ret;
		pthread_mutex_lock( &this->lock );
		int index = this->indexOf( key );
		if ( indexPtr )
			*indexPtr = index;
		ret = index == -1 ? 0 : this->values[ index ];
		pthread_mutex_unlock( &this->lock );
		return ret;
	}

	ValueType *operator[]( const int index ) {
		return this->values[ index ];
	}

	bool replaceKey( KeyType &oldKey, KeyType &newKey ) {
		pthread_mutex_lock( &this->lock );
		int index = this->indexOf( oldKey );
		if ( index != -1 )
			this->keys[ index ] = newKey;
		pthread_mutex_unlock( &this->lock );
		return index != -1;
	}

	size_t size() {
		return this->values.size();
	}

	bool set( KeyType &key, ValueType *value, bool check = false ) {
		pthread_mutex_lock( &this->lock );
		if ( check && this->indexOf( key ) != -1 ) {
			pthread_mutex_unlock( &this->lock );
			return false;
		}
		this->keys.push_back( key );
		this->values.push_back( value );
		pthread_mutex_unlock( &this->lock );
		return true;
	}

	bool remove( KeyType &key ) {
		ValueType *val;
		pthread_mutex_lock( &this->lock );
		int index = this->indexOf( key );
		if ( index == -1 ) {
			pthread_mutex_unlock( &this->lock );
			return false;
		}
		val = this->values[ index ];
		this->keys.erase( this->keys.begin() + index );
		this->values.erase( this->values.begin() + index );
		pthread_mutex_unlock( &this->lock );
		delete val;
		return true;
	}

	bool removeAt( int index ) {
		ValueType *val;
		pthread_mutex_lock( &this->lock );
		val = this->values[ index ];
		this->keys.erase( this->keys.begin() + index );
		this->values.erase( this->values.begin() + index );
		pthread_mutex_unlock( &this->lock );;
		delete val;
		return true;
	}

	void clear() {
		for ( int i = 0, len = this->keys.size(); i < len; i++ )
			delete this->values[ i ];
		this->keys.clear();
		this->values.clear();
	}
};

#endif
