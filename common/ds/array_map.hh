#ifndef __COMMON_DS_ARRAY_MAP_HH__
#define __COMMON_DS_ARRAY_MAP_HH__

#include <vector>
#include <pthread.h>

template<typename KeyType, typename ValueType> class ArrayMap {
private:
	int indexOf( KeyType &key ) {
		for ( int i = 0, len = this->keys.size(); i < len; i++ ) {
			if ( this->keys[ i ] == key )
				return i;
		}
		return -1;
	}

public:
	std::vector<KeyType> keys;
	std::vector<ValueType> values;
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
		ret = index == -1 ? 0 : &this->values[ index ];
		pthread_mutex_unlock( &this->lock );
		return ret;
	}

	ValueType &operator[]( const int index ) {
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

	bool set( KeyType &key, ValueType &value, bool check = false ) {
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
		pthread_mutex_lock( &this->lock );
		int index = this->indexOf( key );
		if ( index == -1 ) {
			pthread_mutex_unlock( &this->lock );
			return false;
		}
		this->keys.erase( this->keys.begin() + index );
		this->values.erase( this->values.begin() + index );
		pthread_mutex_unlock( &this->lock );
		return true;
	}

	bool removeAt( int index ) {
		pthread_mutex_lock( &this->lock );
		this->keys.erase( this->keys.begin() + index );
		this->values.erase( this->values.begin() + index );
		pthread_mutex_unlock( &this->lock );
		return true;
	}
};

#endif
