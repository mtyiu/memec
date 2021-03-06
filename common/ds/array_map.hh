#ifndef __COMMON_DS_ARRAY_MAP_HH__
#define __COMMON_DS_ARRAY_MAP_HH__

#include <vector>
#include <cstdio>
#include "../lock/lock.hh"

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
	bool needsDelete;
	LOCK_T lock;

	ArrayMap() {
		LOCK_INIT( &this->lock );
		this->needsDelete = true;
	}

	ArrayMap( ArrayMap const& arrayMap ) {
		size_t i;
		LOCK_INIT( &this->lock );
		this->needsDelete = true;
		this->clear();
		for ( i = 0; i < arrayMap.size(); i++ ) {
			this->keys.push_back( arrayMap.keys[ i ] );
			this->values.push_back( new ValueType( arrayMap.values[ i ] ) );
		}
	}

	void reserve( int n ) {
		this->keys.reserve( n );
		this->values.reserve( n );
	}

	ValueType *get( KeyType &key, int *indexPtr = 0 ) {
		ValueType *ret;
		LOCK( &this->lock );
		int index = this->indexOf( key );
		if ( indexPtr )
			*indexPtr = index;
		ret = index == -1 ? 0 : this->values[ index ];
		UNLOCK( &this->lock );
		return ret;
	}

	ValueType *operator[]( const int index ) {
		return this->values[ index ];
	}

	bool replaceKey( KeyType &oldKey, KeyType &newKey ) {
		LOCK( &this->lock );
		int index = this->indexOf( oldKey );
		if ( index != -1 )
			this->keys[ index ] = newKey;
		UNLOCK( &this->lock );
		return index != -1;
	}

	size_t size() const {
		return this->values.size();
	}

	bool set( KeyType &key, ValueType *value, bool check = false ) {
		LOCK( &this->lock );
		if ( check && this->indexOf( key ) != -1 ) {
			UNLOCK( &this->lock );
			return false;
		}
		this->keys.push_back( key );
		this->values.push_back( value );
		UNLOCK( &this->lock );
		return true;
	}

	ValueType *set( int index, KeyType &key, ValueType *value ) {
		ValueType *ret;
		LOCK( &this->lock );
		this->keys[ index ] = key;
		ret = this->values[ index ];
		this->values[ index ] = value;
		UNLOCK( &this->lock );
		return ret;
	}

	bool remove( KeyType &key ) {
		ValueType *val;
		LOCK( &this->lock );
		int index = this->indexOf( key );
		if ( index == -1 ) {
			UNLOCK( &this->lock );
			return false;
		}
		val = this->values[ index ];
		this->keys.erase( this->keys.begin() + index );
		this->values.erase( this->values.begin() + index );
		UNLOCK( &this->lock );
		if ( needsDelete ) delete val;
		return true;
	}

	bool removeAt( int index ) {
		ValueType *val;
		LOCK( &this->lock );
		val = this->values[ index ];
		this->keys.erase( this->keys.begin() + index );
		this->values.erase( this->values.begin() + index );
		UNLOCK( &this->lock );
		if ( needsDelete ) delete val;
		return true;
	}

	void clear() {
		LOCK( &this->lock );
		for ( int i = 0, len = this->keys.size(); i < len && needsDelete ; i++ )
			delete this->values[ i ];
		this->keys.clear();
		this->values.clear();
		UNLOCK( &this->lock );
	}
};

#endif
