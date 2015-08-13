#ifndef __COMMON_DS_ARRAY_MAP_HH__
#define __COMMON_DS_ARRAY_MAP_HH__

#include <vector>

template<typename KeyType, typename ValueType> class ArrayMap {
public:
	std::vector<KeyType> keys;
	std::vector<ValueType> values;

	int indexOf( KeyType &key ) {
		for ( int i = 0, len = this->keys.size(); i < len; i++ ) {
			if ( this->keys[ i ] == key )
				return i;
		}
		return -1;
	}

	void reserve( int n ) {
		this->keys.reserve( n );
		this->values.reserve( n );
	}

	ValueType *get( KeyType &key, int *indexPtr = 0 ) {
		int index = this->indexOf( key );
		if ( indexPtr )
			*indexPtr = index;
		return index == -1 ? 0 : &this->values[ index ];
	}

	ValueType &operator[]( const int index ) {
		return this->values[ index ];
	}

	bool replaceKey( KeyType &oldKey, KeyType &newKey ) {
		int index = this->indexOf( oldKey );
		if ( index != -1 ) {
			this->keys[ index ] = newKey;
			return true;
		}
		return false;
	}

	size_t size() {
		return this->values.size();
	}

	bool set( KeyType &key, ValueType &value, bool check = false ) {
		if ( check && this->indexOf( key ) != -1 )
			return false;
		this->keys.push_back( key );
		this->values.push_back( value );
		return true;
	}

	bool remove( KeyType &key ) {
		int index = this->indexOf( key );
		if ( index == -1 )
			return false;
		this->keys.erase( this->keys.begin() + index );
		this->values.erase( this->values.begin() + index );
		return true;
	}

	bool removeAt( int index ) {
		this->keys.erase( this->keys.begin() + index );
		this->values.erase( this->values.begin() + index );
		return true;
	}
};

#endif
