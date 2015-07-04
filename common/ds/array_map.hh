#ifndef __ARRAY_MAP_HH__
#define __ARRAY_MAP_HH__

#include <vector>

template<typename KeyType, typename ValueType> class ArrayMap {
private:
	std::vector<KeyType> keys;
	std::vector<ValueType> values;

	int indexOf( KeyType &key ) {
		for ( int i = 0, len = this->keys.size(); i < len; i++ ) {
			if ( this->keys[ i ] == key )
				return i;
		}
		return -1;
	}

public:
	void reserve( int n ) {
		this->keys.reserve( n );
		this->values.reserve( n );
	}

	ValueType *get( KeyType &key ) {
		int index = this->indexOf( key );
		return index == -1 ? 0 : &this->values[ index ];
	}

	bool set( KeyType &key, ValueType &value, bool check = true ) {
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
};

#endif
