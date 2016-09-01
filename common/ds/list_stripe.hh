#ifndef __COMMON_DS_LIST_STRIPE_HH__
#define __COMMON_DS_LIST_STRIPE_HH__

#include <unordered_set>

class ListStripe {
public:
	uint32_t listId;
	uint32_t stripeId;

	void clear() {
		this->listId = 0;
		this->stripeId = 0;
	}

	void set( uint32_t listId, uint32_t stripeId ) {
		this->listId = listId;
		this->stripeId = stripeId;
	}

	void clone( const ListStripe &l ) {
		this->listId = l.listId;
		this->stripeId = l.stripeId;
	}

	bool operator<( const ListStripe &l ) const {
		if ( this->listId < l.listId )
			return true;
		if ( this->listId > l.listId )
			return false;

		return this->stripeId < l.stripeId;
	}

	bool operator==( const ListStripe &l ) const {
		return (
			this->listId == l.listId &&
			this->stripeId == l.stripeId
		);
	}
};

namespace std {
	template<> struct hash<ListStripe> {
		size_t operator()( const ListStripe &listStripe ) const {
			size_t ret = 0;
			char *ptr = ( char * ) &ret, *tmp;
			tmp = ( char * ) &listStripe.stripeId;
			ptr[ 0 ] = tmp[ 0 ];
			ptr[ 1 ] = tmp[ 1 ];
			ptr[ 2 ] = tmp[ 2 ];
			ptr[ 3 ] = tmp[ 3 ];
			tmp = ( char * ) &listStripe.listId;
			ptr[ 4 ] = tmp[ 0 ];
			ptr[ 5 ] = tmp[ 1 ];
			ptr[ 6 ] = tmp[ 2 ];
			ptr[ 7 ] = tmp[ 3 ];
			return ret;
		}
	};
}

#endif
