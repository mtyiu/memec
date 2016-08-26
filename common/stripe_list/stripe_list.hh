#ifndef __COMMON_STRIPE_LIST_STRIPE_LIST_HH__
#define __COMMON_STRIPE_LIST_STRIPE_LIST_HH__

#include <vector>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdint.h>
#include "../ds/array_map.hh"
#include "../ds/bitmask_array.hh"
#include "../hash/consistent_hash.hh"
#include "../hash/hash_func.hh"

// #define USE_CONSISTENT_HASHING

#ifndef UINT32_MAX
#define UINT32_MAX 0xFFFFFFFF
#endif

enum GenerationAlgorithm {
	ROUND_ROBIN,
	LOAD_AWARE,
	RANDOM
};

typedef struct {
	uint32_t from;
	uint32_t to;
} HashPartition;

typedef struct {
	uint32_t listId;
	uint32_t stripeId;
	uint32_t chunkId;
	bool isParity;
} StripeListIndex;

struct StripeListPartition {
	uint8_t listId;
	uint32_t partitionFrom;
	uint32_t partitionTo;
	std::vector<uint8_t> indices;
};

template <class T> class StripeList {
protected:
	typedef struct {
		uint32_t numServers;
		uint32_t numLists;
		BitmaskArray *data, *parity;
		unsigned int *load, *count;
		std::vector<T **> lists;
		std::vector<HashPartition> partitions;
	#ifdef USE_CONSISTENT_HASHING
		ConsistentHash<uint32_t> listRing;
	#endif
	} StripeListState;

	GenerationAlgorithm algo;
	uint32_t n, k;
	bool generated, isMigrating;
	StripeListState base, migrating;
	std::vector<T *> *servers;

	/*
	 * Utility functions for generating stripe lists
	 */
	inline uint32_t pickNext( int listId, int chunkId, bool isMigrating = false ) {
		return ( listId + chunkId ) % ( isMigrating ? this->migrating.numServers : this->base.numServers );
	}

	inline uint32_t pickMin( int listId, bool isMigrating = false ) {
		int32_t index = -1;
		uint32_t minLoad = UINT32_MAX;
		uint32_t minCount = UINT32_MAX;
		StripeListState &state = isMigrating ? this->migrating : this->base;

		for ( uint32_t i = 0; i < state.numServers; i++ ) {
			if (
				(
					( state.load[ i ] < minLoad ) ||
					( state.load[ i ] == minLoad && state.count[ i ] < minCount )
				) &&
				! state.data->check( listId, i ) && // The server should not be selected before
				! state.parity->check( listId, i ) // The server should not be selected before
			) {
				minLoad = state.load[ i ];
				minCount = state.count[ i ];
				index = i;
			}
		}
		if ( index == -1 ) {
			fprintf( stderr, "Cannot assign a server for stripe list #%d.", listId );
			return 0;
		}
		return ( uint32_t ) index;
	}

	inline uint32_t pickRand( int listId, bool isMigrating = false ) {
		int32_t index = -1;
		uint32_t i;
		StripeListState &state = isMigrating ? this->migrating : this->base;

		while( index == -1 ) {
			i = rand() % state.numServers;
			if (
				! state.data->check( listId, i ) && // The server should not be selected before
				! state.parity->check( listId, i ) // The server should not be selected before
			) {
				index = i;
			}
		}
		if ( index == -1 ) {
			fprintf( stderr, "Cannot assign a server for stripe list #%d.", listId );
			return 0;
		}
		return ( uint32_t ) index;
	}

	void generate( bool verbose = false, GenerationAlgorithm algo = ROUND_ROBIN, bool isMigrating = false ) {
		if ( generated && ! isMigrating )
			return;

		uint32_t i, j, index = 0, dataCount, parityCount;
		StripeListState &state = isMigrating ? this->migrating : this->base;

		for ( i = 0; i < state.numLists; i++ ) {
			T **list = state.lists[ i ];
			for ( j = 0; j < this->n - this->k; j++ ) {
				switch ( algo ) {
					case ROUND_ROBIN : index = pickNext( i, j + this->k, isMigrating ); break;
					case LOAD_AWARE  : index = pickMin ( i, isMigrating );               break;
					case RANDOM      : index = pickRand( i, isMigrating );              break;
				}
				state.parity->set( i, index );
			}
			for ( j = 0; j < this->k; j++ ) {
				switch ( algo ) {
					case ROUND_ROBIN : index = pickNext( i, j, isMigrating ); break;
					case LOAD_AWARE  : index = pickMin ( i, isMigrating );    break;
					case RANDOM      : index = pickRand( i, isMigrating );    break;
				}
				state.data->set( i, index );
			}

			// Implicitly sort the item for LOAD_AWARE and RANDOM
			dataCount = 0;
			parityCount = 0;
			for ( j = 0; j < state.numServers; j++ ) {
				uint32_t serverId = j;
				if ( algo == ROUND_ROBIN )
					serverId = ( serverId + i ) % state.numServers;

				if ( state.data->check( i, serverId ) ) {
					list[ dataCount++ ] = this->servers->at( serverId );
					state.load[ serverId ] += 1;
					state.count[ serverId ]++;
				} else if ( state.parity->check( i, serverId ) ) {
					list[ this->k + ( parityCount++ ) ] = this->servers->at( serverId );
					state.load[ serverId ] += this->k;
					state.count[ serverId ]++;
				}
			}

#ifdef USE_CONSISTENT_HASHING
			state.listRing.add( i );
#endif
		}

		this->generated = true;
		this->assign( isMigrating );
	}

	void assign( bool isMigrating = false ) {
		StripeListState &state = isMigrating ? this->migrating : this->base;
		uint32_t size = UINT32_MAX / state.numLists;
		for ( uint32_t i = 0; i < state.numLists; i++ ) {
			HashPartition partition = {
				.from = i * size,
				.to = ( i == state.numLists - 1 ) ? UINT32_MAX : ( ( i + 1 ) * size - 1 )
			};
			state.partitions.push_back( partition );
		}
	}

	/*
	 * Map hash value to list ID
	 */
	uint32_t getListId( uint32_t hashValue, bool isMigrating = false ) {
		StripeListState &state = isMigrating ? this->migrating : this->base;
		for ( uint32_t i = 0; i < state.numLists; i++ ) {
			if ( state.partitions[ i ].from <= hashValue && state.partitions[ i ].to > hashValue )
				return i;
		}
		fprintf( stderr, "StripeList::getListId(): Cannot get a partition for this hash value: %u.\n", hashValue );
		return -1;
	}

public:
	/*
	 * Initialization
	 */
	StripeList( uint32_t n, uint32_t k, uint32_t numLists, std::vector<T *> &servers, GenerationAlgorithm algo = ROUND_ROBIN ) {
		this->algo = algo;
		this->n = n;
		this->k = k;
		this->generated = false;
		this->isMigrating = false;
		this->servers = &servers;
		this->initState( servers.size(), numLists, false );
		this->generate( false /* verbose */, algo, false );
	}

	/*
	 * Initialize state
	 */
	void initState( uint32_t numServers, uint32_t numLists, bool isMigrating = false ) {
		StripeListState &state = isMigrating ? this->migrating : this->base;

		state.numServers = numServers;
		state.numLists = numLists;
		state.data = new BitmaskArray( numServers, numLists );
		state.parity = new BitmaskArray( numServers, numLists );
		state.load = new unsigned int[ numServers ];
		state.count = new unsigned int[ numServers ];
		memset( state.load, 0, sizeof( unsigned int ) * numServers );
		memset( state.count, 0, sizeof( unsigned int ) * numServers );
		state.lists.reserve( numLists );
		for ( uint32_t i = 0; i < numLists; i++ )
			state.lists.push_back( new T*[ this->n ] );
		state.partitions.reserve( numLists );
	}

	/*
	 * Release memory allocated for state
	 */
	void destroyState( bool isMigrating ) {
		StripeListState &state = isMigrating ? this->migrating : this->base;

		state.numServers = 0;
		state.numLists = 0;

		delete[] state.data;
		delete[] state.parity;
		delete[] state.load;
		delete[] state.count;
		state.data = 0;
		state.parity = 0;
		state.load = 0;
		state.count = 0;

		for ( uint32_t i = 0; i < state.lists.size(); i++ )
			delete[] state.lists[ i ];
		state.lists.clear();
		state.partitions.clear();
	}

	/*
	 * Scale up
	 */
	bool addNewServer( T *server ) {
		if ( this->isMigrating ) {
			fprintf( stderr, "StripeList::addNewServer(): Another data migration process is in-progress. Please try again later.\n" );
			return false;
		}

		this->isMigrating = true;
		this->initState( this->base.numServers + 1, this->base.numLists + 1, true );
		this->generate( false /* verbose */, this->algo, true );

		return true;
	}

	/*
	 * Synchronize parameters from coordinator
	 */
	bool syncParams( bool isMigrating, uint32_t numServers, uint32_t numLists, uint32_t n, uint32_t k ) {
		bool isMatched = true;
		StripeListState &state = isMigrating ? this->migrating : this->base;

		// Fatal error if coding parameters mismatch
		assert( this->n == n && this->k == k );
		isMatched = ( state.numServers == numServers && state.numLists == numLists );

		if ( isMatched )
			return true;

		state.numServers = numServers;
		state.numLists = numLists;

		this->destroyState( true );
		this->initState( numServers, numLists, true );
		this->generate( false, this->algo, true );

		return false;
	}

	/*
	 * Synchronize a specific stripe list from coordinator
	 */
	bool syncStripeList( bool isMigrating, uint8_t listId, uint32_t partitionFrom, uint32_t partitionTo, uint8_t *data, uint8_t *parity ) {
		StripeListState &state = isMigrating ? this->migrating : this->base;
		bool isMatched = true;

		if ( state.partitions[ listId ].from != partitionFrom || state.partitions[ listId ].to != partitionTo ) {
			isMatched = false;
		}

		for ( uint32_t i = 0; isMatched && i < this->n; i++ ) {
			if ( i < this->k ) {
				if ( ! state.data->check( listId, data[ i ] ) ) {
					fprintf( stderr, "Data mismatched!\n" );
					isMatched = false;
				}
			} else {
				if ( ! state.parity->check( listId, parity[ i - this->k ] ) ) {
					fprintf( stderr, "Parity mismatched!\n" );
					isMatched = false;
				}
			}
		}

		if ( ! isMatched ) {
			// Reset load and count vectors
			for ( uint32_t j = 0; j < state.numServers; j++ ) {
				uint32_t serverId = j;
				if ( algo == ROUND_ROBIN )
					serverId = ( serverId + listId ) % state.numServers;

				if ( state.data->check( listId, serverId ) ) {
					state.load[ serverId ] -= 1;
					state.count[ serverId ]--;
					state.data->unset( listId, serverId );
				} else if ( state.parity->check( listId, serverId ) ) {
					state.load[ serverId ] -= this->k;
					state.count[ serverId ]--;
					state.parity->unset( listId, serverId );
				}
			}

			// Update bitmap and list
			uint32_t dataCount = 0, parityCount = 0;
			T **list = state.lists[ listId ];
			for ( uint32_t i = 0; i < this->n; i++ ) {
				uint32_t serverId;
				if ( i < this->k ) {
					serverId = data[ i ];
					state.data->set( listId, serverId );
					list[ dataCount++ ] = this->servers->at( serverId );
					state.load[ serverId ] += 1;
					state.count[ serverId ]++;
				} else {
					serverId = parity[ i - this->k ];
					state.parity->set( listId, serverId );
					list[ this->k + ( parityCount++ ) ] = this->servers->at( serverId );
					state.load[ serverId ] += this->k;
					state.count[ serverId ]++;
				}
			}

			// Update assigned partition
			state.partitions[ listId ].from = partitionFrom;
			state.partitions[ listId ].to   = partitionTo;
		}

		return isMatched;
	}

	/*
	 * Map key to stripe list
	 */
	unsigned int get( const char *key, uint8_t keySize, T **data = 0, T **parity = 0, uint32_t *dataChunkIdPtr = 0, bool full = false, bool isMigrating = false ) {
		unsigned int hashValue = HashFunc::hash( key, keySize );
		uint32_t dataChunkId = hashValue % this->k;
#ifdef USE_CONSISTENT_HASHING
		uint32_t listId = this->listRing.get( key, keySize );
#else
		uint32_t listId = this->getListId( HashFunc::hash( ( char * ) &hashValue, sizeof( hashValue ) ), isMigrating );
#endif
		T **ret = isMigrating ? this->migrating.lists[ listId ] : this->base.lists[ listId ];

		if ( dataChunkIdPtr )
			*dataChunkIdPtr = dataChunkId;

		if ( parity ) {
			for ( uint32_t i = 0; i < this->n - this->k; i++ ) {
				parity[ i ] = ret[ this->k + i ];
			}
		}
		if ( data ) {
			if ( full ) {
				for ( uint32_t i = 0; i < this->k; i++ ) {
					data[ i ] = ret[ i ];
				}
			} else {
				*data = ret[ dataChunkId ];
			}
		}
		return listId;
	}

	/*
	 * Map hash value to stripe list
	 */
	unsigned int getByHash( unsigned int hashValue, T **data, T **parity, bool isMigrating = false ) {
#ifdef USE_CONSISTENT_HASHING
		uint32_t listId = this->listRing.get( hashValue );
#else
		uint32_t listId = this->getListId( HashFunc::hash( ( char * ) &hashValue, sizeof( hashValue ) ), isMigrating );
#endif
		T **ret = isMigrating ? this->migrating.lists[ listId ] : this->base.lists[ listId ];

		for ( uint32_t i = 0; i < this->n - this->k; i++ )
			parity[ i ] = ret[ this->k + i ];
		for ( uint32_t i = 0; i < this->k; i++ )
			data[ i ] = ret[ i ];
		return listId;
	}

	T *get( uint32_t listId, uint32_t chunkId, bool isMigrating = false ) {
		return ( isMigrating ? this->migrating.lists[ listId ][ chunkId ] : this->base.lists[ listId ][ chunkId ] );
	}

	T **get( uint32_t listId, T **parity, T **data = 0, bool isMigrating = false ) {
		T **ret = isMigrating ? this->migrating.lists[ listId ] : this->base.lists[ listId ];
		for ( uint32_t i = 0; i < this->n - this->k; i++ ) {
			parity[ i ] = ret[ this->k + i ];
		}
		if ( data ) {
			for ( uint32_t i = 0; i < this->k; i++ ) {
				data[ i ] = ret[ i ];
			}
		}
		return parity;
	}

	/*
	 * Return the list and stripe IDs whose stripe list contains the specified server
	 */
	std::vector<StripeListIndex> list( uint32_t index, bool isMigrating = false ) {
		uint32_t i, j;
		std::vector<StripeListIndex> ret;
		StripeListState &state = isMigrating ? this->migrating : this->base;

		for ( i = 0; i < state.numLists; i++ ) {
			if ( state.data->check( i, index ) ) {
				StripeListIndex s;
				s.listId = i;
				s.stripeId = 0;
				s.chunkId = 0;
				s.isParity = false;
				for ( j = 0; j < state.numServers; j++ ) {
					uint32_t serverId = j;
					if ( algo == ROUND_ROBIN )
						serverId = ( serverId + i ) % state.numServers;

					if ( serverId == index )
						break;
					if ( state.data->check( i, serverId ) )
						s.chunkId++;
				}
				ret.push_back( s );
			} else if ( state.parity->check( i, index ) ) {
				StripeListIndex s;
				s.listId = i;
				s.stripeId = 0;
				s.chunkId = this->k;
				s.isParity = true;
				for ( j = 0; j < state.numServers; j++ ) {
					uint32_t serverId = j;
					if ( algo == ROUND_ROBIN )
						serverId = ( serverId + i ) % state.numServers;

					if ( serverId == index )
						break;
					if ( state.parity->check( i, serverId ) )
						s.chunkId++;
				}
				ret.push_back( s );
			}
		}
		return ret;
	}

	/*
	 * Update stripe lists during recovery
	 */
	void update( bool isMigrating = false ) {
		if ( ! this->generated ) {
			this->generate();
			return;
		}

		uint32_t i, j, dataCount, parityCount;
		StripeListState &state = isMigrating ? this->migrating : this->base;

		for ( i = 0; i < state.numLists; i++ ) {
			T **list = state.lists[ i ];
			dataCount = 0;
			parityCount = 0;
			for ( j = 0; j < state.numServers; j++ ) {
				uint32_t serverId = j;
				if ( algo == ROUND_ROBIN )
					serverId = ( serverId + i ) % state.numServers;

				if ( state.data->check( i, serverId ) ) {
					list[ dataCount++ ] = this->servers->at( serverId );
				} else if ( state.parity->check( i, serverId ) ) {
					list[ this->k + ( parityCount++ ) ] = this->servers->at( serverId );
				}
			}
		}
	}

	/*
	 * Search for a server in the server list
	 */
	int32_t search( T *target, bool isMigrating = false ) {
		StripeListState &state = isMigrating ? this->migrating : this->base;
		for ( uint32_t i = 0; i < state.numServers; i++ ) {
			if ( target == this->servers->at( i ) )
				return i;
		}
		return -1;
	}

	/*
	 * Return the number of stripe lists
	 */
	inline uint32_t getNumList( bool isMigrating = false ) {
		return isMigrating ? this->migrating.numLists : this->base.numLists;
	}

	/*
	 * Print all internal states of the stripe list
	 */
	void print( FILE *f = stdout, bool isMigrating = false ) {
		uint32_t i, j;
		bool first;
		StripeListState &state = isMigrating ? this->migrating : this->base;

		if ( ! generated ) {
			fprintf( f, "The stripe lists are not generated yet.\n" );
			return;
		}

		fprintf( f, "### Stripe List (%s) ###\n", this->algo == ROUND_ROBIN ? "Round-robin" : ( this->algo == LOAD_AWARE ? "Load-aware" : "Random" ) );
		for ( i = 0; i < state.numLists; i++ ) {
			first = true;
			fprintf( f, "#%u [%10u-%10u]: ((", i, state.partitions[ i ].from, state.partitions[ i ].to );
			for ( j = 0; j < state.numServers; j++ ) {
				uint32_t serverId = j;
				if ( algo == ROUND_ROBIN )
					serverId = ( serverId + i ) % state.numServers;

				if ( state.data->check( i, serverId ) ) {
					fprintf( f, "%s%u", first ? "" : ", ", serverId );
					first = false;
				}
			}
			fprintf( f, "), (" );
			first = true;
			for ( j = 0; j < state.numServers; j++ ) {
				uint32_t serverId = j;
				if ( algo == ROUND_ROBIN )
					serverId = ( serverId + i ) % state.numServers;

				if ( state.parity->check( i, serverId ) ) {
					fprintf( f, "%s%u", first ? "" : ", ", serverId );
					first = false;
				}
			}
			fprintf( f, "))\n" );
		}

		fprintf( f, "\n- Weight vector :" );
		for ( uint32_t i = 0; i < state.numServers; i++ )
			fprintf( f, " %d", state.load[ i ] );
		fprintf( f, "\n- Cost vector   :" );
		for ( uint32_t i = 0; i < state.numServers; i++ )
			fprintf( f, " %d", state.count[ i ] );

		fprintf( f, "\n\n" );

#ifdef USE_CONSISTENT_HASHING
		state.listRing.print( f );

		fprintf( f, "\n" );
#endif
	}

	std::vector<struct StripeListPartition> exportAll( uint32_t &numServers, uint32_t &numLists, uint32_t &n, uint32_t &k, bool isMigrating = false ) {
		std::vector<struct StripeListPartition> ret;
		StripeListState &state = isMigrating ? this->migrating : this->base;

		numServers = state.numServers;
		numLists = state.numLists;
		n = this->n;
		k = this->k;

		ret.reserve( state.numLists );

		for ( uint32_t i = 0; i < state.numLists; i++ ) {
			struct StripeListPartition list;
			list.listId = i;
			list.partitionFrom = state.partitions[ i ].from;
			list.partitionTo   = state.partitions[ i ].to;
			list.indices.reserve( this->n );

			for ( uint32_t j = 0; j < state.numServers; j++ ) {
				uint32_t serverId = j;
				if ( algo == ROUND_ROBIN )
					serverId = ( serverId + i ) % state.numServers;
				if ( state.data->check( i, serverId ) )
					list.indices.push_back( ( uint8_t ) serverId );
			}

			for ( uint32_t j = 0; j < state.numServers; j++ ) {
				uint32_t serverId = j;
				if ( algo == ROUND_ROBIN )
					serverId = ( serverId + i ) % state.numServers;
				if ( state.parity->check( i, serverId ) )
					list.indices.push_back( ( uint8_t ) serverId );
			}

			ret.push_back( list );
		}

		return ret;
	}

	~StripeList() {
		this->destroyState( false );
		if ( this->isMigrating )
			this->destroyState( true );
	}
};

#endif
