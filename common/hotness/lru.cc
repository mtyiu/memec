#include "lru.hh"

LruHotness::LruHotness() : Hotness() {
	this->init( DEFAULT_MAX_HOTNESS_ITEMS );
}

LruHotness::LruHotness( size_t maxItems ) : Hotness( maxItems ) {
	this->init( maxItems );
}

LruHotness::~LruHotness() {
	if ( slots ) 
		delete [] this->slots;
}

bool LruHotness::insert( Metadata newItem ) {
	struct LruListRecord *target = 0;
	LOCK( &this->lock );

	if ( this->existingRecords.count( newItem ) > 0 ) {
		// reuse existing record
		target = this->existingRecords.at( newItem );
		list_move( &target->listPtr, &this->lruList );
	} else if ( this->freeRecords.next != &this->freeRecords ) {
		// allocate a new record
		target = container_of( this->freeRecords.next, LruListRecord, listPtr );
		list_move( &target->listPtr, &this->lruList );
	} else {
		// replace the oldest record
		target = container_of( this->lruList.prev, LruListRecord, listPtr );
		list_move( &target->listPtr, &this->lruList );
		this->existingRecords.erase( target->metadata );

	}
	// mark the metadata as exists in LRU list
	
	std::pair<Metadata, LruListRecord*> record( newItem, target );
	target->metadata = newItem;
	if ( this->existingRecords.count( newItem ) == 0 ) {
		this->existingRecords.insert( record );
	}

	UNLOCK( &this->lock );
	return ( target != 0 );
}

std::vector<Metadata> LruHotness::getItems() {
	std::vector<Metadata> list;

	LOCK( &this->lock );

	struct list_head *rec;
	list_for_each( rec, &this->lruList ) {
		list.push_back( container_of( rec, LruListRecord, listPtr )->metadata );
	}

	UNLOCK( &this->lock );

	return list;
}

std::vector<Metadata> LruHotness::getTopNItems( size_t n ) {
	std::vector<Metadata> list;

	LOCK( &this->lock );

	struct list_head *rec;
	list_for_each( rec, &this->lruList ) {
		list.push_back( container_of( rec, LruListRecord, listPtr )->metadata );
		if ( list.size() > n )
			break;
	}

	UNLOCK( &this->lock );
	return list;
}

size_t LruHotness::getItemCount() {
	size_t count = 0;

	LOCK( &this->lock );
	count = this->existingRecords.size();
	UNLOCK( &this->lock );

	return count;
}

size_t LruHotness::getFreeItemCount() {
	size_t count = 0;

	LOCK( &this->lock );

	struct list_head *rec;
	list_for_each( rec, &this->freeRecords ) {
		count++;
	}

	UNLOCK( &this->lock );

	return count;
}

size_t LruHotness::getAndReset( std::vector<Metadata>& dest, size_t n ) {

	LOCK( &this->lock );

	struct list_head *rec, *savePtr;
	list_for_each_safe( rec, savePtr, &this->lruList ) {
		Metadata &metadata = container_of( rec, LruListRecord, listPtr )->metadata;
		if ( n == 0 || dest.size() < n ) {
			dest.push_back( metadata );
		}
		this->existingRecords.erase( metadata );
		list_move( rec, &this->freeRecords );
	}

	UNLOCK( &this->lock );

	return dest.size();
}

void LruHotness::reset() {
	std::vector<Metadata> list;

	LOCK( &this->lock );

	struct list_head *rec, *savePtr;

	list_for_each_safe( rec, savePtr, &this->lruList ) {
		this->existingRecords.erase( container_of( rec, LruListRecord, listPtr )->metadata );
		list_move( rec, &this->freeRecords );
	}

	UNLOCK( &this->lock );
}

void LruHotness::init( size_t maxItems ) {
	this->maxItems = maxItems;
	LOCK_INIT( &this->lock );
	INIT_LIST_HEAD( &this->freeRecords );
	INIT_LIST_HEAD( &this->lruList );
	slots = new struct LruListRecord[ this->maxItems ];
	for ( size_t i = 0; i < this->maxItems; i++ ) {
		INIT_LIST_HEAD( &slots[ i ].listPtr );
		list_add( &slots[ i ].listPtr, &this->freeRecords );
	}
}

void LruHotness::print( FILE *output ) {
	struct list_head *rec;
	size_t i = 0;
	Metadata *metadata;

	fprintf( output, "Free: %lu; Used: %lu\n", this->getFreeItemCount(), this->getItemCount() );
	LOCK( &this->lock );
	list_for_each( rec, &this->lruList ) {
		metadata = &( container_of( rec, LruListRecord, listPtr )->metadata );
		fprintf( output, "Record [%lu]: list = %u, stripe = %u, chunk = %u\n",
			i, metadata->listId, metadata->stripeId, metadata->chunkId
		);
		i++;
	}
	UNLOCK( &this->lock );
}
