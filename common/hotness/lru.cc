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

bool LruHotness::insert( KeyMetadata newItem ) {
	struct LruListRecord *target = 0;
	Key key;
	key.set( newItem.length, newItem.ptr );

	LOCK( &this->lock );

	if ( this->existingRecords.count( newItem ) > 0 ) {
		// reuse existing record
		target = this->existingRecords.at( newItem );
		list_move( &target->listPtr, &this->lruList );

	} else if ( this->freeRecords.next != &this->freeRecords ) {
		// allocate a new record
		target = container_of( this->freeRecords.next, LruListRecord, listPtr );
		list_move( &target->listPtr, &this->lruList );
		// replicate key for internal reference
		key.dup();
		target->metadata = newItem;
		target->key = key;
	} else {
		// replace the oldest record
		target = container_of( this->lruList.prev, LruListRecord, listPtr );
		list_move( &target->listPtr, &this->lruList );
		this->existingRecords.erase( target->metadata );
		target->key.free();
		// replicate key for internal reference
		key.dup();
		target->metadata = newItem;
		target->key = key;
	}

	// mark the metadata as exists in LRU list
	std::pair<Metadata, LruListRecord*> record( newItem, target );

	if ( this->existingRecords.count( newItem ) == 0 ) {
		this->existingRecords.insert( record );
	}

	UNLOCK( &this->lock );
	return ( target != 0 );
}

std::vector<KeyMetadata> LruHotness::getItems() {
	std::vector<KeyMetadata> list;
	KeyMetadata keyMetadata;
	LruListRecord *record;
	Key key;
	struct list_head *rec;

	LOCK( &this->lock );

	list_for_each( rec, &this->lruList ) {
		record = container_of( rec, LruListRecord, listPtr );
		keyMetadata.set( record->metadata.listId, record->metadata.stripeId, record->metadata.chunkId );
		key.set( record->key.size, record->key.data );
		key.dup();
		keyMetadata.length = key.size;
		keyMetadata.ptr    = key.data;
		list.push_back( keyMetadata );
	}

	UNLOCK( &this->lock );

	return list;
}

std::vector<KeyMetadata> LruHotness::getTopNItems( size_t n ) {
	std::vector<KeyMetadata> list;
	KeyMetadata keyMetadata;
	LruListRecord *record;
	Key key;
	struct list_head *rec;

	LOCK( &this->lock );

	list_for_each( rec, &this->lruList ) {
		record = container_of( rec, LruListRecord, listPtr );
		keyMetadata.set( record->metadata.listId, record->metadata.stripeId, record->metadata.chunkId );
		key.set( record->key.size, record->key.data );
		key.dup();
		keyMetadata.length = key.size;
		keyMetadata.ptr    = key.data;
		list.push_back( keyMetadata );
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
	struct list_head *rec;

	LOCK( &this->lock );

	list_for_each( rec, &this->freeRecords ) {
		count++;
	}

	UNLOCK( &this->lock );

	return count;
}

size_t LruHotness::getAndReset( std::vector<KeyMetadata>& dest, size_t n ) {
	KeyMetadata keyMetadata;
	LruListRecord *record;
	struct list_head *rec, *savePtr;

	LOCK( &this->lock );

	list_for_each_safe( rec, savePtr, &this->lruList ) {
		record = container_of( rec, LruListRecord, listPtr );
		keyMetadata.set( record->metadata.listId, record->metadata.stripeId, record->metadata.chunkId );
		keyMetadata.length = record->key.size;
		keyMetadata.ptr    = record->key.data;
		dest.push_back( keyMetadata );
		if ( n == 0 || dest.size() < n ) {
			dest.push_back( keyMetadata );
		}
		this->existingRecords.erase( record->metadata );
		list_move( rec, &this->freeRecords );
	}

	UNLOCK( &this->lock );

	return dest.size();
}

void LruHotness::reset() {
	std::vector<KeyMetadata> list;
	struct LruListRecord *record;
	struct list_head *rec, *savePtr;

	LOCK( &this->lock );

	list_for_each_safe( rec, savePtr, &this->lruList ) {
		record = container_of( rec, LruListRecord, listPtr );
		record->key.free();
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
	LruListRecord *record;

	fprintf( output, "Free: %lu; Used: %lu\n", this->getFreeItemCount(), this->getItemCount() );
	LOCK( &this->lock );
	list_for_each( rec, &this->lruList ) {
		record = container_of( rec, LruListRecord, listPtr );
		fprintf( output, "Record [%lu]: list = %u, stripe = %u, chunk = %u; key(%u):%.*s\n",
			i, record->metadata.listId, record->metadata.stripeId, record->metadata.chunkId,
			record->key.size, record->key.size, record->key.data
		);
		i++;
	}
	UNLOCK( &this->lock );
}
