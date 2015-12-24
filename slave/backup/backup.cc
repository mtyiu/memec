#include "backup.hh"
#include "../main/slave.hh"

SlaveBackup::SlaveBackup() {
	Slave* slave = Slave::getInstance();
	
	this->localTime = &slave->timestamp;

	LOCK_INIT( &this->dataUpdateLock );
	LOCK_INIT( &this->parityUpdateLock );
	LOCK_INIT( &this->dataDeleteLock );
	LOCK_INIT( &this->parityDeleteLock );
}

SlaveBackup::~SlaveBackup() {
}

#define DEFINE_INSERT_METHOD( _METHOD_NAME_, _VAR_NAME_ ) \
	bool SlaveBackup::insert##_METHOD_NAME_( Timestamp ts, Key key, Value value, uint32_t listId, uint32_t chunkId ) { \
		LOCK_T *lock = &this->_VAR_NAME_##Lock; \
		std::multimap<Timestamp, PendingData> *map = &this->_VAR_NAME_; \
		PendingData pendingData; \
		\
		LOCK( lock ); \
		pendingData.set( listId, chunkId, key, value ); \
		map->insert( std::pair<Timestamp, PendingData>( ts, pendingData ) ); \
		UNLOCK( lock ); \
		return true; \
	}

#define DEFINE_REMOVE_METHOD( _METHOD_NAME_, _VAR_NAME_ ) \
	uint32_t SlaveBackup::remove##_METHOD_NAME_( Timestamp ts ) { \
		LOCK_T *lock = &this->_VAR_NAME_##Lock; \
		std::multimap<Timestamp, PendingData> *map = &this->_VAR_NAME_; \
		\
		std::multimap<Timestamp, PendingData>::iterator lit, rit; \
		\
		LOCK( lock );  \
		rit = map->upper_bound( ts ); \
		for ( lit = map->begin(); lit != rit; lit++ ) { \
			lit->second.free(); \
		} \
		map->erase( map->begin(), rit ); \
		UNLOCK( lock ); \
		\
		return true; \
	}
	
DEFINE_INSERT_METHOD( DataUpdate, dataUpdate )
DEFINE_INSERT_METHOD( ParityUpdate, parityUpdate )
DEFINE_INSERT_METHOD( DataDelete, dataDelete )
DEFINE_INSERT_METHOD( ParityDelete, parityDelete )

DEFINE_REMOVE_METHOD( DataUpdate, dataUpdate )
DEFINE_REMOVE_METHOD( ParityUpdate, parityUpdate )
DEFINE_REMOVE_METHOD( DataDelete, dataDelete )
DEFINE_REMOVE_METHOD( ParityDelete, parityDelete )

#undef DEFINE_INSERT_METHOD
#undef DEFINE_REMOVE_METHOD
