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

// INSERT

#define DEFINE_INSERT_METHOD( _CONTENT_TYPE_, _VAR_TYPE_, _OP_TYPE_ ) \
	bool SlaveBackup::insert##_CONTENT_TYPE_##_OP_TYPE_( Timestamp ts, Key key, Value value, Metadata metadata, bool isChunkDelta, uint32_t offset, uint32_t requestId, uint16_t targetInstanceId ) { \
		LOCK_T *lock = &this->_VAR_TYPE_##_OP_TYPE_##Lock; \
		std::multimap<Timestamp, BackupDelta> *map = &this->_VAR_TYPE_##_OP_TYPE_; \
		BackupDelta backupDelta; \
		bool isData = ( strncmp( #_CONTENT_TYPE_ , "Data", 4 ) == 0 ); \
		bool ret = true; \
		\
		LOCK( lock ); \
		backupDelta.set( metadata, key, value, isChunkDelta, offset, !isData, requestId ); \
		map->insert( std::pair<Timestamp, BackupDelta>( ts, backupDelta ) ); \
		if ( isData ) { \
			BackupPendingIdentifier pi( requestId, targetInstanceId ); \
			if ( this->idToTimestampMap.count( pi ) > 0 ) { \
				__ERROR__( "SlaveBackup", "insertData*", "Duplicated request Id [%d] for %s!", requestId, #_VAR_TYPE_ ); \
				ret = false; \
			} else { \
				this->idToTimestampMap.insert( std::pair<BackupPendingIdentifier, Timestamp>( pi, ts ) ); \
			} \
		} \
		UNLOCK( lock ); \
		return ret; \
	}

#define DEFINE_INSERT_DATA_METHOD( _OP_TYPE_ ) \
	DEFINE_INSERT_METHOD( Data, data, _OP_TYPE_ )

#define DEFINE_INSERT_PARITY_METHOD( _OP_TYPE_ ) \
	DEFINE_INSERT_METHOD( Parity, parity, _OP_TYPE_ )

// REMOVE

#define DEFINE_REMOVE_BY_TIMESTAMP_METHOD( _CONTENT_TYPE_, _VAR_TYPE_, _OP_TYPE_ ) \
	std::vector<BackupDelta> SlaveBackup::remove##_CONTENT_TYPE_##_OP_TYPE_( Timestamp ts, bool free ) { \
		std::vector<BackupDelta> ret; \
		LOCK_T *lock = &this->_VAR_TYPE_##_OP_TYPE_##Lock; \
		std::multimap<Timestamp, BackupDelta> *map = &this->_VAR_TYPE_##_OP_TYPE_; \
		\
		std::multimap<Timestamp, BackupDelta>::iterator lit, rit; \
		bool isData = ( strncmp( #_CONTENT_TYPE_ , "Data", 4 ) == 0 ); \
		\
		LOCK( lock );  \
		lit = map->lower_bound( ts ); \
		rit = map->upper_bound( ts ); \
		if ( lit != map->end() ) { \
			for ( lit = map->begin(); lit != rit; lit++ ) { \
				if ( free ) \
					lit->second.free(); \
				else \
					ret.push_back( lit->second ); \
				if ( isData ) { \
					BackupPendingIdentifier pi ( lit->second.requestId, 0 ) ; \
					if ( ! lit->second.isParity && this->idToTimestampMap.count( pi ) > 0 ) { \
						this->idToTimestampMap.erase( pi ); \
					} \
				} \
			} \
			map->erase( map->begin(), rit ); \
		} \
		UNLOCK( lock ); \
		\
		return ret; \
	}

#define DEFINE_REMOVE_PARITY_METHOD( _OP_TYPE_ ) \
	DEFINE_REMOVE_BY_TIMESTAMP_METHOD( Parity, parity, _OP_TYPE_ )

#define DEFINE_REMOVE_DATA_METHOD( _OP_TYPE_ ) \
	DEFINE_REMOVE_BY_TIMESTAMP_METHOD( Data, data, _OP_TYPE_ )

#define DEFINE_REMOVE_BY_ID_METHOD( _CONTENT_TYPE_, _VAR_TYPE_, _OP_TYPE_ ) \
	BackupDelta SlaveBackup::remove##_CONTENT_TYPE_##_OP_TYPE_( uint32_t requestId, uint16_t targetInstanceId, bool free ) { \
		BackupDelta ret; \
		Timestamp ts; \
		LOCK_T *lock = &this->_VAR_TYPE_##_OP_TYPE_##Lock; \
		std::multimap<Timestamp, BackupDelta> *map = &this->_VAR_TYPE_##_OP_TYPE_; \
		\
		LOCK( lock );  \
		BackupPendingIdentifier pi( requestId, targetInstanceId ); \
		std::unordered_multimap<BackupPendingIdentifier, Timestamp>::iterator pit = this->idToTimestampMap.find( pi ); \
		if ( pit == this->idToTimestampMap.end() ) { \
			UNLOCK( lock ); \
			return ret; \
		} else { \
			ts = pit->second; \
		} \
		\
		this->idToTimestampMap.erase( pit ); \
		std::multimap<Timestamp, BackupDelta>::iterator lit, rit; \
		\
		lit = map->lower_bound( ts ); \
		rit = map->upper_bound( ts ); \
		if ( lit != map->end() ) { \
			for ( ; lit != rit; lit++ ) { \
				if ( lit->second.requestId == requestId ) { \
					ret = lit->second; \
					if ( free ) \
						lit->second.free(); \
					map->erase( lit ); \
					break; \
				} \
			} \
		} \
		\
		UNLOCK( lock ); \
		return ret; \
	}

#define DEFINE_REMOVE_DATA_BY_ID_METHOD( _OP_TYPE_ ) \
	DEFINE_REMOVE_BY_ID_METHOD( Data, data, _OP_TYPE_ )

#define DEFINE_REMOVE_PARITY_BY_ID_METHOD( _OP_TYPE_ ) \
	DEFINE_REMOVE_BY_ID_METHOD( Parity, parity, _OP_TYPE_ )


DEFINE_INSERT_DATA_METHOD( Update )
DEFINE_INSERT_PARITY_METHOD( Update )
DEFINE_INSERT_DATA_METHOD( Delete )
DEFINE_INSERT_PARITY_METHOD( Delete )

DEFINE_REMOVE_DATA_METHOD( Update )
DEFINE_REMOVE_PARITY_METHOD( Update )
DEFINE_REMOVE_DATA_METHOD( Delete )
DEFINE_REMOVE_PARITY_METHOD( Delete )

DEFINE_REMOVE_DATA_BY_ID_METHOD( Update );
DEFINE_REMOVE_DATA_BY_ID_METHOD( Delete );

#undef DEFINE_INSERT_METHOD
#undef DEFINE_INSERT_DATA_METHOD
#undef DEFINE_INSERT_PARITY_METHOD
#undef DEFINE_REMOVE_METHOD
#undef DEFINE_REMOVE_DATA_BY_ID_METHOD
#undef DEFINE_REMOVE_PARITY_BY_ID_METHOD
#undef DEFINE_REMOVE_DATA_BY_ID_METHOD
