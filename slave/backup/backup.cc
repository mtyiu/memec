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

// ------------------------------- INSERT ----------------------------------

#define DEFINE_INSERT_METHOD( _CONTENT_TYPE_, _VAR_TYPE_, _OP_TYPE_ ) \
	bool SlaveBackup::insert##_CONTENT_TYPE_##_OP_TYPE_( Timestamp ts, Key key, Value value, Metadata metadata, bool isChunkDelta, uint32_t offset, uint32_t requestId, Socket *targetSocket ) { \
		LOCK_T *lock = &this->_VAR_TYPE_##_OP_TYPE_##Lock; \
		std::multimap<Timestamp, BackupDelta> *map = &this->_VAR_TYPE_##_OP_TYPE_; \
		BackupDelta backupDelta; \
		bool isData = ( strncmp( #_CONTENT_TYPE_ , "Data", 4 ) == 0 ); \
		bool ret = true; \
		bool duplicate = false; \
		\
		LOCK( lock ); \
		/* if this is data* delta */ \
		BackupPendingIdentifier pi( requestId, targetSocket ); \
		if ( isData ) { \
			if ( this->idToTimestampMap.count( pi ) > 0 ) { \
				__ERROR__( "SlaveBackup", "insertData*", "Duplicated request Id [%d] for %s!", requestId, #_VAR_TYPE_ ); \
				ret = false; \
			} else if ( this->idToTimestampMap.lower_bound( pi ) != this->idToTimestampMap.end() ) { \
				/* keep one copy of backup, but mutltiple ref indexed by a request id */ \
				duplicate = true; \
			} \
		} \
		if ( ret ) { \
			this->idToTimestampMap.insert( std::pair<BackupPendingIdentifier, Timestamp>( pi, ts ) ); \
			if ( ! duplicate ) { \
				backupDelta.set( metadata, key, value, isChunkDelta, offset, !isData, requestId ); \
				map->insert( std::pair<Timestamp, BackupDelta>( ts, backupDelta ) ); \
			} \
		} \
		UNLOCK( lock ); \
		return ret; \
	}

#define DEFINE_INSERT_DATA_METHOD( _OP_TYPE_ ) \
	DEFINE_INSERT_METHOD( Data, data, _OP_TYPE_ )

#define DEFINE_INSERT_PARITY_METHOD( _OP_TYPE_ ) \
	DEFINE_INSERT_METHOD( Parity, parity, _OP_TYPE_ )

// ------------------------------- REMOVE ----------------------------------

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
	BackupDelta SlaveBackup::remove##_CONTENT_TYPE_##_OP_TYPE_( uint32_t requestId, Socket *targetSocket, bool free ) { \
		BackupDelta ret; \
		Timestamp ts; \
		LOCK_T *lock = &this->_VAR_TYPE_##_OP_TYPE_##Lock; \
		std::multimap<Timestamp, BackupDelta> *map = &this->_VAR_TYPE_##_OP_TYPE_; \
		\
		LOCK( lock );  \
		BackupPendingIdentifier pi( requestId, targetSocket ); \
		std::multimap<BackupPendingIdentifier, Timestamp>::iterator pit = this->idToTimestampMap.find( pi ); \
		if ( pit == this->idToTimestampMap.end() ) { \
			UNLOCK( lock ); \
			return ret; \
		} else { \
			ts = pit->second; \
		} \
		this->idToTimestampMap.erase( pit ); \
		/* see if there is more pending reference */ \
		pit = this->idToTimestampMap.lower_bound( pi ); \
		\
		std::multimap<Timestamp, BackupDelta>::iterator lit, rit; \
		\
		lit = map->lower_bound( ts ); \
		rit = map->upper_bound( ts ); \
		/* only remove backup after all parity slave acked */ \
		if ( lit != map->end() && pit == this->idToTimestampMap.end() ) { \
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

// ------------------------------- FIND ----------------------------------

// ------------------------------- PRINT ----------------------------------

void SlaveBackup::print( FILE *f, bool printDelta ) {
	fprintf( f, 
		"Data slave backup: (Update) %lu (Delete) %lu\n"
		"Parity slave backup: (Update) %lu (Delete) %lu\n"
		"Pending slave backup: %lu\n"
		, this->dataUpdate.size(),
		this->dataDelete.size(),
		this->parityUpdate.size(),
		this->parityDelete.size(),
		this->idToTimestampMap.size()
	);
	if ( ! printDelta ) return;

#define PRINT_DELTA_IN_MAP( _OUT_, _MAP_ ) { \
	uint32_t i = 0; \
	for ( auto& it : _MAP_ ) { \
		i++; \
		fprintf( _OUT_, \
			"%7u key: (%u) %.*s;    delta: (%u) [%.*s]\n", \
			i, \
			it.second.key.size, \
			it.second.key.size, \
			( it.second.key.data )? it.second.key.data : "[N/A]", \
			it.second.delta.data.size, \
			it.second.delta.data.size, \
			( it.second.delta.data.data )? it.second.delta.data.data : "[N/A]" \
		); \
	} \
}

	PRINT_DELTA_IN_MAP( f, this->dataUpdate );
	PRINT_DELTA_IN_MAP( f, this->dataDelete );
	PRINT_DELTA_IN_MAP( f, this->parityUpdate );
	PRINT_DELTA_IN_MAP( f, this->parityDelete );

#undef PRINT_DELTA_IN_MAP

}

#undef DEFINE_INSERT_METHOD
#undef DEFINE_INSERT_DATA_METHOD
#undef DEFINE_INSERT_PARITY_METHOD
#undef DEFINE_REMOVE_METHOD
#undef DEFINE_REMOVE_DATA_BY_ID_METHOD
#undef DEFINE_REMOVE_PARITY_BY_ID_METHOD
#undef DEFINE_REMOVE_DATA_BY_ID_METHOD
