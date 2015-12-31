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
			std::multimap<BackupPendingIdentifier, Timestamp>::iterator lit, rit; \
			tie( lit, rit ) = this->idToTimestampMap.equal_range( pi ); \
			int pending = 0; \
			for( ; lit != rit; lit++ ) { \
				if ( lit->first.targetSocket == targetSocket ) { \
					__ERROR__( "SlaveBackup", "insertData*", "Duplicated request Id [%d] for %s!", requestId, #_OP_TYPE_ ); \
					ret = false; \
					break; \
				} \
				pending++; \
			} \
			/* keep one copy of backup, but mutltiple ref indexed by a request id */ \
			duplicate = ( ! ret ) || ( lit == rit && pending > 0 ); \
			if ( ret ) { \
				this->idToTimestampMap.insert( std::pair<BackupPendingIdentifier, Timestamp>( pi, ts ) ); \
			} \
		} \
		if ( ! duplicate && ret ) { \
			backupDelta.set( metadata, key, value, isChunkDelta, offset, !isData, requestId ); \
			map->insert( std::pair<Timestamp, BackupDelta>( ts, backupDelta ) ); \
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
		std::multimap<BackupPendingIdentifier, Timestamp>::iterator lpit, rpit, tpit; \
		tie( lpit, rpit ) = this->idToTimestampMap.equal_range( pi ); \
		for( tpit = lpit ; tpit != rpit && tpit->first.targetSocket != targetSocket; tpit++ ); \
		if ( tpit == rpit ) { \
			__ERROR__( "SlaveBackup", "removeById", "Cannnot find pending parity slave for request ID = %u from socket FD = %u.", requestId, targetSocket->getSocket() ); \
			UNLOCK( lock ); \
			return ret; \
		} \
		ts = tpit->second; \
		int pending = 0; \
		for ( ; lpit != rpit; lpit++ ) \
			pending++; \
		this->idToTimestampMap.erase( tpit ); \
		/* if the only pending reference is removed, remove the backup as well */ \
		if ( pending == 1 ) { \
			std::multimap<Timestamp, BackupDelta>::iterator lit, rit; \
			\
			tie( lit, rit ) = map->equal_range( ts ); \
			/* only remove backup after all parity slave acked */ \
			for ( ; lit != rit; lit++ ) { \
				if ( lit->second.requestId == requestId ) { \
					ret = lit->second; \
					if ( free ) \
						lit->second.free(); \
					map->erase( lit ); \
					break; \
				} \
			} \
			if ( lit == rit ) { \
				__ERROR__( "SlaveBackup" , "removeById", "Cannot find backup for request ID = %u at time = %u.", requestId, ts.getVal() ); \
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
			"%7u  Timestamp: %10u;  key: (%4u) %.*s;  offset: %4u;  isChunkDelta:%1hhu;  isParity:%1hhu;  delta: (%4u) [", \
			i, \
			it.first.getVal(), \
			it.second.key.size, \
			it.second.key.size, \
			( it.second.key.data )? it.second.key.data : "[N/A]", \
			it.second.delta.offset, \
			it.second.isChunkDelta, \
			it.second.isParity, \
			it.second.delta.data.size \
		); \
		if ( ! it.second.delta.data.data ) { \
			fprintf( _OUT_, "[N/A]\n" ); \
		} else { \
			for ( int i = 0, len = it.second.delta.data.size ; i < len; i++ ) { \
				fprintf( _OUT_, "%hhx", it.second.delta.data.data[ i ] ); \
			}; \
			fprintf( _OUT_, "]\n" ); \
		} \
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
