#include "backup.hh"
#include "../main/slave.hh"

SlaveBackup::SlaveBackup() {
	Slave* slave = Slave::getInstance();

	this->localTime = &slave->timestamp;

	LOCK_INIT( &this->dataUpdateLock );
	LOCK_INIT( &this->parityUpdateLock );
	// LOCK_INIT( &this->idToTimestampMapLock );
	LOCK_INIT( &this->dataDeleteLock );
	LOCK_INIT( &this->parityDeleteLock );
}

SlaveBackup::~SlaveBackup() {
}

// ------------------------------- INSERT ----------------------------------

bool SlaveBackup::addPendingAck( BackupPendingIdentifier pi, Timestamp ts, bool &isDuplicated, const char* type ) {
	// LOCK( &this->idToTimestampMapLock );

	std::multimap<BackupPendingIdentifier, Timestamp>::iterator lit, rit;
	tie( lit, rit ) = this->idToTimestampMap.equal_range( pi );
	int pending = 0;
	for( ; lit != rit; lit++ ) {
		if ( lit->first.targetSocket == pi.targetSocket ) {
			__ERROR__( "SlaveBackup", "addPendingAck", "Duplicated request Id [%d] for %s!", pi.requestId, type? type : "[N/A]" );
			return false;
		}
		pending++;
	}
	isDuplicated = pending > 0;

	this->idToTimestampMap.insert( std::pair<BackupPendingIdentifier, Timestamp>( pi, ts ) );

	// UNLOCK( &this->idToTimestampMapLock );

	return true;
}

#define DEFINE_INSERT_DATA_METHOD( _OP_TYPE_ ) \
bool SlaveBackup::insertData##_OP_TYPE_( Timestamp ts, Key key, Value value, Metadata metadata, bool isChunkDelta, uint32_t valueOffset, uint32_t chunkOffset, uint32_t requestId, Socket *targetSocket ) { \
	LOCK_T *lock = &this->data##_OP_TYPE_##Lock; \
	std::multimap<Timestamp, BackupDelta> *map = &this->data##_OP_TYPE_; \
	BackupDelta backupDelta; \
	BackupPendingIdentifier pi( requestId, targetSocket ); \
	bool ret = true, duplicate = false; \
	\
	LOCK( lock ); \
	ret = this->addPendingAck( pi, ts, duplicate, #_OP_TYPE_ ); \
	/* keep one copy of backup, but mutltiple ref indexed by a request id */ \
	if ( ! duplicate && ret ) { \
		backupDelta.set( metadata, key, value, isChunkDelta, valueOffset, chunkOffset, false, requestId, 0 ); \
		map->insert( std::pair<Timestamp, BackupDelta>( ts, backupDelta ) ); \
	} \
	UNLOCK( lock ); \
	return ret; \
}

#define DEFINE_INSERT_PARITY_METHOD( _OP_TYPE_ ) \
	bool SlaveBackup::insertParity##_OP_TYPE_( Timestamp ts, Key key, Value value, Metadata metadata, bool isChunkDelta, uint32_t valueOffset, uint32_t chunkOffset, uint16_t dataSlaveId, uint32_t requestId ) { \
		LOCK_T *lock = &this->parity##_OP_TYPE_##Lock; \
		std::multimap<Timestamp, BackupDelta> *map = &this->parity##_OP_TYPE_; \
		BackupDelta backupDelta; \
		\
		LOCK( lock ); \
		backupDelta.set( metadata, key, value, isChunkDelta, valueOffset, chunkOffset, true, requestId, dataSlaveId ); \
		map->insert( std::pair<Timestamp, BackupDelta>( ts, backupDelta ) ); \
		UNLOCK( lock ); \
		return true; \
	}

// ------------------------------- REMOVE ----------------------------------

#define REMOVE_ITEMS( _MAP_, _LIT_, _RIT_, _FREE_, _RET_, _IS_DATA_, _SLAVE_ID_ ) \
	saveIt = _LIT_; \
	for ( ; _LIT_ != _RIT_; _LIT_ = saveIt ) { \
		saveIt++; \
		if ( _SLAVE_ID_ != 0 && _LIT_->second.dataSlaveId != _SLAVE_ID_ ) \
			continue; \
		if ( _FREE_ ) \
			_LIT_->second.free(); \
		else \
			_RET_.push_back( _LIT_->second ); \
		if ( _IS_DATA_ ) { \
			BackupPendingIdentifier pi ( _LIT_->second.requestId, 0 ) ; \
			/* LOCK( &this->idToTimestampMapLock ); */ \
			this->idToTimestampMap.erase( pi ); \
			/* UNLOCK( &this->idToTimestampMapLock ); */ \
		} \
		_MAP_->erase( _LIT_ ); \
	}

#define DEFINE_REMOVE_BY_TIMESTAMP_METHOD( _CONTENT_TYPE_, _VAR_TYPE_, _OP_TYPE_ ) \
	std::vector<BackupDelta> SlaveBackup::remove##_CONTENT_TYPE_##_OP_TYPE_( Timestamp from, Timestamp to, uint16_t dataSlaveId, bool free ) { \
		std::vector<BackupDelta> ret; \
		LOCK_T *lock = &this->_VAR_TYPE_##_OP_TYPE_##Lock; \
		std::multimap<Timestamp, BackupDelta> *map = &this->_VAR_TYPE_##_OP_TYPE_; \
		\
		std::multimap<Timestamp, BackupDelta>::iterator lit, rit, it, saveIt; \
		bool isData = ( strncmp( #_CONTENT_TYPE_ , "Data", 4 ) == 0 ); \
		\
		LOCK( lock );  \
		bool wrapped = ( from.getVal() > to.getVal() ); \
		lit = map->lower_bound( from ); \
		rit = map->upper_bound( to ); \
		if ( wrapped ) { \
			/* from --> end of map */ \
			it = lit; \
			REMOVE_ITEMS( map, it, map->end(), free, ret, isData, dataSlaveId ); \
			/* start of map --> to */ \
			rit = map->upper_bound( to ); \
			it = map->begin(); \
			REMOVE_ITEMS( map, it, rit, free, ret, isData, dataSlaveId ); \
		} else { \
			it = lit; \
			REMOVE_ITEMS( map, it, rit, free, ret, isData, dataSlaveId ); \
		} \
		UNLOCK( lock ); \
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
		/* LOCK( &this->idToTimestampMapLock ); */ \
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
		/* UNLOCK( &this->idToTimestampMapLock ); */ \
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
DEFINE_INSERT_DATA_METHOD( Delete )
DEFINE_INSERT_PARITY_METHOD( Update )
DEFINE_INSERT_PARITY_METHOD( Delete )

DEFINE_REMOVE_DATA_METHOD( Update )
DEFINE_REMOVE_DATA_METHOD( Delete )
DEFINE_REMOVE_PARITY_METHOD( Update )
DEFINE_REMOVE_PARITY_METHOD( Delete )

DEFINE_REMOVE_DATA_BY_ID_METHOD( Update );
DEFINE_REMOVE_DATA_BY_ID_METHOD( Delete );

// ------------------------------- FIND ----------------------------------


#define DEFINE_FIND_BY_TIMESTAMPS_METHOD( _CONTENT_TYPE_, _VAR_TYPE_, _OP_TYPE_ ) \
	std::vector<BackupDelta> SlaveBackup::find##_CONTENT_TYPE_##_OP_TYPE_( uint32_t from, uint32_t to ) { \
		std::vector<BackupDelta> deltaVector; \
		\
		std::multimap<Timestamp, BackupDelta> *map = &this->_VAR_TYPE_##_OP_TYPE_; \
		LOCK_T *lock = &this->_VAR_TYPE_##_OP_TYPE_##Lock; \
		bool wrapped = from > to; \
		\
		LOCK( lock ); \
		if ( wrapped ) { \
			/* from -->  MAX timestamp */ \
			for ( auto it = map->lower_bound( from ); it != map->end(); it++ ) \
				deltaVector.push_back( it->second ); \
 \
			/* MIN timestamp --> to */ \
			auto end = map->upper_bound( to ); \
			for ( auto it = map->begin(); it != map->end() && it != end; it++ ) \
				deltaVector.push_back( it->second ); \
		} else { \
			auto lit = map->lower_bound( from ); \
			auto rit = map->upper_bound( to ); \
			for ( ; lit != map->end() && lit != rit ; lit ++ ) \
				deltaVector.push_back( lit->second ); \
		} \
		UNLOCK( lock ); \
		return deltaVector; \
		\
	} \

#define DEFINE_FIND_DATA_BY_TIMESTAMPS_METHOD( _OP_TYPE_ ) \
	DEFINE_FIND_BY_TIMESTAMPS_METHOD( Data, data, _OP_TYPE_ )

#define DEFINE_FIND_PARITY_BY_TIMESTAMPS_METHOD( _OP_TYPE_ ) \
	DEFINE_FIND_BY_TIMESTAMPS_METHOD( Parity, parity, _OP_TYPE_ )

DEFINE_FIND_DATA_BY_TIMESTAMPS_METHOD( Update );
DEFINE_FIND_DATA_BY_TIMESTAMPS_METHOD( Delete );
DEFINE_FIND_PARITY_BY_TIMESTAMPS_METHOD( Update );
DEFINE_FIND_PARITY_BY_TIMESTAMPS_METHOD( Delete );

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
			"%7u  Timestamp: %10u;  From: %5hu; key: (%4u) %.*s;  offset: %4u %4u;  isChunkDelta:%1hhu;  isParity:%1hhu;  delta: (%4u) [", \
			i, \
			it.first.getVal(), \
			it.second.dataSlaveId, \
			it.second.key.size, \
			it.second.key.size, \
			( it.second.key.data )? it.second.key.data : "[N/A]", \
			it.second.delta.valueOffset, \
			it.second.delta.chunkOffset, \
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

#undef REMOVE_ITEMS

#undef DEFINE_INSERT_METHOD
#undef DEFINE_INSERT_DATA_METHOD
#undef DEFINE_INSERT_PARITY_METHOD
#undef DEFINE_REMOVE_METHOD
#undef DEFINE_REMOVE_DATA_BY_ID_METHOD
#undef DEFINE_REMOVE_PARITY_BY_ID_METHOD
#undef DEFINE_REMOVE_DATA_BY_ID_METHOD
#undef DEFINE_FIND_BY_TIMESTAMPS_METHOD
#undef DEFINE_FIND_DATA_BY_TIMESTAMPS_METHOD
#undef DEFINE_FIND_PARITY_BY_TIMESTAMPS_METHOD
