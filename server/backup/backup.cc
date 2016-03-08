#include "backup.hh"
#include "../main/server.hh"

ServerBackup::ServerBackup() {
	Server* server = Server::getInstance();

	this->localTime = &server->timestamp;

	LOCK_INIT( &this->dataUpdateLock );
	LOCK_INIT( &this->parityUpdateLock );
	// LOCK_INIT( &this->idToTimestampMapLock );
	LOCK_INIT( &this->dataDeleteLock );
	LOCK_INIT( &this->parityDeleteLock );
}

ServerBackup::~ServerBackup() {
}

// ------------------------------- INSERT ----------------------------------

bool ServerBackup::addPendingAck( BackupPendingIdentifier pi, Timestamp ts, bool &isDuplicated, const char* type ) {
	// LOCK( &this->idToTimestampMapLock );

	std::multimap<BackupPendingIdentifier, Timestamp>::iterator lit, rit;
	tie( lit, rit ) = this->idToTimestampMap.equal_range( pi );
	int pending = 0;
	for( ; lit != rit; lit++ ) {
		if ( lit->first.targetSocket == pi.targetSocket ) {
			__ERROR__( "ServerBackup", "addPendingAck", "Duplicated request Id [%d] for %s!", pi.requestId, type? type : "[N/A]" );
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
bool ServerBackup::insertData##_OP_TYPE_( Timestamp ts, Key key, Value value, Metadata metadata, bool isChunkDelta, uint32_t valueOffset, uint32_t chunkOffset, uint32_t requestId, uint16_t targetId, Socket *targetSocket ) { \
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
		backupDelta.set( metadata, key, value, isChunkDelta, valueOffset, chunkOffset, false, requestId, 0, ts.getVal() ); \
		backupDelta.parityServers.insert( targetId ); \
		map->insert( std::pair<Timestamp, BackupDelta>( ts, backupDelta ) ); \
	} else if ( ret ) { \
		/* maintain a list of parity servers for data delta */ \
		std::multimap<Timestamp, BackupDelta>::iterator lit, rit; \
		tie( lit, rit ) = map->equal_range( ts ); \
		\
		for ( ; lit != rit; lit++ ) { \
			if ( lit->second.requestId == requestId ) { \
				lit->second.parityServers.insert( targetId ); \
			} \
		} \
	} \
	UNLOCK( lock ); \
	return ret; \
}

#define DEFINE_INSERT_PARITY_METHOD( _OP_TYPE_ ) \
	bool ServerBackup::insertParity##_OP_TYPE_( Timestamp ts, Key key, Value value, Metadata metadata, bool isChunkDelta, uint32_t valueOffset, uint32_t chunkOffset, uint16_t dataServerId, uint32_t requestId ) { \
		LOCK_T *lock = &this->parity##_OP_TYPE_##Lock; \
		std::multimap<Timestamp, BackupDelta> *map = &this->parity##_OP_TYPE_; \
		BackupDelta backupDelta; \
		\
		LOCK( lock ); \
		backupDelta.set( metadata, key, value, isChunkDelta, valueOffset, chunkOffset, true, requestId, dataServerId, ts.getVal() ); \
		map->insert( std::pair<Timestamp, BackupDelta>( ts, backupDelta ) ); \
		UNLOCK( lock ); \
		return true; \
	}

// ------------------------------- REMOVE ----------------------------------

#define REMOVE_ITEMS( _MAP_, _LIT_, _RIT_, _FREE_, _RET_, _IS_DATA_, _SERVER_ID_ ) \
	saveIt = _LIT_; \
	for ( ; _LIT_ != _RIT_; _LIT_ = saveIt ) { \
		saveIt++; \
		/*
		 * skip if target server id is specified, and
		 *
		 * (1) (a) isParity && data source != target server id, and
		 *     (b) a set of timestamps is specified but the backup timestamp is not in the set;
		 *
		 * OR
		 *
		 * (2) isData && target server id not in  parity servers (no need to check timstamps,
		 *     since yet removed > yet all parity acked > must revert;
		 *     TODO what if client send revert to parity before ack from data server reach client??
		 */ \
		if ( \
			_SERVER_ID_ != 0 && \
			( \
				( ( ! _IS_DATA_ && _LIT_->second.dataServerId != _SERVER_ID_ ) && \
			  	  ( ! timestamps.empty() && timestamps.count( _LIT_->first.getVal() ) == 0 ) \
				) || \
				( _IS_DATA_ && _LIT_->second.parityServers.count( _SERVER_ID_ ) == 0 ) \
			) \
		) { \
			continue; \
		} \
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

#define DEFINE_REMOVE_BY_TIMESTAMP_METHOD1( _CONTENT_TYPE_, _VAR_TYPE_, _OP_TYPE_ ) \
	std::vector<BackupDelta> ServerBackup::remove##_CONTENT_TYPE_##_OP_TYPE_( Timestamp from, Timestamp to, uint16_t dataServerId, bool free ) { \
		std::vector<BackupDelta> ret; \
		LOCK_T *lock = &this->_VAR_TYPE_##_OP_TYPE_##Lock; \
		std::multimap<Timestamp, BackupDelta> *map = &this->_VAR_TYPE_##_OP_TYPE_; \
		\
		std::multimap<Timestamp, BackupDelta>::iterator lit, rit, it, saveIt; \
		bool isData = ( strncmp( #_CONTENT_TYPE_ , "Data", 4 ) == 0 ); \
		std::set<uint32_t> timestamps; \
		\
		LOCK( lock );  \
		bool wrapped = ( from.getVal() > to.getVal() ); \
		lit = map->lower_bound( from ); \
		rit = map->upper_bound( to ); \
		if ( wrapped ) { \
			/* from --> end of map */ \
			it = lit; \
			REMOVE_ITEMS( map, it, map->end(), free, ret, isData, dataServerId ); \
			/* start of map --> to */ \
			rit = map->upper_bound( to ); \
			it = map->begin(); \
			REMOVE_ITEMS( map, it, rit, free, ret, isData, dataServerId ); \
		} else { \
			it = lit; \
			REMOVE_ITEMS( map, it, rit, free, ret, isData, dataServerId ); \
		} \
		UNLOCK( lock ); \
		return ret; \
	}

#define DEFINE_REMOVE_BY_TIMESTAMP_METHOD2( _CONTENT_TYPE_, _VAR_TYPE_, _OP_TYPE_ ) \
	std::vector<BackupDelta> ServerBackup::remove##_CONTENT_TYPE_##_OP_TYPE_( std::set<uint32_t> timestamps, uint16_t dataServerId, bool free ) { \
		std::vector<BackupDelta> ret; \
		LOCK_T *lock = &this->_VAR_TYPE_##_OP_TYPE_##Lock; \
		std::multimap<Timestamp, BackupDelta> *map = &this->_VAR_TYPE_##_OP_TYPE_; \
		\
		std::multimap<Timestamp, BackupDelta>::iterator lit, rit, it, saveIt; \
		bool isData = ( strncmp( #_CONTENT_TYPE_ , "Data", 4 ) == 0 ); \
		\
		LOCK( lock );  \
		uint32_t from = *timestamps.begin(), to = *timestamps.end(); \
		bool wrapped = ( from > to ); \
		lit = map->lower_bound( Timestamp( from ) ); \
		rit = map->upper_bound( Timestamp( to ) ); \
		if ( wrapped ) { \
			/* from --> end of map */ \
			it = lit; \
			REMOVE_ITEMS( map, it, map->end(), free, ret, isData, dataServerId ); \
			/* start of map --> to */ \
			rit = map->upper_bound( to ); \
			it = map->begin(); \
			REMOVE_ITEMS( map, it, rit, free, ret, isData, dataServerId ); \
		} else { \
			it = lit; \
			REMOVE_ITEMS( map, it, rit, free, ret, isData, dataServerId ); \
		} \
		UNLOCK( lock ); \
		return ret; \
	}

#define DEFINE_REMOVE_DATA_METHOD( _OP_TYPE_ ) \
	DEFINE_REMOVE_BY_TIMESTAMP_METHOD2( Data, data, _OP_TYPE_ )

#define DEFINE_REMOVE_PARITY_BY_TIMESTAMPS_METHOD( _OP_TYPE_ ) \
	DEFINE_REMOVE_BY_TIMESTAMP_METHOD2( Parity, parity, _OP_TYPE_ )

#define DEFINE_REMOVE_PARITY_BY_TIMESTAMP_RANGE_METHOD( _OP_TYPE_ ) \
	DEFINE_REMOVE_BY_TIMESTAMP_METHOD1( Parity, parity, _OP_TYPE_ )

#define DEFINE_REMOVE_BY_ID_METHOD( _CONTENT_TYPE_, _VAR_TYPE_, _OP_TYPE_ ) \
	BackupDelta ServerBackup::remove##_CONTENT_TYPE_##_OP_TYPE_( uint32_t requestId, uint16_t targetId, Socket *targetSocket, bool free ) { \
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
			__ERROR__( "ServerBackup", "removeById", "Cannnot find pending parity server for request ID = %u from socket FD = %u.", requestId, targetSocket->getSocket() ); \
			UNLOCK( lock ); \
			return ret; \
		} \
		ts = tpit->second; \
		int pending = 0; \
		for ( ; lpit != rpit; lpit++ ) \
			pending++; \
		this->idToTimestampMap.erase( tpit ); \
		std::multimap<Timestamp, BackupDelta>::iterator lit, rit; \
		\
		tie( lit, rit ) = map->equal_range( ts ); \
		/* only remove backup after all parity server acked */ \
		for ( ; lit != rit; lit++ ) { \
			if ( lit->second.requestId == requestId ) { \
				break; \
			} \
		} \
		if ( lit == rit ) { \
			__ERROR__( "ServerBackup" , "removeById", "Cannot find backup for request ID = %u at time = %u.", requestId, ts.getVal() ); \
			return ret; \
		} \
		/* UNLOCK( &this->idToTimestampMapLock ); */ \
		/* if the only pending reference is removed, remove the backup as well */ \
		if ( pending == 1 ) { \
			ret = lit->second; \
			if ( free ) \
				lit->second.free(); \
			map->erase( lit ); \
		} else { \
			lit->second.parityServers.erase( targetId ); \
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
DEFINE_REMOVE_PARITY_BY_TIMESTAMPS_METHOD( Update )
DEFINE_REMOVE_PARITY_BY_TIMESTAMPS_METHOD( Delete )

DEFINE_REMOVE_DATA_BY_ID_METHOD( Update )
DEFINE_REMOVE_DATA_BY_ID_METHOD( Delete )

DEFINE_REMOVE_PARITY_BY_TIMESTAMP_RANGE_METHOD( Update )
DEFINE_REMOVE_PARITY_BY_TIMESTAMP_RANGE_METHOD( Delete )


// ------------------------------- FIND ----------------------------------

#define DEFINE_FIND_BY_TIMESTAMPS_METHOD( _CONTENT_TYPE_, _VAR_TYPE_, _OP_TYPE_ ) \
	std::vector<BackupDelta> ServerBackup::find##_CONTENT_TYPE_##_OP_TYPE_( uint32_t from, uint32_t to ) { \
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

void ServerBackup::print( FILE *f, bool printDelta ) {
	fprintf( f,
		"Data server backup: (Update) %lu (Delete) %lu\n"
		"Parity server backup: (Update) %lu (Delete) %lu\n"
		"Pending server backup: %lu\n"
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
			"%7u  Timestamp: %10u;  From: %5hu; key: (%4u) %.*s;  offset: %4u %4u;  isChunkDelta:%1hhu;  isParity:%1hhu (%3lu);  delta: (%4u) [", \
			i, \
			it.first.getVal(), \
			it.second.dataServerId, \
			it.second.key.size, \
			it.second.key.size, \
			( it.second.key.data )? it.second.key.data : "[N/A]", \
			it.second.delta.valueOffset, \
			it.second.delta.chunkOffset, \
			it.second.isChunkDelta, \
			it.second.isParity, \
			it.second.parityServers.size(), \
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

#undef DEFINE_INSERT_DATA_METHOD
#undef DEFINE_INSERT_PARITY_METHOD

#undef DEFINE_REMOVE_METHOD
#undef DEFINE_REMOVE_BY_TIMESTAMP_METHOD1
#undef DEFINE_REMOVE_BY_TIMESTAMP_METHOD2
#undef DEFINE_REMOVE_BY_ID_METHOD

#undef DEFINE_REMOVE_DATA_METHOD
#undef DEFINE_REMOVE_DATA_BY_ID_METHOD
#undef DEFINE_REMOVE_PARITY_BY_ID_METHOD
#undef DEFINE_REMOVE_PARITY_BY_TIMESTAMPS_METHOD
#undef DEFINE_REMOVE_PARITY_BY_TIMESTAMP_RANGE_METHOD

#undef DEFINE_FIND_BY_TIMESTAMPS_METHOD
#undef DEFINE_FIND_DATA_BY_TIMESTAMPS_METHOD
#undef DEFINE_FIND_PARITY_BY_TIMESTAMPS_METHOD
