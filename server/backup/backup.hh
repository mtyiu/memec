#ifndef __SLAVE_BACKUP_SLAVE_BACKUP_HH__
#define __SLAVE_BACKUP_SLAVE_BACKUP_HH__

#include <cstdio>
#include <cstddef>
#include <map>
#include <vector>
#include <set>
#include "../socket/server_socket.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/ds/value.hh"
#include "../../common/ds/sockaddr_in.hh"
#include "../../common/lock/lock.hh"
#include "../../common/socket/socket.hh"
#include "../../common/timestamp/timestamp.hh"

class BackupDelta {
public:
	Key key;
	struct {
		Value data;
		uint32_t valueOffset;
		uint32_t chunkOffset;
	} delta;
	Metadata metadata;
	bool isChunkDelta;

	bool isParity;
	std::set<uint16_t> paritySlaves;

	uint32_t requestId;
	uint16_t dataSlaveId;
	uint32_t timestamp;

	void set( Metadata metadata, Key key, Value value, bool isChunkDelta, uint32_t valueOffset, uint32_t chunkOffset, bool isParity, uint32_t requestId, uint16_t dataSlaveId, uint32_t timestamp ) {
		this->metadata = metadata;
		if ( value.size > 0 )
			this->delta.data.dup( value.size, value.data );
		else
			this->delta.data.set( 0, 0 );
		if ( key.size > 0 )
			this->key.dup( key.size, key.data );
		else
			this->key.set( 0, 0 );
		this->isChunkDelta = isChunkDelta;
		this->delta.valueOffset = valueOffset;
		this->delta.chunkOffset = chunkOffset;
		this->isParity = isParity;
		this->requestId = requestId;
		this->dataSlaveId = dataSlaveId;
		this->timestamp = timestamp;
	}

	void free() {
		if ( key.size )
			this->key.free();
		if ( delta.data.size )
			this->delta.data.free();
	}

	void print( FILE *f = stdout ) {
		fprintf( f,
			"Timestamp: %10u; From: %5hu; key: (%4u) %.*s;  offset: %4u %4u;  isChunkDelta:%1hhu;  isParity:%1hhu (%3lu);  delta: (%4u) [",
			this->timestamp,
			this->dataSlaveId,
			this->key.size,
			this->key.size,
			( this->key.data )? this->key.data : "[N/A]",
			this->delta.valueOffset,
			this->delta.chunkOffset,
			this->isChunkDelta,
			this->isParity,
			this->paritySlaves.size(),
			this->delta.data.size
		);
		if ( ! this->delta.data.data ) {
			fprintf( f, "[N/A]\n" );
		} else {
			for ( int i = 0, len = this->delta.data.size ; i < len; i++ ) {
				fprintf( f, "%hhx", this->delta.data.data[ i ] );
			};
			fprintf( f, "]\n" );
		}
	}
};

class BackupPendingIdentifier {
public:
	uint32_t requestId;
	Socket *targetSocket;

	BackupPendingIdentifier( uint32_t requestId, Socket *targetSocket ) {
		this->requestId = requestId;
		this->targetSocket = targetSocket;
	}

	bool operator<( const BackupPendingIdentifier &pi ) const {
		// lose comparison
		return ( this->requestId < pi.requestId );
	}

	bool operator==( const BackupPendingIdentifier &pi ) const {
		// strict comparison
		return ( this->requestId == pi.requestId && this->targetSocket == pi.targetSocket );
	}

};

namespace std {
	template<> struct hash<BackupPendingIdentifier> {
		size_t operator()( const BackupPendingIdentifier &pi ) const {
			return HashFunc::hash( ( char* ) &pi.requestId, sizeof( pi.requestId ) );
		}
	};
}

class SlaveBackup {
private:
	Timestamp *localTime;

	// backup key-value in the pending structure
	// timestamp -> delta
	std::multimap<Timestamp, BackupDelta> dataUpdate;
	std::multimap<Timestamp, BackupDelta> dataDelete;
	// ( requestId, serverSocket ) -> timestamp
	std::multimap<BackupPendingIdentifier, Timestamp> idToTimestampMap;
	// timestamp -> delta
	std::multimap<Timestamp, BackupDelta> parityUpdate;
	std::multimap<Timestamp, BackupDelta> parityDelete;
	// locks
	LOCK_T dataUpdateLock;
	LOCK_T dataDeleteLock;
	//LOCK_T idToTimestampMapLock;
	LOCK_T parityUpdateLock;
	LOCK_T parityDeleteLock;

	bool addPendingAck( BackupPendingIdentifier pi, Timestamp ts, bool &isDuplicated, const char* type );
public:

	SlaveBackup();
	~SlaveBackup();

	// backup key-value for update and delete
	bool insertDataUpdate( Timestamp ts, Key key, Value value, Metadata metadata, bool isChunkDelta, uint32_t valueOffset, uint32_t chunkOffset, uint32_t requestId, uint16_t targetId, Socket *targetSocket );
	bool insertDataDelete( Timestamp ts, Key key, Value value, Metadata metadata, bool isChunkDelta, uint32_t valueOffset, uint32_t chunkOffset, uint32_t requestId, uint16_t targetId, Socket *targetSocket );
	bool insertParityUpdate( Timestamp ts, Key key, Value value, Metadata metadata, bool isChunkDelta, uint32_t valueOffset, uint32_t chunkOffset, uint16_t dataSlaveId, uint32_t requestId );
	bool insertParityDelete( Timestamp ts, Key key, Value value, Metadata metadata, bool isChunkDelta, uint32_t valueOffset, uint32_t chunkOffset, uint16_t dataSlaveId, uint32_t requestId );

	// clear key-value backup for update and delete
	std::vector<BackupDelta> removeDataUpdate( std::set<uint32_t> timestamps, uint16_t dataSlaveId = 0, bool free = true ); // timestamps
	std::vector<BackupDelta> removeDataDelete( std::set<uint32_t> timestamps, uint16_t dataSlaveId = 0, bool free = true );
	std::vector<BackupDelta> removeParityUpdate( std::set<uint32_t> timestamps, uint16_t dataSlaveId = 0, bool free = true );
	std::vector<BackupDelta> removeParityDelete( std::set<uint32_t> timestamps, uint16_t dataSlaveId = 0, bool free = true );
	std::vector<BackupDelta> removeParityUpdate( Timestamp from, Timestamp to, uint16_t dataSlaveId = 0, bool free = true ); // range of timestamps
	std::vector<BackupDelta> removeParityDelete( Timestamp from, Timestamp to, uint16_t dataSlaveId = 0, bool free = true );
	// remove backup upon all parity acked using requestId
	BackupDelta removeDataUpdate( uint32_t requestId, uint16_t targetId, Socket *targetSocket, bool free = true ); // id and source data slave
	BackupDelta removeDataDelete( uint32_t requestId, uint16_t targetId, Socket *targetSocket, bool free = true );

	// find key-value backup for update and delete by a timestamp or request id
	std::vector<BackupDelta> findDataUpdate( uint32_t from, uint32_t to );
	std::vector<BackupDelta> findDataDelete( uint32_t from, uint32_t to );
	std::vector<BackupDelta> findParityUpdate( uint32_t from, uint32_t to );
	std::vector<BackupDelta> findParityDelete( uint32_t from, uint32_t to );

	void print( FILE *f = stdout, bool printDelta = false );
};

#endif
