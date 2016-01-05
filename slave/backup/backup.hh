#ifndef __SLAVE_BACKUP_SLAVE_BACKUP_HH__
#define __SLAVE_BACKUP_SLAVE_BACKUP_HH__

#include <cstdio>
#include <cstddef>
#include <map>
#include <vector>
#include "../socket/slave_socket.hh"
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
		uint32_t offset;
	} delta;
	Metadata metadata;
	bool isChunkDelta;
	uint32_t requestId;
	bool isParity;
	uint16_t dataSlaveId;

	void set( Metadata metadata, Key key, Value value, bool isChunkDelta, uint32_t offset, bool isParity, uint32_t requestId, uint16_t dataSlaveId ) {
		this->metadata = metadata;
		this->delta.data.dup( value.size, value.data );
		this->key.dup( key.size, key.data );
		this->isChunkDelta = isChunkDelta;
		this->delta.offset = offset;
		this->isParity = isParity;
		this->requestId = requestId;
		this->dataSlaveId = dataSlaveId;
	}

	void free() {
		this->key.free();
		this->delta.data.free();
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
	// ( requestId, slaveSocket ) -> timestamp
	std::multimap<BackupPendingIdentifier, Timestamp> idToTimestampMap;
	// timestamp -> delta
	std::multimap<Timestamp, BackupDelta> parityUpdate;
	std::multimap<Timestamp, BackupDelta> parityDelete;
	// locks
	LOCK_T dataUpdateLock;
	LOCK_T dataDeleteLock;
	LOCK_T idToTimestampMapLock;
	LOCK_T parityUpdateLock;
	LOCK_T parityDeleteLock;

	bool addPendingAck( BackupPendingIdentifier pi, Timestamp ts, bool &isDuplicated, const char* type );
public:

	SlaveBackup();
	~SlaveBackup();

	// backup key-value for update and delete
	bool insertDataUpdate( Timestamp ts, Key key, Value value, Metadata metadata, bool isChunkDelta, uint32_t offset, uint32_t requestId, Socket *targetSocket );
	bool insertDataDelete( Timestamp ts, Key key, Value value, Metadata metadata, bool isChunkDelta, uint32_t offset, uint32_t requestId, Socket *targetSocket );
	bool insertParityUpdate( Timestamp ts, Key key, Value value, Metadata metadata, bool isChunkDelta, uint32_t offset, uint16_t dataSlaveId, uint32_t requestId );
	bool insertParityDelete( Timestamp ts, Key key, Value value, Metadata metadata, bool isChunkDelta, uint32_t offset, uint16_t dataSlaveId, uint32_t requestId );

	// clear key-value backup for update and delete at and before a timestamp (inclusive)
	// remove backup at and before a timestamp
	std::vector<BackupDelta> removeDataUpdate( Timestamp from, Timestamp ts, uint16_t dataSlaveId = 0, bool free = true );
	std::vector<BackupDelta> removeDataDelete( Timestamp from, Timestamp ts, uint16_t dataSlaveId = 0, bool free = true );
	std::vector<BackupDelta> removeParityUpdate( Timestamp from, Timestamp to, uint16_t dataSlaveId = 0, bool free = true );
	std::vector<BackupDelta> removeParityDelete( Timestamp from, Timestamp to, uint16_t dataSlaveId = 0, bool free = true );
	// remove backup upon all parity acked using requestId
	BackupDelta removeDataUpdate( uint32_t requestId, Socket *targetSocket, bool free = true );
	BackupDelta removeDataDelete( uint32_t requestId, Socket *targetSocket, bool free = true );

	// find key-value backup for update and delete by a timestamp or request id
	std::vector<BackupDelta> findDataUpdate( uint32_t from, uint32_t to );
	std::vector<BackupDelta> findDataDelete( uint32_t from, uint32_t to );
	std::vector<BackupDelta> findParityUpdate( uint32_t from, uint32_t to );
	std::vector<BackupDelta> findParityDelete( uint32_t from, uint32_t to );

	// TODO : undo parity update on specific key

	// TODO : undo parity delete on specific key (key-recompaction??)


	void print( FILE *f = stdout, bool printDelta = false );
};

#endif
