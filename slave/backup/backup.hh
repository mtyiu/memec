#ifndef __SLAVE_BACKUP_SLAVE_BACKUP_HH__
#define __SLAVE_BACKUP_SLAVE_BACKUP_HH__

#include <cstdio>
#include <cstddef>
#include <map>
#include "../ds/pending_data.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/value.hh"
#include "../../common/ds/sockaddr_in.hh"
#include "../../common/lock/lock.hh"
#include "../../common/timestamp/timestamp.hh"

class SlaveBackup {
private:
	Timestamp *localTime;

	// backup key-value in the pending structure
	std::multimap<Timestamp, PendingData> dataUpdate;
	std::multimap<Timestamp, PendingData> parityUpdate;
	std::multimap<Timestamp, PendingData> dataDelete;
	std::multimap<Timestamp, PendingData> parityDelete;
	LOCK_T dataUpdateLock;
	LOCK_T parityUpdateLock;
	LOCK_T dataDeleteLock;
	LOCK_T parityDeleteLock;

public:
	
	SlaveBackup();
	~SlaveBackup();

	// backup key-value for update and delete
	bool insertDataUpdate( Timestamp ts, Key key, Value value, uint32_t listId, uint32_t chunkId );
	bool insertParityUpdate( Timestamp ts, Key key, Value value, uint32_t listId, uint32_t chunkId );
	bool insertDataDelete( Timestamp ts, Key key, Value value, uint32_t listId, uint32_t chunkId );
	bool insertParityDelete( Timestamp ts, Key key, Value value, uint32_t listId, uint32_t chunkId );

	// clear key-value back for update and delete at and before a timestamp (inclusive)
	uint32_t removeDataUpdate( Timestamp ts );
	uint32_t removeParityUpdate( Timestamp ts );
	uint32_t removeDataDelete( Timestamp ts );
	uint32_t removeParityDelete( Timestamp ts );

	// TODO : undo parity update on specific key 
	
	// TODO : undo parity delete on specific key (key-recompaction??)
	
	
	void print( FILE *f );
};

#endif
