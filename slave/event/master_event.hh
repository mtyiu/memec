#ifndef __SLAVE_EVENT_MASTER_EVENT_HH__
#define __SLAVE_EVENT_MASTER_EVENT_HH__

#include "../socket/master_socket.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/key_value.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/event/event.hh"

enum MasterEventType {
	MASTER_EVENT_TYPE_UNDEFINED,
	// Register
	MASTER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE,
	// GET
	MASTER_EVENT_TYPE_GET_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_GET_RESPONSE_FAILURE,
	// SET
	MASTER_EVENT_TYPE_SET_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_SET_RESPONSE_FAILURE,
	// UPDATE
	MASTER_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_UPDATE_RESPONSE_FAILURE,
	// UPDATE_CHUNK
	MASTER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_FAILURE,
	// DELETE
	MASTER_EVENT_TYPE_DELETE_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_DELETE_RESPONSE_FAILURE,
	// DELETE_CHUNK (offset)
	MASTER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_FAILURE,
	// Pending
	MASTER_EVENT_TYPE_PENDING
};

class MasterEvent : public Event {
public:
	MasterEventType type;
	MasterSocket *socket;
	union {
		Key key;
		KeyValue keyValue;
		struct {
			Key key;
			Metadata metadata;
			uint32_t offset;
			uint32_t length;
			char *delta;
		} keyValueUpdate;
		struct {
			Metadata metadata;
			uint32_t offset;
			uint32_t length;
		} chunkUpdate;
	} message;

	// Register
	void resRegister( MasterSocket *socket, bool success = true );
	// GET
	void resGet( MasterSocket *socket, KeyValue &keyValue );
	void resGet( MasterSocket *socket, Key &key );
	// SET
	void resSet( MasterSocket *socket, Key &key );
	// UPDATE
	void resUpdate( MasterSocket *socket, Key &key, Metadata &metadata, uint32_t offset, uint32_t length, char *delta );
	void resUpdate( MasterSocket *socket, Key &key );
	// UPDATE_CHUNK
	void resUpdateChunk( MasterSocket *socket, Metadata &metadata, uint32_t offset, uint32_t length, bool success = true );
	// DELETE
	void resDelete( MasterSocket *socket, Key &key, Metadata &metadata, uint32_t offset, uint32_t length, char *delta );
	void resDelete( MasterSocket *socket, Key &key );
	// DELETE_CHUNK
	void resDeleteChunk( MasterSocket *socket, Metadata &metadata, uint32_t offset, uint32_t length, bool success = true );

	void pending( MasterSocket *socket );
};

#endif
