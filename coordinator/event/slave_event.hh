#ifndef __COORDINATOR_EVENT_SLAVE_EVENT_HH__
#define __COORDINATOR_EVENT_SLAVE_EVENT_HH__

#include "../socket/slave_socket.hh"
#include "../../common/ds/packet_pool.hh"
#include "../../common/event/event.hh"

enum SlaveEventType {
	SLAVE_EVENT_TYPE_UNDEFINED,
	SLAVE_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS,
	SLAVE_EVENT_TYPE_REGISTER_RESPONSE_FAILURE,
	SLAVE_EVENT_TYPE_ANNOUNCE_SLAVE_CONNECTED,
	SLAVE_EVENT_TYPE_ANNOUNCE_SLAVE_RECONSTRUCTED,
	SLAVE_EVENT_TYPE_PENDING,
	SLAVE_EVENT_TYPE_REQUEST_SEAL_CHUNKS,
	SLAVE_EVENT_TYPE_REQUEST_FLUSH_CHUNKS,
	SLAVE_EVENT_TYPE_REQUEST_SYNC_META,
	SLAVE_EVENT_TYPE_PARITY_MIGRATE,
	SLAVE_EVENT_TYPE_REQUEST_RELEASE_DEGRADED_LOCK,
	SLAVE_EVENT_TYPE_RESPONSE_HEARTBEAT,
	SLAVE_EVENT_TYPE_DISCONNECT
};

class SlaveEvent : public Event {
public:
	SlaveEventType type;
	uint16_t instanceId;
	uint32_t requestId;
	SlaveSocket *socket;
	union {
		struct {
			SlaveSocket *src;
			SlaveSocket *dst;
		} reconstructed;
		struct {
			pthread_mutex_t *lock;
			pthread_cond_t *cond;
			bool *done;
		} degraded;
		struct {
			Packet *packet;
		} parity;
		bool *sync;
		struct {
			uint32_t timestamp;
			uint32_t sealed;
			uint32_t keys;
			bool isLast;
		} heartbeat;
	} message;

	void pending( SlaveSocket *socket );
	void resRegister( SlaveSocket *socket, uint16_t instanceId, uint32_t requestId, bool success = true );
	void announceSlaveConnected( SlaveSocket *socket );
	void announceSlaveReconstructed( SlaveSocket *srcSocket, SlaveSocket *dstSocket );
	void reqSealChunks( SlaveSocket *socket );
	void reqFlushChunks( SlaveSocket *socket );
	void reqSyncMeta( SlaveSocket *socket, bool *sync );
	void reqReleaseDegradedLock( SlaveSocket *socket, pthread_mutex_t *lock, pthread_cond_t *cond, bool *done );
	void syncRemappedData( SlaveSocket *socket, Packet *packet );
	void resHeartbeat( SlaveSocket *socket, uint32_t timestamp, uint32_t sealed, uint32_t keys, bool isLast );
	void disconnect( SlaveSocket *socket );
};

#endif
