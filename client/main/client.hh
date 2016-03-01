#ifndef __MASTER_MAIN_MASTER_HH__
#define __MASTER_MAIN_MASTER_HH__

#include <map>
#include <set>
#include <cstdio>
#include "../config/client_config.hh"
#include "../ds/pending.hh"
#include "../ds/stats.hh"
#include "../event/event_queue.hh"
#include "../remap/remap_msg_handler.hh"
#include "../socket/application_socket.hh"
#include "../socket/coordinator_socket.hh"
#include "../socket/client_socket.hh"
#include "../socket/server_socket.hh"
#include "../worker/worker.hh"
#include "../../common/config/global_config.hh"
#include "../../common/ds/array_map.hh"
#include "../../common/ds/id_generator.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/key_value.hh"
#include "../../common/ds/latency.hh"
#include "../../common/ds/packet_pool.hh"
#include "../../common/ds/sockaddr_in.hh"
#include "../../common/stripe_list/stripe_list.hh"
#include "../../common/socket/epoll.hh"
#include "../../common/signal/signal.hh"
#include "../../common/util/option.hh"
#include "../../common/util/time.hh"

// Implement the singleton pattern
class Master {
private:
	bool isRunning;
	struct timespec startTime;
	std::vector<MasterWorker> workers;

	Master();
	// Do not implement
	Master( Master const& );
	void operator=( Master const& );

	// helper function to update slave stats
	void updateSlavesCurrentLoading();
	void updateSlavesCumulativeLoading();

	void free();
	// Commands
	void help();
	// void time();

public:
	struct {
		GlobalConfig global;
		MasterConfig master;
	} config;
	struct {
		MasterSocket self;
		EPoll epoll;
		ArrayMap<int, ApplicationSocket> applications;
		ArrayMap<int, CoordinatorSocket> coordinators;
		ArrayMap<int, SlaveSocket> slaves;
		std::unordered_map<uint16_t, SlaveSocket*> slavesIdToSocketMap;
		LOCK_T slavesIdToSocketLock;
	} sockets;
	IDGenerator idGenerator;
	Pending pending;
	MasterEventQueue eventQueue;
	PacketPool packetPool;
	StripeList<SlaveSocket> *stripeList;
	/* Remapping */
	MasterRemapMsgHandler remapMsgHandler;
	/* Loading statistics */
	SlaveLoading slaveLoading;
	OverloadedSlave overloadedSlave;
	Timer statsTimer;
	/* Instance ID (assigned by coordinator) */
	static uint16_t instanceId;
	/* For debugging only */
	struct {
		bool isDegraded;
	} debugFlags;
	Timestamp timestamp;

	static Master *getInstance() {
		static Master master;
		return &master;
	}

	static void signalHandler( int signal );

	bool init( char *path, OptionList &options, bool verbose );
	bool start();
	bool stop();
	void info( FILE *f = stdout );
	void debug( FILE *f = stdout );
	void printInstanceId( FILE *f = stdout );
	void printPending( FILE *f = stdout );
	void printRemapping( FILE *f = stdout );
	void printBackup( FILE *f = stdout );
	void syncMetadata();
	void time();
	void ackParityDelta( FILE *f = 0, SlaveSocket *target = 0, pthread_cond_t *condition = 0, LOCK_T *lock = 0, uint32_t *counter = 0, bool force = false );
	bool revertDelta( FILE *f = 0, SlaveSocket *target = 0, pthread_cond_t *condition = 0, LOCK_T *lock = 0, uint32_t *counter = 0, bool force = false );
	double getElapsedTime();
	void interactive();
	bool setDebugFlag( char *input );

	// Helper function for detecting whether degraded mode is enabled
	bool isDegraded( SlaveSocket *socket );

	// Helper function to update slave stats
	void mergeSlaveCumulativeLoading(
		ArrayMap<struct sockaddr_in, Latency> *getLatency,
		ArrayMap<struct sockaddr_in, Latency> *setLatency
	);
};

#endif