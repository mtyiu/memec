#ifndef __COORDINATOR_MAIN_COORDINATOR_HH__
#define __COORDINATOR_MAIN_COORDINATOR_HH__

#include <cstdio>
#include <pthread.h>
#include <set>
#include <unordered_map>
#include "../config/coordinator_config.hh"
#include "../ds/log.hh"
#include "../ds/pending.hh"
#include "../ds/remapping_record_map.hh"
#include "../event/event_queue.hh"
#include "../remap/remap_msg_handler.hh"
#include "../socket/coordinator_socket.hh"
#include "../socket/master_socket.hh"
#include "../socket/slave_socket.hh"
#include "../worker/worker.hh"
#include "../../common/config/global_config.hh"
#include "../../common/ds/array_map.hh"
#include "../../common/ds/id_generator.hh"
#include "../../common/lock/lock.hh"
#include "../../common/signal/signal.hh"
#include "../../common/socket/epoll.hh"
#include "../../common/stripe_list/stripe_list.hh"
#include "../../common/util/option.hh"
#include "../../common/util/time.hh"

// Implement the singleton pattern
class Coordinator {
private:
	bool isRunning;
	struct timespec startTime;
	std::vector<CoordinatorWorker> workers;

	Coordinator();
	// Do not implement
	Coordinator( Coordinator const& );
	void operator=( Coordinator const& );

	void free();

	// Helper functions to determine slave loading
	void updateAverageSlaveLoading( ArrayMap<struct sockaddr_in, Latency> *slaveGetLatency,
			ArrayMap<struct sockaddr_in, Latency> *slaveSetLatency );
	// return previously overloaded slaves for per-slave phase change
	std::set<struct sockaddr_in> updateOverloadedSlaveSet(
			ArrayMap<struct sockaddr_in, Latency> *slaveGetLatency,
			ArrayMap<struct sockaddr_in, Latency> *slaveSetLatency,
			std::set<struct sockaddr_in> *slaveSet
	);
	void switchPhase( std::set<struct sockaddr_in> prevOverloadedSlaves );

	// Commands
	void help();

public:
	struct {
		GlobalConfig global;
		CoordinatorConfig coordinator;
	} config;
	struct {
		CoordinatorSocket self;
		EPoll epoll;
		ArrayMap<int, MasterSocket> masters;
		ArrayMap<int, SlaveSocket> slaves;
		ArrayMap<int, SlaveSocket> backupSlaves;
	} sockets;
	IDGenerator idGenerator;
	CoordinatorEventQueue eventQueue;
	/* Stripe list */
	StripeList<SlaveSocket> *stripeList;
	/* Remapping */
	CoordinatorRemapMsgHandler *remapMsgHandler;
	RemappingRecordMap remappingRecords;
	struct {
		std::unordered_map<Key, RemappingRecord> toSend;
		LOCK_T toSendLock;
	} pendingRemappingRecords;
	PacketPool packetPool;
	/* Loading statistics */
	struct {
		// ( slaveAddr, ( mastserAddr, Latency ) )
		ArrayMap< struct sockaddr_in, ArrayMap< struct sockaddr_in, Latency > > latestGet;
		ArrayMap< struct sockaddr_in, ArrayMap< struct sockaddr_in, Latency > > latestSet;
		LOCK_T lock;
	} slaveLoading;
	struct {
		std::set< struct sockaddr_in > slaveSet;
		LOCK_T lock;
	} overloadedSlaves;
	struct {
		std::vector<Log> items;
		LOCK_T lock;
	} log;
	Timer statsTimer;
	Pending pending;
	static uint16_t instanceId;

	static Coordinator *getInstance() {
		static Coordinator coordinator;
		return &coordinator;
	}

	void switchPhaseForCrashedSlave( SlaveSocket *slaveSocket );

	static void signalHandler( int signal );

	bool init( char *path, OptionList &options, bool verbose );
	bool start();
	bool stop();
	void info( FILE *f = stdout );
	void debug( FILE *f = stdout );
	void dump();
	void printInstanceId( FILE *f = stdout );
	void printRemapping( FILE *f = stdout );
	void printPending( FILE *f = stdout );
	void time();
	void seal();
	void flush();
	void metadata();
	void printLog();
	void syncSlaveMeta( struct sockaddr_in slave, bool *sync );
	void releaseDegradedLock();
	void releaseDegradedLock( struct sockaddr_in slave, pthread_mutex_t *lock, pthread_cond_t *cond, bool *done );
	void syncRemappedData( struct sockaddr_in target, pthread_mutex_t *lock, pthread_cond_t *cond, bool *done );
	double getElapsedTime();
	void hash();
	void lookup();
	void appendLog( Log log );
	void interactive();
};

#endif
