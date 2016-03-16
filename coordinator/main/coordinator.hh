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
#include "../state_transit/state_transit_handler.hh"
#include "../socket/coordinator_socket.hh"
#include "../socket/client_socket.hh"
#include "../socket/server_socket.hh"
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

	// Helper functions to determine server loading
	void updateAverageServerLoading( ArrayMap<struct sockaddr_in, Latency> *serverGetLatency,
			ArrayMap<struct sockaddr_in, Latency> *serverSetLatency );
	// return previously overloaded servers for per-server phase change
	std::set<struct sockaddr_in> updateOverloadedServerSet(
			ArrayMap<struct sockaddr_in, Latency> *serverGetLatency,
			ArrayMap<struct sockaddr_in, Latency> *serverSetLatency,
			std::set<struct sockaddr_in> *serverSet
	);
	void switchPhase( std::set<struct sockaddr_in> prevOverloadedServers );

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
		ArrayMap<int, ClientSocket> clients;
		ArrayMap<int, ServerSocket> servers;
		ArrayMap<int, ServerSocket> backupServers;
	} sockets;
	IDGenerator idGenerator;
	CoordinatorEventQueue eventQueue;
	/* Stripe list */
	StripeList<ServerSocket> *stripeList;
	/* Remapping */
	CoordinatorStateTransitHandler *stateTransitHandler;
	RemappingRecordMap remappingRecords;
	struct {
		std::unordered_map<Key, RemappingRecord> toSend;
		LOCK_T toSendLock;
	} pendingRemappingRecords;
	PacketPool packetPool;
	/* Loading statistics */
	struct {
		// ( serverAddr, ( mastserAddr, Latency ) )
		ArrayMap< struct sockaddr_in, ArrayMap< struct sockaddr_in, Latency > > latestGet;
		ArrayMap< struct sockaddr_in, ArrayMap< struct sockaddr_in, Latency > > latestSet;
		LOCK_T lock;
	} serverLoading;
	struct {
		std::set< struct sockaddr_in > serverSet;
		LOCK_T lock;
	} overloadedServers;
	struct {
		std::vector<Log> items;
		LOCK_T lock;
	} log;
	struct {
		bool isRecovering;
		std::vector<ServerSocket *> sockets;
		LOCK_T lock;
	} waitingForRecovery;
	Timer statsTimer;
	Pending pending;
	static uint16_t instanceId;

	static Coordinator *getInstance() {
		static Coordinator coordinator;
		return &coordinator;
	}

	void switchPhaseForCrashedServer( ServerSocket *serverSocket );

	static void signalHandler( int signal );

	bool init( char *path, OptionList &globalOptions, OptionList &coordinatorOptions, bool verbose );
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
	void syncServerMeta( struct sockaddr_in server, bool *sync );
	void releaseDegradedLock();
	void releaseDegradedLock( struct sockaddr_in server, pthread_mutex_t *lock, pthread_cond_t *cond, bool *done );
	void syncRemappedData( struct sockaddr_in target, pthread_mutex_t *lock, pthread_cond_t *cond, bool *done );
	double getElapsedTime();
	void hash();
	void lookup();
	void stripe();
	void appendLog( Log log );
	void setServer( bool overloaded );
	void switchToManualOverload();
	void switchToAutoOverload();
	void interactive();
};

#endif
