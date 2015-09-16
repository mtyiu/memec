#ifndef __MASTER_MAIN_MASTER_HH__
#define __MASTER_MAIN_MASTER_HH__

#include <map>
#include <set>
#include <cstdio>
#include "../config/master_config.hh"
#include "../ds/pending.hh"
#include "../event/event_queue.hh"
#include "../remap/remap_msg_handler.hh"
#include "../socket/application_socket.hh"
#include "../socket/coordinator_socket.hh"
#include "../socket/master_socket.hh"
#include "../socket/slave_socket.hh"
#include "../worker/worker.hh"
#include "../../common/config/global_config.hh"
#include "../../common/ds/array_map.hh"
#include "../../common/ds/id_generator.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/key_value.hh"
#include "../../common/ds/packet_pool.hh"
#include "../../common/ds/latency.hh"
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

	MasterRemapMsgHandler remapMsgHandler;

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
	} sockets;
	IDGenerator idGenerator;
	Pending pending;
	MasterEventQueue eventQueue;
	PacketPool packetPool;
	StripeList<SlaveSocket> *stripeList;

	struct {
		struct {
			ArrayMap< ServerAddr, std::set< Latency > > get;
			ArrayMap< ServerAddr, std::set< Latency > > set;
		} past;
		struct {
			ArrayMap< ServerAddr, Latency > get;
			ArrayMap< ServerAddr, Latency > set;
		} current;
		struct {
			ArrayMap< ServerAddr, Latency > get;
			ArrayMap< ServerAddr, Latency > set;
		} cumulative;
		pthread_mutex_t loadLock;
	} slaveLoading;

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
	void printPending( FILE *f = stdout );
	void time();
	double getElapsedTime();
	void interactive();
};

#endif
