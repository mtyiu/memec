#ifndef __SLAVE_MAIN_SLAVE_HH__
#define __SLAVE_MAIN_SLAVE_HH__

#include <map>
#include <vector>
#include <cstdio>
#include "../buffer/mixed_chunk_buffer.hh"
#include "../config/slave_config.hh"
#include "../ds/map.hh"
#include "../ds/pending.hh"
#include "../ds/slave_load.hh"
#include "../event/event_queue.hh"
#include "../socket/coordinator_socket.hh"
#include "../socket/master_socket.hh"
#include "../socket/slave_socket.hh"
#include "../socket/slave_peer_socket.hh"
#include "../worker/worker.hh"
#include "../../common/coding/coding.hh"
#include "../../common/config/global_config.hh"
#include "../../common/ds/array_map.hh"
#include "../../common/ds/chunk.hh"
#include "../../common/ds/memory_pool.hh"
#include "../../common/signal/signal.hh"
#include "../../common/socket/epoll.hh"
#include "../../common/stripe_list/stripe_list.hh"
#include "../../common/util/option.hh"
#include "../../common/util/time.hh"

// Implement the singleton pattern
class Slave {
private:
	bool isRunning;
	struct timespec startTime;
	std::vector<SlaveWorker> workers;

	Slave();
	// Do not implement
	Slave( Slave const& );
	void operator=( Slave const& );

	void free();
	// Commands
	void sync();
	void help();

public:
	struct {
		GlobalConfig global;
		SlaveConfig slave;
	} config;
	struct {
		SlaveSocket self;
		EPoll epoll;
		ArrayMap<int, CoordinatorSocket> coordinators;
		ArrayMap<int, MasterSocket> masters;
		ArrayMap<int, SlavePeerSocket> slavePeers;
	} sockets;
	Pending pending;
	SlaveLoad load;
	Map map;
	SlaveEventQueue eventQueue;
	Coding *coding;
	StripeList<SlavePeerSocket> *stripeList;
	std::vector<StripeListIndex> stripeListIndex;
	MemoryPool<Chunk> *chunkPool;
	std::vector<MixedChunkBuffer *> chunkBuffer;

	static Slave *getInstance() {
		static Slave slave;
		return &slave;
	}

	static void signalHandler( int signal );

	bool init( char *path, OptionList &options, bool verbose );
	bool start();
	bool stop();
	void info( FILE *f = stdout );
	void debug( FILE *f = stdout );
	void dump();
	void printPending( FILE *f = stdout );
	void time();
	void alarm();
	Load &aggregateLoad( FILE *f = 0 );
	double getElapsedTime();
	void interactive();
};

#endif
