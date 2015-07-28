#ifndef __SLAVE_MAIN_SLAVE_HH__
#define __SLAVE_MAIN_SLAVE_HH__

#include <map>
#include <vector>
#include <cstdio>
#include "../buffer/chunk_buffer.hh"
#include "../buffer/data_chunk_buffer.hh"
#include "../buffer/parity_chunk_buffer.hh"
#include "../config/slave_config.hh"
#include "../event/event_queue.hh"
#include "../socket/coordinator_socket.hh"
#include "../socket/master_socket.hh"
#include "../socket/slave_socket.hh"
#include "../socket/slave_peer_socket.hh"
#include "../worker/worker.hh"
#include "../../common/config/global_config.hh"
#include "../../common/ds/array_map.hh"
#include "../../common/ds/chunk.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/key_value.hh"
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
	void dump();
	void help();
	void time();

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
	struct {
		std::map<Key, KeyValue *> keyValue;
	} map;
	SlaveEventQueue eventQueue;
	StripeList<SlavePeerSocket> *stripeList;
	std::vector<StripeListIndex> stripeListIndex;
	ChunkBuffer **chunkBuffer;
	MemoryPool<Chunk> *chunkPool;
	
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
	double getElapsedTime();
	void interactive();
};

#endif
