#ifndef __APPLICATION_WORKER_WORKER_HH__
#define __APPLICATION_WORKER_WORKER_HH__

#include <cstdio>
#include "../config/application_config.hh"
#include "../ds/pending.hh"
#include "../event/event_queue.hh"
#include "../protocol/protocol.hh"
#include "../../common/worker/worker.hh"
#include "../../common/ds/id_generator.hh"

class ApplicationWorker : public Worker {
private:
	uint32_t workerId;
	ApplicationProtocol protocol;
	struct {
		char *value;
		uint32_t valueSize;
	} buffer;
	static IDGenerator *idGenerator;
	static ApplicationEventQueue *eventQueue;
	static Pending *pending;

	void dispatch( MixedEvent event );
	void dispatch( ClientEvent event );
	void free();
	static void *run( void *argv );

public:
	static bool init();
	bool init( ApplicationConfig &config, uint32_t workerId );
	bool start();
	void stop();
	void print( FILE *f = stdout );
};

#endif
