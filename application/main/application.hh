#ifndef __APPLICATION_MAIN_APPLICATION_HH__
#define __APPLICATION_MAIN_APPLICATION_HH__

#include <cstdio>
#include <pthread.h>
#include "../config/application_config.hh"
#include "../socket/master_socket.hh"
#include "../worker/worker.hh"
#include "../../common/ds/array_map.hh"
#include "../../common/socket/epoll.hh"
#include "../../common/signal/signal.hh"
#include "../../common/util/option.hh"
#include "../../common/util/time.hh"

// Implement the singleton pattern
class Application {
private:
	bool isRunning;
	pthread_t tid;
	struct timespec startTime;
	std::vector<ApplicationWorker> workers;

	Application();
	// Do not implement
	Application( Application const& );
	void operator=( Application const& );

	void free();
	// Commands
	void help();
	void time();
	bool set( char *key, char *path );
	bool get( char *key, char *path );

public:
	struct {
		ApplicationConfig application;
	} config;
	struct {
		EPoll epoll;
		ArrayMap<int, MasterSocket> masters;
	} sockets;
	ApplicationEventQueue eventQueue;
	
	static Application *getInstance() {
		static Application application;
		return &application;
	}

	static void signalHandler( int signal );
	static void *run( void *argv );
	static bool epollHandler( int fd, uint32_t events, void *data );

	bool init( char *path, OptionList &options, bool verbose );
	bool start();
	bool stop();
	void info( FILE *f = stdout );
	void debug( FILE *f = stdout );
	double getElapsedTime();
	void interactive();
};

#endif
