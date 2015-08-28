#ifndef __COMMON_SOCKET_EPOLL_HH__
#define __COMMON_SOCKET_EPOLL_HH__

#include <pthread.h>
#include <signal.h>
#include <sys/epoll.h>

#define EPOLL_MAX_EVENTS	64
#define SIG_EPOLL			SIGUSR1
#define EPOLL_EVENT_SET		EPOLLIN | EPOLLET | EPOLLRDHUP | EPOLLONESHOT
#define EPOLL_EVENT_LISTEN	EPOLLIN | EPOLLET | EPOLLRDHUP

class EPoll {
private:
	int efd;
	int maxEvents;
	int timeout;
	struct epoll_event *events;
	bool isRunning;

public:
	EPoll();
	bool init( int maxEvents = EPOLL_MAX_EVENTS, int timeout = -1 );
	bool add( int fd, uint32_t events );
	bool modify( int fd, uint32_t events );
	bool remove( int fd );
	bool start( bool (*handler)( int, uint32_t, void * ), void *data );
	void stop();
	void stop( pthread_t tid );
};

#endif
