#include <cstdlib>
#include <cerrno>
#include <sys/signalfd.h>
#include "epoll.hh"
#include "../util/debug.hh"

EPoll::EPoll() {
	this->efd = -1;
	this->maxEvents = 0;
	this->timeout = 0;
	this->events = 0;
	this->isRunning = false;
}

bool EPoll::init( int maxEvents, int timeout ) {
	if ( maxEvents < 1 ) {
		__ERROR__( "EPoll", "init", "The maximum number of events should be greater than 0." );
		return false;
	}

	this->efd = epoll_create1( 0 );
	if ( this->efd == -1 ) {
		__ERROR__( "EPoll", "init", "%s", strerror( errno ) );
		return false;
	}

	this->events = ( struct epoll_event * ) calloc( maxEvents, sizeof( struct epoll_event ) );
	if ( ! this->events ) {
		__ERROR__( "EPoll", "init", "Cannot allocate memory." );
		return false;
	}
	this->maxEvents = maxEvents;
	this->timeout = timeout;
	return true;
}

bool EPoll::add( int fd, uint32_t events ) {
	if ( this->efd == -1 )
		return false;
	struct epoll_event event;
	event.data.fd = fd;
	event.events = events;
	if ( epoll_ctl( this->efd, EPOLL_CTL_ADD, fd, &event ) == -1 ) {
		__ERROR__( "EPoll", "add", "%s", strerror( errno ) );
		return false;
	}
	return true;
}

bool EPoll::modify( int fd, uint32_t events ) {
	if ( this->efd == -1 )
		return false;
	struct epoll_event event;
	event.data.fd = fd;
	event.events = events;
	if ( epoll_ctl( this->efd, EPOLL_CTL_MOD, fd, &event ) == -1 ) {
		__ERROR__( "EPoll", "modify", "%s", strerror( errno ) );
		return false;
	}
	return true;
}

bool EPoll::remove( int fd ) {
	if ( this->efd == -1 )
		return false;
	if ( epoll_ctl( this->efd, EPOLL_CTL_DEL, fd, NULL ) == -1 ) {
		__ERROR__( "EPoll", "remove", "%s", strerror( errno ) );
		return false;
	}
	return true;
}

bool EPoll::start( bool (*handler)( int, uint32_t, void * ), void *data ) {
	if ( this->efd == -1 )
		return false;

	int i, sfd, numEvents;
	sigset_t sigmask;

	// Set signal fd
	sigemptyset( &sigmask );
	sigaddset( &sigmask, SIG_EPOLL );
	if ( ( sfd = signalfd( -1, &sigmask, 0 ) ) == -1 ) {
		__ERROR__( "EPoll", "start", "%s", strerror( errno ) );
		return false;
	}
	this->add( sfd, EPOLLIN | EPOLLET );

	// Start polling
	this->isRunning = true;
	while( this->isRunning ) {
		numEvents = epoll_pwait( this->efd, this->events, this->maxEvents, this->timeout, &sigmask );
		if ( numEvents == -1 ) {
			__ERROR__( "EPoll", "start", "%s", strerror( errno ) );
			this->isRunning = false;
			return false;
		}
		__ERROR__( "EPoll", "start", "Number of epoll events = %d.", numEvents );
		for ( i = 0; i < numEvents; i++ ) {
			if ( events[ i ].data.fd != sfd )
				handler( events[ i ].data.fd, events[ i ].events, data );
			else
				this->timeout = 0;
		}
	}
	::free( this->events );
	return true;
}

void EPoll::stop() {
	if ( this->efd == -1 )
		return;
	this->isRunning = false;
}

void EPoll::stop( pthread_t tid ) {
	this->stop();
	pthread_kill( tid, SIG_EPOLL );
}
