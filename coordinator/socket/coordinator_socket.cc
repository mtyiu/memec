#include <cerrno>
#include "coordinator_socket.hh"
#include "../../common/util/debug.hh"

#define SOCKET_COLOR YELLOW

CoordinatorSocket::CoordinatorSocket() {
	this->isRunning = false;
	this->epoll = 0;
	this->tid = 0;
}

bool CoordinatorSocket::init( int type, unsigned long addr, unsigned short port, int numSlaves, EPoll *epoll ) {
	this->epoll = epoll;
	bool ret = (
		Socket::init( type, addr, port ) &&
		this->listen() &&
		epoll->add( this->sockfd, EPOLLIN | EPOLLET )
	);
	if ( ret ) {
		this->sockets.reserve( numSlaves );
	}
	return ret;
}

bool CoordinatorSocket::start() {
	this->isRunning = true;
	if ( pthread_create( &this->tid, NULL, CoordinatorSocket::run, ( void * ) this ) != 0 ) {
		__ERROR__( "CoordinatorSocket", "start", "Cannot start CoordinatorSocket thread." );
		return false;
	}
	this->isRunning = true;
	return true;
}

void CoordinatorSocket::stop() {
	if ( this->isRunning ) {
		this->epoll->stop( this->tid );
		this->isRunning = false;
		pthread_join( this->tid, 0 );
	}
}

void CoordinatorSocket::debug() {
	__DEBUG__( SOCKET_COLOR, "CoordinatorSocket", "debug", "CoordinatorSocket thread for epoll #%lu is %srunning.", this->tid, this->isRunning ? "" : "not " );
}

void *CoordinatorSocket::run( void *argv ) {
	CoordinatorSocket *socket = ( CoordinatorSocket * ) argv;
	socket->epoll->start( CoordinatorSocket::handler, socket );
	pthread_exit( 0 );
	return 0;
}

bool CoordinatorSocket::handler( int fd, uint32_t events, void *data ) {
	CoordinatorSocket *socket = ( CoordinatorSocket * ) data;

	if ( ( events & EPOLLERR ) || ( events & EPOLLHUP ) ) {
		::close( fd );
	} else if ( fd == socket->getSocket() ) {
		struct sockaddr_in addr;
		socklen_t addrlen;
		while( 1 ) {
			fd = socket->accept( &addr, &addrlen );

			if ( fd == -1 ) {
				if ( errno != EAGAIN && errno != EWOULDBLOCK )
					__ERROR__( "CoordinatorSocket", "handler", "%s", strerror( errno ) );
				break;
			}

			socket->sockets.set( fd, addr, false );
			socket->epoll->add( fd, EPOLLIN | EPOLLET );
		}
	} else {
	}
	return true;
}
