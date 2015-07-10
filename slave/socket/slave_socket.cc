#include <cerrno>
#include "slave_socket.hh"
#include "../../common/util/debug.hh"

#define SOCKET_COLOR YELLOW

SlaveSocket::SlaveSocket() {
	this->isRunning = false;
	this->tid = 0;
	this->epoll = 0;
}

bool SlaveSocket::init( int type, unsigned long addr, unsigned short port, EPoll *epoll ) {
	this->epoll = epoll;
	return (
		Socket::init( type, addr, port ) &&
		this->listen() &&
		epoll->add( this->sockfd, EPOLLIN | EPOLLET )
	);
}

bool SlaveSocket::start() {
	if ( pthread_create( &this->tid, NULL, SlaveSocket::run, ( void * ) this ) != 0 ) {
		__ERROR__( "SlaveSocket", "start", "Cannot start SlaveSocket thread." );
		return false;
	}
	this->isRunning = true;
	return true;
}

void SlaveSocket::stop() {
	if ( this->isRunning ) {
		this->epoll->stop( this->tid );
		this->isRunning = false;
		pthread_join( this->tid, 0 );
	}
}

void SlaveSocket::debug() {
	__DEBUG__( SOCKET_COLOR, "SlaveSocket", "debug", "SlaveSocket thread for epoll #%lu is %srunning.", this->tid, this->isRunning ? "" : "not " );
}

void *SlaveSocket::run( void *argv ) {
	SlaveSocket *socket = ( SlaveSocket * ) argv;
	socket->epoll->start( SlaveSocket::handler, socket );
	pthread_exit( 0 );
	return 0;
}

bool SlaveSocket::handler( int fd, uint32_t events, void *data ) {
	SlaveSocket *socket = ( SlaveSocket * ) data;

	if ( ( events & EPOLLERR ) || ( events & EPOLLHUP ) ) {
		::close( fd );
	} else if ( fd == socket->getSocket() ) {
		struct sockaddr_in addr;
		socklen_t addrlen;
		while( 1 ) {
			fd = socket->accept( &addr, &addrlen );

			if ( fd == -1 ) {
				if ( errno != EAGAIN && errno != EWOULDBLOCK )
					__ERROR__( "SlaveSocket", "handler", "%s", strerror( errno ) );
				break;
			}

			socket->sockets.set( fd, addr, false );
			socket->epoll->add( fd, EPOLLIN | EPOLLET );
		}
	} else {
	}
	return true;
}
