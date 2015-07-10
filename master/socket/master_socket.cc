#include <cerrno>
#include "master_socket.hh"
#include "../../common/util/debug.hh"

#define SOCKET_COLOR YELLOW

MasterSocket::MasterSocket() {
	this->isRunning = false;
	this->tid = 0;
	this->epoll = 0;
}

bool MasterSocket::init( int type, unsigned long addr, unsigned short port, EPoll *epoll ) {
	this->epoll = epoll;
	return (
		Socket::init( type, addr, port ) &&
		this->listen() &&
		epoll->add( this->sockfd, EPOLLIN | EPOLLET )
	);
}

bool MasterSocket::start() {
	if ( pthread_create( &this->tid, NULL, MasterSocket::run, ( void * ) this ) != 0 ) {
		__ERROR__( "MasterSocket", "start", "Cannot start MasterSocket thread." );
		return false;
	}
	this->isRunning = true;
	return true;
}

void MasterSocket::stop() {
	if ( this->isRunning ) {
		this->epoll->stop( this->tid );
		this->isRunning = false;
		pthread_join( this->tid, 0 );
	}
}

void MasterSocket::debug() {
	__DEBUG__( SOCKET_COLOR, "MasterSocket", "debug", "MasterSocket thread for epoll #%lu is %srunning.", this->tid, this->isRunning ? "" : "not " );
}

void *MasterSocket::run( void *argv ) {
	MasterSocket *socket = ( MasterSocket * ) argv;
	socket->epoll->start( MasterSocket::handler, socket );
	pthread_exit( 0 );
	return 0;
}

bool MasterSocket::handler( int fd, uint32_t events, void *data ) {
	MasterSocket *socket = ( MasterSocket * ) data;

	if ( ( events & EPOLLERR ) || ( events & EPOLLHUP ) ) {
		::close( fd );
	} else if ( fd == socket->getSocket() ) {
		struct sockaddr_in addr;
		socklen_t addrlen;
		while( 1 ) {
			fd = socket->accept( &addr, &addrlen );

			if ( fd == -1 ) {
				if ( errno != EAGAIN && errno != EWOULDBLOCK )
					__ERROR__( "MasterSocket", "handler", "%s", strerror( errno ) );
				break;
			}

			socket->sockets.set( fd, addr, false );
			socket->epoll->add( fd, EPOLLIN | EPOLLET );
		}
	} else {
	}
	return true;
}
