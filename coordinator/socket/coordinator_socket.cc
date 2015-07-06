#include <cerrno>
#include "coordinator_socket.hh"
#include "../../common/util/debug.hh"

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
	return this->epoll->start( CoordinatorSocket::handler, this );
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
