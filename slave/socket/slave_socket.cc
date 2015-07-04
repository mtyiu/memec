#include <cerrno>
#include "slave_socket.hh"
#include "../../common/util/debug.hh"

bool SlaveSocket::init( int type, unsigned long addr, unsigned short port, int maxEvents, int timeout ) {
	return (
		Socket::init( type, addr, port ) &&
		this->listen() &&
		this->epoll.init( maxEvents, timeout ) &&
		this->epoll.add( this->sockfd, EPOLLIN | EPOLLET )
	);
}

bool SlaveSocket::start() {
	return this->epoll.start( SlaveSocket::handler, this );
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

			socket->temps.set( fd, addr, false );
			socket->epoll.add( fd, EPOLLIN | EPOLLET );
		}
	} else {
	}
	return true;
}
