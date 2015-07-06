#include <cerrno>
#include "master_socket.hh"
#include "../../common/util/debug.hh"

bool MasterSocket::init( int type, unsigned long addr, unsigned short port, EPoll *epoll ) {
	this->epoll = epoll;
	return (
		Socket::init( type, addr, port ) &&
		this->listen() &&
		epoll->add( this->sockfd, EPOLLIN | EPOLLET )
	);
}

bool MasterSocket::start() {
	return this->epoll->start( MasterSocket::handler, this );
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

			socket->temps.set( fd, addr, false );
			socket->epoll->add( fd, EPOLLIN | EPOLLET );
		}
	} else {
	}
	return true;
}
