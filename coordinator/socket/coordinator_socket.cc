#include <cerrno>
#include "coordinator_socket.hh"
#include "../../common/util/debug.hh"

bool CoordinatorSocket::prepare( int maxEvents, int timeout ) {
	return (
		this->listen() &&
		this->epoll.init( maxEvents, timeout ) &&
		this->epoll.add( this->sockfd, EPOLLIN | EPOLLET )
	);
}

bool CoordinatorSocket::start() {
	return this->epoll.start( CoordinatorSocket::handler, this );
}

bool CoordinatorSocket::handler( int fd, uint32_t events, void *data ) {
	CoordinatorSocket *socket = ( CoordinatorSocket * ) data;

	if ( ( events & EPOLLERR ) || ( events & EPOLLHUP ) ) {
		::close( fd );
	} else if ( fd == socket->getSocket() ) {
		while( 1 ) {
			fd = socket->accept();

			if ( fd == -1 ) {
				if ( errno != EAGAIN && errno != EWOULDBLOCK )
					__ERROR__( "CoordinatorSocket", "handler", "%s", strerror( errno ) );
				break;
			}

			socket->epoll.add( fd, EPOLLIN | EPOLLET );
			__ERROR__( "CoordinatorSocket", "handler", "accept() returns %d", fd );
		}
	}
	return true;
}
