#include "slave_socket.hh"
#include "../main/coordinator.hh"
#include "../event/slave_event.hh"

ArrayMap<int, SlaveSocket> *SlaveSocket::slaves;

void SlaveSocket::setArrayMap( ArrayMap<int, SlaveSocket> *slaves ) {
	SlaveSocket::slaves = slaves;
	slaves->needsDelete = false;
}

bool SlaveSocket::start() {
	return this->connect();
}

void SlaveSocket::stop() {
	SlaveSocket::slaves->remove( this->sockfd );
	Socket::stop();

	SlaveEvent event;
	event.disconnect( this );
	Coordinator::getInstance()->eventQueue.insert( event );
	// TODO: Fix memory leakage!
	// delete this;
}

bool SlaveSocket::setRecvFd( int fd, struct sockaddr_in *addr ) {
	bool ret = false;
	this->recvAddr = *addr;

	if ( fd != this->sockfd ) {
		this->sockfd = fd;
		ret = true;
	}
	return ret;
}

ssize_t SlaveSocket::send( char *buf, size_t ulen, bool &connected ) {
	return Socket::send( this->sockfd, buf, ulen, connected );
}

ssize_t SlaveSocket::recv( char *buf, size_t ulen, bool &connected, bool wait ) {
	return Socket::recv( this->sockfd, buf, ulen, connected, wait );
}

ssize_t SlaveSocket::recvRem( char *buf, size_t expected, char *prevBuf, size_t prevSize, bool &connected ) {
	return Socket::recvRem( this->sockfd, buf, expected, prevBuf, prevSize, connected );
}

bool SlaveSocket::done() {
	return Socket::done( this->sockfd );
}
