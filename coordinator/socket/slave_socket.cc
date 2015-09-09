#include "slave_socket.hh"

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
	delete this;
}

void SlaveSocket::setListenAddr( uint32_t addr, uint16_t port ) {
	this->listenAddr.addr = addr;
	this->listenAddr.port = port;
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
