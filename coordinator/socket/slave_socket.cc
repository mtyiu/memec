#include "slave_socket.hh"

bool SlaveSocket::start() {
	return this->connect();
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
