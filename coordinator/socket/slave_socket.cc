#include "slave_socket.hh"

bool SlaveSocket::init( int sockfd, struct sockaddr_in addr ) {
	this->mode = SOCKET_MODE_SLAVE_CLIENT;
	this->sockfd = sockfd;
	this->addr = addr;
	return true;
}

bool SlaveSocket::start() {
	return true;
}

ssize_t SlaveSocket::send( char *buf, size_t ulen, bool &connected ) {
	return Socket::send( this->sockfd, buf, ulen, connected );
}
ssize_t SlaveSocket::recv( char *buf, size_t ulen, bool &connected, bool wait ) {
	return Socket::recv( this->sockfd, buf, ulen, connected, wait );
}
