#include "master_socket.hh"

bool MasterSocket::init( int sockfd, struct sockaddr_in addr ) {
	this->mode = SOCKET_MODE_MASTER_CLIENT;
	this->sockfd = sockfd;
	this->addr = addr;
	return true;
}

bool MasterSocket::start() {
	return true;
}

ssize_t MasterSocket::send( char *buf, size_t ulen, bool &connected ) {
	return Socket::send( this->sockfd, buf, ulen, connected );
}
ssize_t MasterSocket::recv( char *buf, size_t ulen, bool &connected, bool wait ) {
	return Socket::recv( this->sockfd, buf, ulen, connected, wait );
}
