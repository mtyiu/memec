#include "master_socket.hh"
#include "../../common/util/debug.hh"

bool MasterSocket::start() {
	return this->connect();
}

ssize_t MasterSocket::send( char *buf, size_t ulen, bool &connected ) {
	return Socket::send( this->sockfd, buf, ulen, connected );
}

ssize_t MasterSocket::recv( char *buf, size_t ulen, bool &connected, bool wait ) {
	return Socket::recv( this->sockfd, buf, ulen, connected, wait );
}

ssize_t MasterSocket::recvRem( char *buf, size_t expected, char *prevBuf, size_t prevSize, bool &connected ) {
	return Socket::recvRem( this->sockfd, buf, expected, prevBuf, prevSize, connected );
}

bool MasterSocket::done() {
	return Socket::done( this->sockfd );
}
