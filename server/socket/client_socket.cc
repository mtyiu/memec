#include "client_socket.hh"
#include "../../common/util/debug.hh"

ArrayMap<int, MasterSocket> *MasterSocket::masters;

void MasterSocket::setArrayMap( ArrayMap<int, MasterSocket> *masters ) {
	MasterSocket::masters = masters;
	masters->needsDelete = false;
}

bool MasterSocket::start() {
	return this->connect();
}

void MasterSocket::stop() {
	MasterSocket::masters->remove( this->sockfd );
	Socket::stop();
	// TODO: Fix memory leakage!
	// delete this;
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
