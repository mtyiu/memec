#include "application_socket.hh"
#include "../../common/util/debug.hh"

bool ApplicationSocket::start() {
	return this->connect();
}

ssize_t ApplicationSocket::send( char *buf, size_t ulen, bool &connected ) {
	ssize_t bytes = Socket::send( this->sockfd, buf, ulen, connected );
	__DEBUG__( MAGENTA, "ApplicationSocket", "send", "Sent %ld bytes...", bytes );
	return bytes;
}
ssize_t ApplicationSocket::recv( char *buf, size_t ulen, bool &connected, bool wait ) {
	ssize_t bytes = Socket::recv( this->sockfd, buf, ulen, connected, wait );
	__DEBUG__( MAGENTA, "ApplicationSocket", "recv", "Received %ld bytes...", bytes );
	return bytes;
}
