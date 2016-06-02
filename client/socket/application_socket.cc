#include "application_socket.hh"
#include "../../common/util/debug.hh"

ArrayMap<int, ApplicationSocket> *ApplicationSocket::applications;

void ApplicationSocket::setArrayMap( ArrayMap<int, ApplicationSocket> *applications ) {
	ApplicationSocket::applications = applications;
	applications->needsDelete = false;
}

bool ApplicationSocket::start() {
	return this->connect();
}

void ApplicationSocket::stop() {
	ApplicationSocket::applications->remove( this->sockfd );
	Socket::stop();
	// TODO: Fix memory leakage!
	// delete this;
}
