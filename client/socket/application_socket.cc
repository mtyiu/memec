#include "application_socket.hh"
#include "../main/client.hh"
#include "../../common/socket/named_pipe.hh"
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

	if ( this->isNamedPipe() ) {
		this->print( stderr );
		NamedPipe &namedPipe = Client::getInstance()->sockets.namedPipe;
		namedPipe.close( this->sockfd );
		namedPipe.close( this->wPipefd );

		this->sockfd = - this->sockfd;
		this->wPipefd = - this->wPipefd;
	}

	// TODO: Fix memory leakage!
	// delete this;
}
