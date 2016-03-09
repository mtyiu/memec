#include "worker.hh"
#include "../main/client.hh"

bool ClientWorker::handleServerReconstructedMsg( CoordinatorEvent event, char *buf, size_t size ) {
	struct AddressHeader srcHeader, dstHeader;
	if ( ! this->protocol.parseSrcDstAddressHeader( srcHeader, dstHeader, buf, size ) ) {
		__ERROR__( "ClientWorker", "handleServerReconstructedMsg", "Invalid address header." );
		return false;
	}

	char srcTmp[ 22 ], dstTmp[ 22 ];
	Socket::ntoh_ip( srcHeader.addr, srcTmp, 16 );
	Socket::ntoh_port( srcHeader.port, srcTmp + 16, 6 );
	Socket::ntoh_ip( dstHeader.addr, dstTmp, 16 );
	Socket::ntoh_port( dstHeader.port, dstTmp + 16, 6 );
	// __DEBUG__(
	// 	YELLOW,
	__ERROR__(
		"ClientWorker", "handleServerReconstructedMsg",
		"Server: %s:%s is reconstructed at %s:%s.",
		srcTmp, srcTmp + 16, dstTmp, dstTmp + 16
	);

	// Find the server peer socket in the array map
	int index = -1, sockfd = -1;
	ServerSocket *original, *s;
	Client *client = Client::getInstance();
	ArrayMap<int, ServerSocket> *servers = &client->sockets.servers;

	// Remove the failed server
	for ( int i = 0, len = servers->size(); i < len; i++ ) {
		if ( servers->values[ i ]->equal( srcHeader.addr, srcHeader.port ) ) {
			index = i;
			original = servers->values[ i ];
			break;
		}
	}
	if ( index == -1 ) {
		__ERROR__( "ClientWorker", "handleServerReconstructedMsg", "The server is not in the list. Ignoring this server..." );
		return false;
	}
	original->stop();

	// Create a new socket for the reconstructed server
	ServerAddr serverAddr( 0, dstHeader.addr, dstHeader.port );
	s = new ServerSocket();
	s->init( serverAddr, &client->sockets.epoll );

	// Update sockfd in the array Map
	sockfd = s->getSocket();
	servers->set( index, sockfd, s );

	// add the server addrs to remapMsgHandler
	client->remapMsgHandler.removeAliveServer( original->getAddr() );
	client->remapMsgHandler.addAliveServer( s->getAddr() );

	// Connect to the server
	servers->values[ index ]->timestamp.current.setVal( 0 );
	servers->values[ index ]->timestamp.lastAck.setVal( 0 );
	if ( ! s->start() ) {
		__ERROR__( "ClientWorker", "handleServerReconstructedMsg", "Cannot start socket." );
	}
	s->registerClient();

	ClientWorker::stripeList->update();
	delete original;

	return true;
}
