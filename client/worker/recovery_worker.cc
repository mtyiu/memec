#include "worker.hh"
#include "../main/master.hh"

bool MasterWorker::handleSlaveReconstructedMsg( CoordinatorEvent event, char *buf, size_t size ) {
	struct AddressHeader srcHeader, dstHeader;
	if ( ! this->protocol.parseSrcDstAddressHeader( srcHeader, dstHeader, buf, size ) ) {
		__ERROR__( "MasterWorker", "handleSlaveReconstructedMsg", "Invalid address header." );
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
		"MasterWorker", "handleSlaveReconstructedMsg",
		"Slave: %s:%s is reconstructed at %s:%s.",
		srcTmp, srcTmp + 16, dstTmp, dstTmp + 16
	);

	// Find the slave peer socket in the array map
	int index = -1, sockfd = -1;
	SlaveSocket *original, *s;
	Master *master = Master::getInstance();
	ArrayMap<int, SlaveSocket> *slaves = &master->sockets.slaves;

	// Remove the failed slave
	for ( int i = 0, len = slaves->size(); i < len; i++ ) {
		if ( slaves->values[ i ]->equal( srcHeader.addr, srcHeader.port ) ) {
			index = i;
			original = slaves->values[ i ];
			break;
		}
	}
	if ( index == -1 ) {
		__ERROR__( "MasterWorker", "handleSlaveReconstructedMsg", "The slave is not in the list. Ignoring this slave..." );
		return false;
	}
	original->stop();

	// Create a new socket for the reconstructed slave
	ServerAddr serverAddr( 0, dstHeader.addr, dstHeader.port );
	s = new SlaveSocket();
	s->init( serverAddr, &master->sockets.epoll );

	// Update sockfd in the array Map
	sockfd = s->getSocket();
	slaves->set( index, sockfd, s );

	// add the slave addrs to remapMsgHandler
	master->remapMsgHandler.removeAliveSlave( original->getAddr() );
	master->remapMsgHandler.addAliveSlave( s->getAddr() );

	// Connect to the slave
	slaves->values[ index ]->timestamp.current.setVal( 0 );
	slaves->values[ index ]->timestamp.lastAck.setVal( 0 );
	if ( ! s->start() ) {
		__ERROR__( "MasterWorker", "handleSlaveReconstructedMsg", "Cannot start socket." );
	}
	s->registerMaster();

	MasterWorker::stripeList->update();
	delete original;

	return true;
}
