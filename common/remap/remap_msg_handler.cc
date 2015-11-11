#include <cstring>
#include <arpa/inet.h>
#include "remap_msg_handler.hh"

RemapMsgHandler::RemapMsgHandler() {
	this->reader = -1;
	this->isConnected = false;
	this->msgCount = 0;
	this->group = ( char* ) GROUP_NAME;
}

RemapMsgHandler::~RemapMsgHandler() {
}

bool RemapMsgHandler::sendStatus( std::vector<struct sockaddr_in> slaves, const char *targetGroup ) {
	char buf[ MAX_MESSLEN ];
	int len = 0;
	int recordSize = this->slaveStatusRecordSize;

	buf[0] = ( uint8_t ) slaves.size();
	len += 1;

	for ( uint32_t i = 0; i < slaves.size(); i++ ) {
		// slave info
		*( ( uint32_t * )( buf + len ) ) = slaves.at(i).sin_addr.s_addr;
		*( ( uint32_t * )( buf + len + sizeof( uint32_t ) ) ) = slaves.at(i).sin_port;
		*( ( uint32_t * )( buf +  len + sizeof( uint32_t ) + sizeof( uint16_t ) ) ) = ( uint8_t ) this->slavesStatus[ slaves.at(i) ];
		len += recordSize;
	}
	return ( SP_multicast ( this->mbox, MSG_TYPE, targetGroup , 0, len, buf ) > 0 );
}

bool RemapMsgHandler::init( const char *spread, const char *user ) {
	//this->quit();
	if ( spread ) {
		memcpy( this->spread, spread, MAX_SPREAD_NAME - 1 );
		memset( this->spread + MAX_SPREAD_NAME - 1, 0, 1 );
	}
	if ( user ) {
		memcpy( this->user, user, MAX_SPREAD_NAME - 1 );
		memset( this->user + MAX_SPREAD_NAME - 1, 0, 1 );
	}

	this->msgCount = 0;
	slavesStatusLock.clear();

	// construct the spread name, username
	// connect to spread daemon
	int ret = SP_connect( this->spread, this->user, 0, 1, &mbox, privateGroup );
	if ( ret != ACCEPT_SESSION ) {
		fprintf( stderr, "Cannot establish a session with spread daemon!\n" );
		SP_error( ret );
		return false;
	}

	// join the group
	ret = SP_join( mbox, this->group );
	if ( ret != 0 ) {
		fprintf( stderr, "Cannot join the group %s!\n", this->group );
		SP_error( ret );
	}

	this->isConnected = true;

	return true;
}

void RemapMsgHandler::quit() {
	if ( isConnected ) {
		isConnected = false;
		SP_leave( mbox, GROUP_NAME );
		SP_disconnect( mbox );
	}
}

void RemapMsgHandler::listAliveSlaves() {
	uint32_t slaveCount = this->slavesStatus.size();
	char buf[ INET_ADDRSTRLEN ];
	fprintf( stderr, "No. of slaves = %u\n", slaveCount );
	for ( auto slave : this->slavesStatus ) {
		inet_ntop( AF_INET, &slave.first.sin_addr, buf, INET_ADDRSTRLEN ), 
		fprintf( 
			stderr, 
			"\tSlave %s:%hu\n", 
			buf,
			ntohs( slave.first.sin_port ) 
		);
	}
	fprintf( stderr, "No. of slaves = %u\n", slaveCount );
}
