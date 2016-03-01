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

int RemapMsgHandler::sendState( std::vector<struct sockaddr_in> &servers, int numGroup, const char targetGroup[][ MAX_GROUP_NAME ] ) {
	char buf[ MAX_MESSLEN ];
	int len = 0, ret = 0;
	int recordSize = this->serverStateRecordSize;

	buf[0] = ( uint8_t ) servers.size();
	len += 1;

	for ( uint32_t i = 0; i < servers.size(); i++ ) {
		// server info
		*( ( uint32_t * )( buf + len ) ) = servers.at(i).sin_addr.s_addr;
		*( ( uint32_t * )( buf + len + sizeof( uint32_t ) ) ) = servers.at(i).sin_port;
		*( ( uint32_t * )( buf + len + sizeof( uint32_t ) + sizeof( uint16_t ) ) ) = ( uint8_t ) this->serversState[ servers.at(i) ];
		len += recordSize;
	}
	if ( numGroup > 1 ) {
		ret = SP_multigroup_multicast ( this->mbox, MSG_TYPE, numGroup, targetGroup , 0, len, buf ) > 0;
	} else {
		ret = SP_multicast ( this->mbox, MSG_TYPE, targetGroup[0], 0, len, buf ) > 0;
	}
	return ret;
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
	serversStateLock.clear();

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
	uint32_t serverCount = this->serversState.size();
	char buf[ INET_ADDRSTRLEN ];
	for ( auto server : this->serversState ) {
		inet_ntop( AF_INET, &server.first.sin_addr, buf, INET_ADDRSTRLEN ),
		fprintf(
			stderr,
			"\tServer %s:%hu --> ",
			buf,
			ntohs( server.first.sin_port )
		);
		switch( server.second ) {
			case REMAP_UNDEFINED:
				fprintf( stderr, "REMAP_UNDEFINED\n" );
				break;
			case REMAP_NORMAL:
				fprintf( stderr, "REMAP_NORMAL\n" );
				break;
			case REMAP_INTERMEDIATE:
				fprintf( stderr, "REMAP_INTERMEDIATE\n" );
				break;
			case REMAP_COORDINATED:
				fprintf( stderr, "REMAP_COORDINATED\n" );
				break;
			case REMAP_DEGRADED:
				fprintf( stderr, "REMAP_DEGRADED\n" );
				break;
			case REMAP_WAIT_DEGRADED:
				fprintf( stderr, "REMAP_WAIT_DEGRADED\n" );
				break;
			case REMAP_WAIT_NORMAL:
				fprintf( stderr, "REMAP_WAIT_NORMAL\n" );
				break;
		}
	}
	fprintf( stderr, "No. of servers = %u\n", serverCount );
}
