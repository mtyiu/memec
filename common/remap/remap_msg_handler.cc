#include <cstring>
#include "remap_msg_handler.hh"

RemapMsgHandler::RemapMsgHandler() {
	this->reader = -1;
	this->isConnected = false;
	this->msgCount = 0;
	this->group = ( char* ) GROUP_NAME;
	LOCK_INIT( &this->stlock );
}

RemapMsgHandler::~RemapMsgHandler() {
}

bool RemapMsgHandler::init( const char *spread, const char *user ) {
	this->quit();
	if ( spread ) {
		memcpy( this->spread, spread, MAX_SPREAD_NAME - 1 );
		memset( this->spread + MAX_SPREAD_NAME - 1, 0, 1 );
	}
	if ( user ) {
		memcpy( this->user, user, MAX_SPREAD_NAME - 1 );
		memset( this->user + MAX_SPREAD_NAME - 1, 0, 1 );
	}

	LOCK( &this->stlock );
	this->status = REMAP_NONE;
	UNLOCK( &this->stlock );
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
