#include <cstring>
#include "remap_msg_handler.hh"

static sp_time spTimeout = { 2, 0 };

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

    this->status = REMAP_NONE;
    this->msgCount = 0;

    // construct the spread name, username
    // connect to spread daemon
    int ret = SP_connect_timeout( this->spread, this->user, 0, 1, &mbox, privateGroup, spTimeout );
    if ( ret != ACCEPT_SESSION ) {
        fprintf( stderr, "Cannot establish a session with spread daemon!\n" );
        SP_error( ret );
        return false;
    }

    // join the group
    ret = SP_join( mbox, this->group );
    fprintf( stderr, "Join %s\n", this->group );
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
