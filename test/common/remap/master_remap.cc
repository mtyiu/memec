#include <unistd.h>
#include "../../../common/remap/remap_status.hh"
#include "../../../master/remap/remap_msg_handler.hh"

#define TIME_OUT 1

int main () {
    MasterRemapMsgHandler* mh = new MasterRemapMsgHandler();
    mh->init( "master01" );
    if ( ! mh->start() ) {
        fprintf( stderr, "Cannot start reading message with message handler\n" );
    } else { 
        while ( mh->getStatus() != REMAP_PREPARE_START ) 
            sleep( TIME_OUT );
        mh->ackRemap();
        while ( mh->getStatus() != REMAP_PREPARE_END )
            sleep( TIME_OUT );
        mh->ackRemap();
        while ( mh->getStatus() != REMAP_NONE )
            sleep( TIME_OUT );
        mh->stop();
    }
    mh->quit();

    delete mh;

    return 0;
}
