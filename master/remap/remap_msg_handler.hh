#ifndef __MASTER_REMAP_REMAP_MSG_HANDLER_HH__
#define __MASTER_REMAP_REMAP_MSG_HANDLER_HH__

#include "../../common/remap/remap_msg_handler.hh"
#include "../../common/remap/remap_group.hh"

class MasterRemapMsgHandler : public RemapMsgHandler {
private:
    bool isListening;

    void setStatus( char* msg, int len );
    static void *readMessages( void *argv );

public:

    MasterRemapMsgHandler();
    ~MasterRemapMsgHandler();

    bool init( const char *user = NULL );
    void quit();

    bool start();
    bool stop();

    bool ackRemap();
};

#endif
