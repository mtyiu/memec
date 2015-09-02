#ifndef __COMMON_REMAP_REMAP_MSG_HANDLER_HH__
#define __COMMON_REMAP_REMAP_MSG_HANDLER_HH__

#include <cstdio>
#include <stdint.h>
#include <sys/types.h>
#include <pthread.h>
#include <sp.h>

#define MAX_MESSLEN     1024
#define MAX_SPREAD_NAME 1024
#define MAX_GROUP_NUM   2
#define GROUP_NAME      "plio"
#define MSG_TYPE        FIFO_MESS


class RemapMsgHandler {
protected:
    mailbox mbox;
    char privateGroup[ MAX_GROUP_NAME ];
    char spread[ MAX_SPREAD_NAME ];
    char user[ MAX_SPREAD_NAME ];

    pthread_mutex_t connlock;
    bool isConnected;
    uint32_t msgCount;

    inline void increMsgCount() {
        this->msgCount++;
    }

    inline void decreMsgCount() {
        this->msgCount--;
    }

public:
    RemapMsgHandler() {
        pthread_mutex_init(&this->connlock, NULL);
        this->isConnected = false;
        this->msgCount = 0;
    }
    
    inline bool getIsConnected () {
        return this->isConnected;
    }

    bool init(char* spread = NULL, char* user = NULL);

    inline void quit() {
        if ( isConnected ) {
            SP_leave( mbox, GROUP_NAME );
            SP_disconnect( mbox );
        }
        pthread_mutex_lock(&this->connlock);
        isConnected = false;
        pthread_mutex_unlock(&this->connlock);
    }

    // blocking to send or recieve message
    virtual int run() = 0;
};

#endif
