#ifndef __MASTER_DS_STATS_HH__
#define __MASTER_DS_STATS_HH__

#include <ctime>
#include "../../common/config/server_addr.hh"

typedef struct {
	struct sockaddr_in addr;
	struct timespec sttime;
} RequestStartTime;

#endif
