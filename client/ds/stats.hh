#ifndef __CLIENT_DS_STATS_HH__
#define __CLIENT_DS_STATS_HH__

#include <ctime>
#include <arpa/inet.h>
#include "../../common/config/server_addr.hh"
#include "../../common/ds/array_map.hh"
#include "../../common/ds/latency.hh"
#include "../../common/lock/lock.hh"

typedef struct {
	struct sockaddr_in addr;
	struct timespec sttime;
} RequestStartTime;

typedef struct {
	struct {
		ArrayMap< struct sockaddr_in, std::set< Latency > > get;
		ArrayMap< struct sockaddr_in, std::set< Latency > > set;
	} past;
	struct {
		ArrayMap< struct sockaddr_in, Latency > get;
		ArrayMap< struct sockaddr_in, Latency > set;
	} current;
	struct {
		ArrayMap< struct sockaddr_in, Latency > get;
		ArrayMap< struct sockaddr_in, Latency > set;
	} cumulative;
	struct {
		ArrayMap< struct sockaddr_in, Latency > get;
		ArrayMap< struct sockaddr_in, Latency > set;
	} cumulativeMirror;
	LOCK_T lock;
} SlaveLoading;

typedef struct {
	std::set< struct sockaddr_in > slaveSet;
	LOCK_T lock;
} OverloadedSlave;

#endif
