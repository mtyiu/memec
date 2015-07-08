#ifndef __COMMON_EVENT_EVENT_QUEUE_HH__
#define __COMMON_EVENT_EVENT_QUEUE_HH__

#include <cstdio>

class EventQueue {
public:
	virtual bool start() = 0;
	virtual void stop() = 0;
	virtual void info( FILE *f = stdout ) = 0;
	virtual void debug( FILE *f = stdout ) = 0;
};

#endif
