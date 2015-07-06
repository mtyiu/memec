#ifndef __COMMON_UTIL_TIME_HH__
#define __COMMON_UTIL_TIME_HH__

#include <time.h>

#ifdef __MACH__
// Workaround for Mac
#include <mach/clock.h>
#include <mach/mach.h>

#define CLOCK_REALTIME 0

static inline int clock_gettime( int clk_id, struct timespec *tp ) {
	clock_serv_t cclock;
	mach_timespec_t mts;
	host_get_clock_service( mach_host_self(), CALENDAR_CLOCK, &cclock );
	clock_get_time( cclock, &mts );
	mach_port_deallocate( mach_task_self(), cclock );
	tp->tv_sec = mts.tv_sec;
	tp->tv_nsec = mts.tv_nsec;
	return 0;
}

#endif

#define start_timer() ( { \
	struct timespec ts; \
	clock_gettime( CLOCK_REALTIME, &ts ); \
	ts; \
} )

#define get_timer() start_timer()

#define get_elapsed_time(start_time) ( { \
	struct timespec end_time = get_timer(); \
	double elapsed_time = end_time.tv_sec - start_time.tv_sec; \
	elapsed_time += 1.0e-9 * ( end_time.tv_nsec - start_time.tv_nsec ); \
	elapsed_time; \
} )

#endif