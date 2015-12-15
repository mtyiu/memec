#ifndef __BENCHMARK_HUAWEI_TIME_HH__
#define __BENCHMARK_HUAWEI_TIME_HH__

#include <stdint.h>
#include <signal.h>
#include <time.h>

#define MILLION	( 1000 * 1000 )

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

#define get_elapsed_time_in_us(start_time) ( { \
	struct timespec end_time = get_timer(); \
	double elapsed_time = ( end_time.tv_sec - start_time.tv_sec ) * 1e6; \
	elapsed_time += 1.0e-3 * ( end_time.tv_nsec - start_time.tv_nsec ); \
	elapsed_time; \
} )

class Timer {
private:
	struct itimerspec timer;
	struct itimerspec dummy;
	struct sigevent sigev;
	timer_t id;

	void setInterval( uint32_t sec, uint32_t msec, struct itimerspec *target ) {
		target->it_value.tv_sec = sec;
		target->it_value.tv_nsec = msec * MILLION;
		target->it_interval.tv_sec = sec;
		target->it_interval.tv_nsec = msec * MILLION;
	}

public:
	Timer( uint32_t sec = 0, uint32_t msec = 0 ) {
		this->setInterval( sec, msec );
		this->setInterval( 0, 0, &this->dummy );
		this->sigev.sigev_notify = SIGEV_SIGNAL;
		this->sigev.sigev_signo = SIGALRM;
		this->sigev.sigev_value.sival_ptr = &this->id;
		timer_create( CLOCK_REALTIME, &this->sigev, &this->id );
	}

	void setInterval( uint32_t sec, uint32_t msec ) {
		this->setInterval( sec, msec, &this->timer );
	}


	int start() {
		return timer_settime( this->id, 0, &this->timer, NULL );
	}

	int stop() {
		return timer_settime( this->id, 0, &this->dummy, NULL );
	}
};

#endif
