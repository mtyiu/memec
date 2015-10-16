#ifndef __COMMON_LOCK_LOCK_HH__
#define __COMMON_LOCK_LOCK_HH__

#include <pthread.h>

#ifdef LOCK_T
#undef LOCK_T
#endif

#ifdef LOCK
#undef LOCK
#endif

#ifdef LOCK_INIT
#undef LOCK_INIT
#endif

#ifdef UNLOCK
#undef UNLOCK
#endif

#define LOCK_T               pthread_mutex_t
#define LOCK_INIT( l )       pthread_mutex_init( l, 0 )
#define LOCK( l )            pthread_mutex_lock( l )
#define UNLOCK( l )          pthread_mutex_unlock( l )

#endif
