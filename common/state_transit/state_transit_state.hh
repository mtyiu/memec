#ifndef __COMMON_STATE_TRANSIT_STATE_TRANSIT_STATE_HH__
#define __COMMON_STATE_TRANSIT_STATE_TRANSIT_STATE_HH__

#include "../util/debug.hh"

enum RemapState {
	// core states
	STATE_UNDEFINED,				// undefined
	STATE_NORMAL,					// normal (phase 0)
	STATE_INTERMEDIATE,				// intermediate (phase 1a)
	STATE_COORDINATED,				// coordinated (phase 1b)
	STATE_DEGRADED,					// degraded (phase 2)
	// client states (wait for all)
	STATE_WAIT_DEGRADED,			// wait for return to degraded
	STATE_WAIT_NORMAL				// wait for return to normal
};

// return true if the state are listed in RemapState, otherwise, return false
bool printServerState( RemapState state, char *mod = 0, char *func = 0, char *ip = 0, short port = 0 );
#endif
