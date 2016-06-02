#include "state_transit_state.hh"

bool printServerState( RemapState state, char *mod, char *func, char *ip, short port ) {
	char *stateString = 0;
	char *unknown = ( char* ) "Unknown";
	bool valid = true;
	switch( state ) {
		case STATE_NORMAL:
			stateString = ( char* ) "STATE_NORMAL";
			break;
		case STATE_INTERMEDIATE:
			stateString = ( char* ) "STATE_INTERMEDIATE";
			break;
		case STATE_COORDINATED:
			stateString = ( char* ) "STATE_COORDINATED";
			break;
		case STATE_DEGRADED:
			stateString = ( char* ) "STATE_DEGRADED";
			break;
		case STATE_WAIT_DEGRADED:
			stateString = ( char* ) "STATE_WAIT_DEGRADED";
			break;
		case STATE_WAIT_NORMAL:
			stateString = ( char* ) "STATE_WAIT_NORMAL";
			break;
		case STATE_UNDEFINED:
			stateString = ( char* ) "STATE_UNDEFINED";
			break;
		default:	
			stateString = ( char* ) "STATE_UNKNOWN";
			valid = false;
			break;
	}
	if ( mod == 0 ) mod = unknown;
	if ( func == 0 ) func = unknown;
	if ( ip == 0 ) ip = unknown;
	__INFO__( BLUE, mod, func, "%s %s:%hu", stateString, ip, port );
	return valid;
}

