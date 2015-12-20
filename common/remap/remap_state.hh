#ifndef __COMMON_REMAP_REMAP_STATE_HH__
#define __COMMON_REMAP_REMAP_STATE_HH__

enum RemapState {
	// core states
	REMAP_UNDEFINED,				// undefined
	REMAP_NORMAL,					// no remapping (phase 0)
	REMAP_INTERMEDIATE,				// intermediate (phase 1a)
	REMAP_COORDINATED,				// coordinated (phase 1b)
	REMAP_DEGRADED,					// degraded (phase 2)
	// master states (wait for all)
	REMAP_WAIT_DEGRADED,			// wait for retruen to degraded
	REMAP_WAIT_NORMAL				// wait for retruen to normal 
};

enum RequestRemapState{
	NO_REMAP,
	DATA_REMAP,
	PARITY_REMAP,
	MIXED_REMAP
};

#endif
