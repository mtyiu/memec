#ifndef __COMMON_REMAP_REMAP_STATUS_HH__
#define __COMMON_REMAP_REMAP_STATUS_HH__

enum RemapStatus {
	REMAP_UNDEFINED,				// 0, undefined
	REMAP_NONE,						// no remapping (phase 0)
	REMAP_PREPARE_START,			// start get locks (phase 1)
	REMAP_WAIT_START,				// all ops get locks (can enter phase 2)
	REMAP_START,					// all ops get locks, meta sync (phase 3)
	REMAP_PREPARE_END,				// 5, no more remap (back to phase 2)
	REMAP_WAIT_END,					// not remapping (back to phase 1)
	REMAP_END						// signal to go back to no remapping (phase 0)
};

#endif
