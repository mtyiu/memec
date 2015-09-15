#ifndef __COMMON_REMAP_REMAP_STATUS_HH__
#define __COMMON_REMAP_REMAP_STATUS_HH__

enum RemapStatus {
    REMAP_UNDEFINED,        // 0
    REMAP_NONE,
    REMAP_PREPARE_START,
    REMAP_WAIT_START,
    REMAP_START,
    REMAP_PREPARE_END,      // 5 
    REMAP_WAIT_END,
    REMAP_END
};

#endif