#ifndef __COMMON_PROTOCOL_MAGIC_HH__
#define __COMMON_PROTOCOL_MAGIC_HH__

/************************
 *  Magic byte (1 byte) *
 ************************/
// (Bit: 0-2) //
#define PROTO_MAGIC_HEARTBEAT                     0x00 // -----000
#define PROTO_MAGIC_REQUEST                       0x01 // -----001
#define PROTO_MAGIC_RESPONSE_SUCCESS              0x02 // -----010
#define PROTO_MAGIC_RESPONSE_FAILURE              0x03 // -----011
#define PROTO_MAGIC_ANNOUNCEMENT                  0x04 // -----100
#define PROTO_MAGIC_LOADING_STATS                 0x05 // -----101
#define PROTO_MAGIC_REMAPPING                     0x06 // -----110
#define PROTO_MAGIC_ACKNOWLEDGEMENT               0x07 // -----111
// (Bit: 3-4) //
#define PROTO_MAGIC_FROM_APPLICATION              0x00 // ---00---
#define PROTO_MAGIC_FROM_COORDINATOR              0x08 // ---01---
#define PROTO_MAGIC_FROM_CLIENT                   0x10 // ---10---
#define PROTO_MAGIC_FROM_SERVER                   0x18 // ---11---
 // (Bit: 5-6) //
#define PROTO_MAGIC_TO_APPLICATION                0x00 // -00-----
#define PROTO_MAGIC_TO_COORDINATOR                0x20 // -01-----
#define PROTO_MAGIC_TO_CLIENT                     0x40 // -10-----
#define PROTO_MAGIC_TO_SERVER                     0x60 // -11-----
// (Bit: 7): Reserved //

#endif
