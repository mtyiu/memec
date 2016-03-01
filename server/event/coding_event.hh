#ifndef __SLAVE_EVENT_CODING_EVENT_HH__
#define __SLAVE_EVENT_CODING_EVENT_HH__

#include "../../common/ds/bitmask_array.hh"
#include "../../common/ds/chunk.hh"
#include "../../common/event/event.hh"

enum CodingEventType {
	CODING_EVENT_TYPE_UNDEFINED,
	CODING_EVENT_TYPE_DECODE
};

class CodingEvent : public Event {
public:
	CodingEventType type;
	union {
		struct {
			Chunk **chunks;
			BitmaskArray *status;
		} decode;
	} message;

	void decode( Chunk **chunks, BitmaskArray *status );
};

#endif