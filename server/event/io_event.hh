#ifndef __SERVER_EVENT_IO_EVENT_HH__
#define __SERVER_EVENT_IO_EVENT_HH__

#include "../../common/ds/chunk.hh"
#include "../../common/event/event.hh"

enum IOEventType {
	IO_EVENT_TYPE_FLUSH_CHUNK
};

class IOEvent : public Event {
public:
	IOEventType type;
	Chunk *chunk;
	bool clear;

	void flush( Chunk *chunk, bool clear = false );
};

#endif
