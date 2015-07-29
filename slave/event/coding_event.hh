#ifndef __SLAVE_EVENT_CODING_EVENT_HH__
#define __SLAVE_EVENT_CODING_EVENT_HH__

#include "../../common/event/event.hh"

enum CodingEventType {
	CODING_EVENT_TYPE_UNDEFINED
};

class CodingEvent : public Event {
public:
	CodingEventType type;
};

#endif
