#ifndef __APPLICATION_EVENT_APPLICATION_EVENT_HH__
#define __APPLICATION_EVENT_APPLICATION_EVENT_HH__

#include "../../common/event/event.hh"

enum ApplicationEventType {
	APPLICATION_EVENT_TYPE_UNDEFINED
};

class ApplicationEvent : public Event {
public:
	ApplicationEventType type;
};

#endif
