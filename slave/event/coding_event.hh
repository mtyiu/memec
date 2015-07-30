#ifndef __SLAVE_EVENT_CODING_EVENT_HH__
#define __SLAVE_EVENT_CODING_EVENT_HH__

#include "../../common/ds/stripe.hh"
#include "../../common/event/event.hh"

enum CodingEventType {
	CODING_EVENT_TYPE_UNDEFINED,
	CODING_EVENT_TYPE_ENCODE
};

class CodingEvent : public Event {
public:
	CodingEventType type;
	union {
		Stripe *stripe;
	} message;

	void encode( Stripe *stripe );
};

#endif
