#include "coding_event.hh"

void CodingEvent::encode( Stripe *stripe ) {
	this->type = CODING_EVENT_TYPE_ENCODE;
	this->message.stripe = stripe;
}
