#ifndef __COMMON_EVENT_EVENT_HH__
#define __COMMON_EVENT_EVENT_HH__

template <class T> class Event {
public:
	uint16_t instanceId;
	uint32_t requestId;
	T *socket;

	inline void set( uint16_t instanceId = 0, uint32_t requestId = 0, T *socket = 0 ) {
		this->instanceId = instanceId;
		this->requestId = requestId;
		this->socket = socket;
	}
};

#endif
