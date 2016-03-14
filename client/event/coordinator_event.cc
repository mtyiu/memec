#include "coordinator_event.hh"

void CoordinatorEvent::reqRegister( CoordinatorSocket *socket, uint32_t addr, uint16_t port ) {
	this->type = COORDINATOR_EVENT_TYPE_REGISTER_REQUEST;
	this->socket = socket;
	this->message.address.addr = addr;
	this->message.address.port = port;
}

void CoordinatorEvent::reqSendLoadStats(
		CoordinatorSocket *socket,
		ArrayMap< struct sockaddr_in, Latency > *serverGetLatency,
		ArrayMap< struct sockaddr_in, Latency > *serverSetLatency )
{
	this->type = COORDINATOR_EVENT_TYPE_PUSH_LOAD_STATS;
	this->socket = socket;
	this->message.loading.serverGetLatency = serverGetLatency;
	this->message.loading.serverSetLatency = serverSetLatency;
}

void CoordinatorEvent::pending( CoordinatorSocket *socket ) {
	this->type = COORDINATOR_EVENT_TYPE_PENDING;
	this->socket = socket;
}
