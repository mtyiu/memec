#include "coordinator_event.hh"

void CoordinatorEvent::reqRegister( CoordinatorSocket *socket, uint32_t addr, uint16_t port ) {
	this->type = COORDINATOR_EVENT_TYPE_REGISTER_REQUEST;
	this->socket = socket;
	this->message.address.addr = addr;
	this->message.address.port = port;
}

void CoordinatorEvent::reqSendLoadStats(
		CoordinatorSocket *socket,
		ArrayMap< struct sockaddr_in, Latency > *slaveGetLatency,
		ArrayMap< struct sockaddr_in, Latency > *slaveSetLatency ) 
{
	this->type = COORDINATOR_EVENT_TYPE_PUSH_LOAD_STATS;
	this->socket = socket;
	this->message.loading.slaveGetLatency = slaveGetLatency;
	this->message.loading.slaveSetLatency = slaveSetLatency;
}

void CoordinatorEvent::pending( CoordinatorSocket *socket ) {
	this->type = COORDINATOR_EVENT_TYPE_PENDING;
	this->socket = socket;
}
