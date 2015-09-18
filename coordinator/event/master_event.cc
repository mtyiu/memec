#include "master_event.hh"

void MasterEvent::resRegister( MasterSocket *socket, uint32_t id, bool success ) {
	this->type = success ? MASTER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
	this->id = id;
	this->socket = socket;
}

void MasterEvent::reqPushLoadStats( MasterSocket *socket, ArrayMap<struct sockaddr_in, Latency> *slaveGetLatency, 
		ArrayMap<struct sockaddr_in, Latency> *slaveSetLatency ) {
	this->type = MASTER_EVENT_TYPE_PUSH_LOADING_STATS;
	this->socket = socket;
	this->message.slaveLoading.slaveGetLatency = slaveGetLatency;
	this->message.slaveLoading.slaveSetLatency = slaveSetLatency;
}

void MasterEvent::pending( MasterSocket *socket ) {
	this->type = MASTER_EVENT_TYPE_PENDING;
	this->socket = socket;
}
