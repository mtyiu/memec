#include "master_event.hh"

void MasterEvent::resRegister( MasterSocket *socket, uint32_t id, bool success ) {
	this->type = success ? MASTER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
	this->id = id;
	this->socket = socket;
}

void MasterEvent::reqPushLoadStats( MasterSocket *socket, ArrayMap<struct sockaddr_in, Latency> *slaveGetLatency, 
		ArrayMap<struct sockaddr_in, Latency> *slaveSetLatency, std::set<struct sockaddr_in> *overloadedSlaveSet ) {
	this->type = MASTER_EVENT_TYPE_PUSH_LOADING_STATS;
	this->socket = socket;
	this->message.slaveLoading.slaveGetLatency = slaveGetLatency;
	this->message.slaveLoading.slaveSetLatency = slaveSetLatency;
	this->message.slaveLoading.overloadedSlaveSet = overloadedSlaveSet;
}

void MasterEvent::switchPhase( bool toRemap ) {
	this->type = MASTER_EVENT_TYPE_SWITCH_PHASE;
	this->message.remap.toRemap = toRemap;
	// TODO specify the vector of slaves
	this->message.remap.slaves = NULL;
}

void MasterEvent::pending( MasterSocket *socket ) {
	this->type = MASTER_EVENT_TYPE_PENDING;
	this->socket = socket;
}
