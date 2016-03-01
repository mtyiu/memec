#ifndef __CLIENT_WORKER_WORKER_ROLE_HH__
#define __CLIENT_WORKER_WORKER_ROLE_HH__

enum WorkerRole {
	WORKER_ROLE_UNDEFINED,
	WORKER_ROLE_MIXED,
	WORKER_ROLE_APPLICATION,
	WORKER_ROLE_COORDINATOR,
	WORKER_ROLE_CLIENT,
	WORKER_ROLE_SERVER
};

#endif
