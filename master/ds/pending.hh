#ifndef __MASTER_DS_PENDING_HH__
#define __MASTER_DS_PENDING_HH__

#include <set>
#include "../../common/ds/key.hh"

typedef struct {
	struct {
		std::set<Key> get;
		std::set<Key> set;
	} applications;
	struct {
		std::set<Key> get;
		std::set<Key> set;
	} slaves;
} Pending;

#endif
