#ifndef __APPLICATION_DS_PENDING_HH__
#define __APPLICATION_DS_PENDING_HH__

#include <set>
#include "../../common/ds/key.hh"

typedef struct {
	struct {
		std::set<Key> get;
		std::set<Key> set;
	} application;
	struct {
		std::set<Key> get;
		std::set<Key> set;
	} masters;
} Pending;

#endif
