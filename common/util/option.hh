#ifndef __COMMON_UTIL_OPTION_HH__
#define __COMMON_UTIL_OPTION_HH__

#include <vector>

struct option_t {
	char *section;
	char *name;
	char *value;
};

typedef std::vector<struct option_t> OptionList;

#endif
