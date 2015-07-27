#ifndef __COMMON_UTIL_OPTION_HH__
#define __COMMON_UTIL_OPTION_HH__

struct option_t {
	char *section;
	char *name;
	char *value;
};

typedef std::vector<struct option_t> OptionList;

#endif
