#ifndef __COMMON_DS_SOCKADDR_IN_HH__
#define __COMMON_DS_SOCKADDR_IN_HH__

#include <netinet/in.h>

bool operator==( const struct sockaddr_in &lhs, const struct sockaddr_in &rhs );
bool operator<( const struct sockaddr_in &lhs, const struct sockaddr_in &rhs );

#endif
