#include "sockaddr_in.hh"

bool operator==( const struct sockaddr_in &lhs, const struct sockaddr_in &rhs ) {
	return (
		lhs.sin_port == rhs.sin_port &&
		lhs.sin_addr.s_addr == rhs.sin_addr.s_addr
	);
}
