#ifndef __SERVER_ADDR_HH__
#define __SERVER_ADDR_HH__

class ServerAddr {
public:
	char *name;
	unsigned long addr;
	unsigned short port;
	int type;

	ServerAddr();
	bool parse( const char *name, const char *addr );
	ServerAddr( const char *, unsigned long, unsigned short, int );
	void print();
	~ServerAddr();
};

#endif
