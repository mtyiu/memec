#include <cstdio>
#include <cstdlib>
#include <getopt.h>
#include "master.hh"

int main( int argc, char **argv ) {
	int opt, ret = 0;
	bool verbose = false;
	char *path = NULL, path_default[] = "bin/config/local";
	Master *master = 0;
	static struct option long_options[] = {
		{ "path", required_argument, NULL, 'p' },
		{ "help", no_argument, NULL, 'h' },
		{ "verbose", no_argument, NULL, 'v' },
		{ 0, 0, 0, 0 }
	};

	//////////////////////////////////
	// Parse command-line arguments //
	//////////////////////////////////
	opterr = 0;
	while( ( opt = getopt_long( argc, argv,
	                            "p:hv",
	                            long_options, NULL ) ) != -1 ) {
		switch( opt ) {
			case 'p':
				path = optarg;
				break;
			case 'h':
				ret = 0;
				goto usage;
			case 'v':
				verbose = true;
				break;
			default:
				goto usage;
		}
	}
	path = path == NULL ? path_default : path;

	////////////////////////////////
	// Pass control to the Master //
	////////////////////////////////
	master = Master::getInstance();
	if ( master->init( path, verbose ) ) {
		fprintf( stderr, "Error: Cannot initialize master.\n" );
		return 1;
	}

	return 0;

usage:
	fprintf(
		stderr,
		"Usage: %s [OPTION]...\n"
		"Start a coordinator.\n\n"
		"Mandatory arguments to long options are mandatory for short "
		"options too.\n"
		"  -p, --path         Specify the path to the directory containing the config files\n"
		"  -v, --verbose      Show configuration\n"
		"  -h, --help         Display this help and exit\n",
		argv[ 0 ]
	);
	
	return ret;
}
