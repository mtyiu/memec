#include <cstdio>
#include <cstdlib>
#include <getopt.h>
#include "application.hh"
#include "../../common/util/option.hh"

int main( int argc, char **argv ) {
	int opt, ret = 0;
	bool verbose = false;
	char *path = NULL, path_default[] = "bin/config/local";
	Application *application = 0;
	OptionList options;
	struct option_t tmpOption;
	static struct option long_options[] = {
		{ "path", required_argument, NULL, 'p' },
		{ "option", required_argument, NULL, 'o' },
		{ "help", no_argument, NULL, 'h' },
		{ "verbose", no_argument, NULL, 'v' },
		{ 0, 0, 0, 0 }
	};

	//////////////////////////////////
	// Parse command-line arguments //
	//////////////////////////////////
	opterr = 0;
	while( ( opt = getopt_long( argc, argv,
	                            "p:o:hv",
	                            long_options, NULL ) ) != -1 ) {
		switch( opt ) {
			case 'p':
				path = optarg;
				break;
			case 'o':
				tmpOption.section = 0;
				tmpOption.name = 0;
				tmpOption.value = 0;
				for ( int i = optind - 1, j = 0; i < argc && argv[ i ][ 0 ] != '-'; i++, j++ ) {
					switch( j ) {
						case 0: tmpOption.section = argv[ i ]; break;
						case 1: tmpOption.name = argv[ i ]; break;
						case 2: tmpOption.value = argv[ i ]; break;
					}
				}
				if ( tmpOption.value )
					options.push_back( tmpOption );
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

	for ( int i = 0, size = options.size(); i < size; i++ ) {
		printf( "%s, %s, %s\n", options[ i ].section, options[ i ].name, options[ i ].value );
	}

	/////////////////////////////////////
	// Pass control to the Application //
	/////////////////////////////////////
	application = Application::getInstance();
	if ( ! application->init( path, options, verbose ) ) {
		fprintf( stderr, "Error: Cannot initialize application.\n" );
		return 1;
	}
	if ( ! application->start() ) {
		fprintf( stderr, "Error: Cannot start application.\n" );
		return 1;
	}
	application->interactive();
	application->stop();

	return 0;

usage:
	fprintf(
		stderr,
		"Usage: %s [OPTION]...\n"
		"Start an application.\n\n"
		"Mandatory arguments to long options are mandatory for short "
		"options too.\n"
		"  -p, --path         Specify the path to the directory containing the config files\n"
		"  -o, --option       Override the options in the config file of application\n"
		"  -v, --verbose      Show configuration\n"
		"  -h, --help         Display this help and exit\n",
		argv[ 0 ]
	);
	
	return ret;
}
