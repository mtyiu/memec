#include <cstdlib>
#include <sys/stat.h>
#include "server_config.hh"

ServerConfig::ServerConfig() {
}

bool ServerConfig::merge( GlobalConfig &globalConfig ) {
	return true;
}

bool ServerConfig::parse( const char *path ) {
	return Config::parse( path, "server.ini" );
}

bool ServerConfig::override( OptionList &options ) {
	bool ret = true;
	for ( int i = 0, size = options.size(); i < size; i++ ) {
		ret &= this->set(
			options[ i ].section,
			options[ i ].name,
			options[ i ].value
		);
	}
	return ret;
}

bool ServerConfig::set( const char *section, const char *name, const char *value ) {
	if ( match( section, "server" ) ) {
		return this->server.addr.parse( name, value );
	} else if ( match( section, "pool" ) ) {
		if ( match( name, "chunks" ) )
			this->pool.chunks = atoll( value );
		else
			return false;
	} else if ( match( section, "buffer" ) ) {
		if ( match( name, "chunks_per_list" ) )
			this->buffer.chunksPerList = atoi( value );
		else
			return false;
	} else if ( match( section, "seal" ) ) {
		if ( match( name, "disabled" ) )
			this->seal.disabled = match( value, "true" );
		else
			return false;
	} else if ( match( section, "storage" ) ) {
		if ( match( name, "type" ) ) {
			if ( match( value, "local" ) )
				this->storage.type = STORAGE_TYPE_LOCAL;
			else
				this->storage.type = STORAGE_TYPE_UNDEFINED;
		} else if ( match( name, "path" ) ) {
			if ( strlen( value ) >= STORAGE_PATH_MAX )
				return false;
			strncpy( this->storage.path, value, STORAGE_PATH_MAX );
		} else {
			return false;
		}
	} else {
		return false;
	}
	return true;
}

bool ServerConfig::validate() {
	if ( ! this->server.addr.isInitialized() )
		CFG_PARSE_ERROR( "ServerConfig", "The server is not assigned with an valid address." );

	if ( this->pool.chunks < 1 )
		CFG_PARSE_ERROR( "ServerConfig", "The size of chunk pool should be greater than 0." );

	if ( this->buffer.chunksPerList < 1 )
		CFG_PARSE_ERROR( "ServerConfig", "The number of temporary chunks per stripe list should be at least 1." );

	if ( this->storage.type == STORAGE_TYPE_UNDEFINED ) {
		CFG_PARSE_ERROR( "ServerConfig", "The specified storage type is invalid." );
	} else if ( this->storage.type == STORAGE_TYPE_LOCAL ) {
		struct stat st;
		if ( stat( this->storage.path, &st ) != 0 )
			CFG_PARSE_ERROR( "ServerConfig", "The specified storage path does not exist." );

		if ( ! S_ISDIR( st.st_mode ) )
			CFG_PARSE_ERROR( "ServerConfig", "The specified storage path is not a directory." );
	}

	return true;
}

int ServerConfig::validate( std::vector<ServerAddr> servers ) {
	if ( this->validate() ) {
		for ( int i = 0, len = servers.size(); i < len; i++ ) {
			if ( this->server.addr == servers[ i ] )
				return i;
		}
		__ERROR__( "ServerConfig", "validate", "The assigned address does not match with the global server list." );
	}
	return -1;
}

void ServerConfig::print( FILE *f ) {
	int width = 24;
	fprintf(
		f,
		"### Server Configuration ###\n"
		"- Server\n"
		"\t- %-*s : ",
		width, "Address"
	);
	this->server.addr.print( f );
	fprintf(
		f,
		"- Pool\n"
		"\t- %-*s : %lu\n"
		"- Buffer\n"
		"\t- %-*s : %u\n"
		"- Seal"
		"\t- %-*s : %s\n"
		"- Storage\n"
		"\t- %-*s : %s\n"
		"\t- %-*s : %s\n",
		width, "Chunks", this->pool.chunks,
		width, "Chunks per list", this->buffer.chunksPerList,
		width, "Disabled", this->seal.disabled ? "Yes" : "No",
		width, "Type", this->storage.type == STORAGE_TYPE_LOCAL ? "Local" : "Undefined",
		width, "Path", this->storage.path
	);
	fprintf( f, "\n" );
}
