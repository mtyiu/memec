#include "storage.hh"
#include "allstorage.hh"

StorageType Storage::type;

Storage::~Storage() {}

Storage *Storage::instantiate( ServerConfig &config ) {
	Storage *ret;
	Storage::type = config.storage.type;
	switch( config.storage.type ) {
		case STORAGE_TYPE_LOCAL:
			ret = new LocalStorage();
			break;
		default:
			return 0;
	}
	ret->init( config );
	return ret;
}

void Storage::destroy( Storage *storage ) {
	switch( Storage::type ) {
		case STORAGE_TYPE_LOCAL:
			delete static_cast<LocalStorage *>( storage );
			break;
		default:
			return;
	}
}
