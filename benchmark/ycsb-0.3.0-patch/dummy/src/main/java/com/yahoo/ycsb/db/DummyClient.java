package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.ByteIterator;

public class DummyClient extends DB {
	public static final int OK = 0;

	@Override
	public int read( String table, String key, Set<String> fields, HashMap<String, ByteIterator> result ) {
		return OK;
	}

	@Override
	public int insert( String table, String key, HashMap<String, ByteIterator> values ) {
		return OK;
	}

	@Override
	public int delete( String table, String key ) {
		return OK;
	}

	@Override
	public int update( String table, String key, HashMap<String, ByteIterator> values ) {
		return OK;
	}

	@Override
	public int scan( String table, String startKey, int recordCount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result ) {
		return OK;
	}
}
