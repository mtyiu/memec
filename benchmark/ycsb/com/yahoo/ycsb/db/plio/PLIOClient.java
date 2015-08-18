package com.yahoo.ycsb.db;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

public class PLIOClient extends DB {
   // Properties
	public static final String HOST_PROPERTY = "plio.host";
	public static final String PORT_PROPERTY = "plio.port";
	public static final String KEY_SIZE_PROPERTY = "plio.key_size";
	public static final String CHUNK_SIZE_PROPERTY = "plio.chunk_size";
   // Return values
   public static final int OK = 0;
   public static final int ERROR = -1;
   public static final int NOT_FOUND = -2;

	public void init() throws DBException {
		Properties props = getProperties();
		String host, portString;
		int port;

		host = props.getProperty( HOST_PROPERTY );

		portString = props.getProperty( PORT_PROPERTY );
		port = portString != null ? Integer.parseInt( portString ) : PLIO.DEFAULT_PORT;
	}

	public void cleanup() throws DBException {
		jedis.disconnect();
	}

	/* Calculate a hash for a key to store it in an index.  The actual return
	* value of this function is not interesting -- it primarily needs to be
	* fast and scattered along the whole space of doubles.  In a real world
	* scenario one would probably use the ASCII values of the keys.
	*/
	private double hash( String key ) {
		return key.hashCode();
	}

	@Override
	public int read( String table, String key, Set<String> fields, HashMap<String, ByteIterator> result ) {
		if (fields == null) {
			StringByteIterator.putAllAsByteIterators(result, jedis.hgetAll(key));
		}
		else {
			String[] fieldArray = (String[])fields.toArray(new String[fields.size()]);
			List<String> values = jedis.hmget(key, fieldArray);

			Iterator<String> fieldIterator = fields.iterator();
			Iterator<String> valueIterator = values.iterator();

			while (fieldIterator.hasNext() && valueIterator.hasNext()) {
				result.put(fieldIterator.next(),
				new StringByteIterator(valueIterator.next()));
			}
			assert !fieldIterator.hasNext() && !valueIterator.hasNext();
		}
		return result.isEmpty() ? 1 : 0;
	}

	@Override
	public int insert( String table, String key, HashMap<String, ByteIterator> values ) {
		if (jedis.hmset(key, StringByteIterator.getStringMap(values)).equals("OK")) {
			jedis.zadd(INDEX_KEY, hash(key), key);
			return 0;
		}
		return 1;
	}

	@Override
	public int delete( String table, String key ) {
		return jedis.del(key) == 0
		&& jedis.zrem(INDEX_KEY, key) == 0
		? 1 : 0;
	}

	@Override
	public int update( String table, String key, HashMap<String, ByteIterator> values ) {
		return jedis.hmset(key, StringByteIterator.getStringMap(values)).equals("OK") ? 0 : 1;
	}

	@Override
	public int scan( String table, String startKey, int recordCount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result ) {
		Set<String> keys = jedis.zrangeByScore(INDEX_KEY, hash(startkey),
		Double.POSITIVE_INFINITY, 0, recordcount);

		HashMap<String, ByteIterator> values;
		for (String key : keys) {
			values = new HashMap<String, ByteIterator>();
			read(table, key, fields, values);
			result.add(values);
		}

		return 0;
	}

}
