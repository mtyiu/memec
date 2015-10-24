// Jacob Leverich <leverich@stanford.edu>, 2011
// Memcached client for YCSB framework.
//
// (Extended version for memcached cluster in 2015)
//
// Properties:
//   memcached.server=memcached.xyz.com
//   memcached.port=11211

package com.yahoo.ycsb.db;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ExecutionException;

import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.KetamaConnectionFactory;
import net.spy.memcached.internal.OperationFuture;

public class MemcachedCluster extends com.yahoo.ycsb.DB
{
  MemcachedClient client;
  Properties props;

  public static final int OK = 0;
  public static final int ERROR = -1;
  public static final int NOT_FOUND = -2;

  public static final String MEMCACHED_SERVER = "memcached.server";
  public static final String MEMCACHED_PORT = "memcached.port";
  public static final String MEMCACHED_COUNT = "memcached.serverCount";

  /**
   * Initialize any state for this DB.  Called once per DB instance;
   * there is one DB instance per client thread.
   */
  public void init() throws DBException {
    Vector<InetSocketAddress> servers = new Vector<InetSocketAddress>();
    int serverCount;

    props = getProperties();
    String serverCountString = props.getProperty(MEMCACHED_COUNT);
    if (serverCountString == null) {
      serverCount = 1;
    } else {
      serverCount = Integer.parseInt(serverCountString);
    }

    // construct the list of servers
    for (int i = 0, count = 0; count < serverCount; i++ ) {
      String server = props.getProperty(MEMCACHED_SERVER.concat(Integer.toString(count)));
      int port = 11211;

      if (server == null) {
        if (i > serverCount * 10)
          throw new DBException("memcached.server param must be specified");
        continue;
      }

      try { port = Integer.parseInt(props.getProperty(MEMCACHED_PORT.concat(Integer.toString(count)))); }
      catch (Exception e) {}
      servers.add(new InetSocketAddress(server, port));
      count++;
  }

  // init client using MD5 as the hashing function
    try {
      client = new MemcachedClient(new KetamaConnectionFactory(), servers);
    } catch (IOException e) { throw new DBException(e); }
  }

  /**
   * Cleanup any state for this DB.  Called once per DB instance;
   * there is one DB instance per client thread.
   */
  public void cleanup() throws DBException
  {
    client.shutdown();
  }

  /**
   * Read a record from the database. Each field/value pair from the
   * result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  public int read(String table, String key, Set<String> fields,
                  HashMap<String,ByteIterator> result) {
    HashMap<String, byte[]> values =
      (HashMap<String, byte[]>) client.get(table + ":" + key);

    if (values == null) return NOT_FOUND;
    if (values.keySet().isEmpty()) return NOT_FOUND;
    if (fields == null) fields = values.keySet();

    for (String k: fields) {
      byte[] v = values.get(k);
      if (v == null) return NOT_FOUND;
      result.put(k, new ByteArrayByteIterator(v));
    }

    return OK;
  }

  /**
   * Perform a range scan for a set of records in the database. Each
   * field/value pair from the result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields The list of fields to read, or null for all of them
   * @param result A Vector of HashMaps, where each HashMap is a set
   * field/value pairs for one record
   * @return Zero on success, a non-zero error code on error.  See
   * this class's description for a discussion of error codes.
   */
  public int scan(String table, String startkey, int recordcount,
                  Set<String> fields,
                  Vector<HashMap<String,ByteIterator>> result) {
    return ERROR;
  }

  /**
   * Update a record in the database. Any field/value pairs in the
   * specified values HashMap will be written into the record with the
   * specified record key, overwriting any existing values with the
   * same field name.
   *
   * @param table The name of the table
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error.  See
   * this class's description for a discussion of error codes.
   */
  public int update(String table, String key,
                    HashMap<String,ByteIterator> values) {
    HashMap<String, byte[]> new_values = new HashMap<String, byte[]>();

    for (String k: values.keySet()) {
      new_values.put(k, values.get(k).toArray());
    }

    OperationFuture<Boolean> f =
      client.set(table + ":" + key, 3600, new_values);

    try { return f.get() ? OK : ERROR; }
    catch (InterruptedException e) { return ERROR; }
    catch (ExecutionException e) { return ERROR; }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the
   * specified values HashMap will be written into the record with the
   * specified record key.
   *
   * @param table The name of the table
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the
   * record
   * @return Zero on success, a non-zero error code on error.  See
   * this class's description for a discussion of error codes.
   */
  public int insert(String table, String key,
                    HashMap<String,ByteIterator> values) {
    return update(table, key, values);
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error.  See
   * this class's description for a discussion of error codes.
   */
  public int delete(String table, String key) {
    client.delete(table + ":" + key);
    return OK; // FIXME check future
  }
}
