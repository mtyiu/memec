package com.yahoo.ycsb.db;

import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import edu.cuhk.cse.memec.MemEC;

/**
 * MemEC YCSB client.
 *
 * MemEC YCSB client via the MemEC application interface in Java 
 */
public class MemECApplication extends DB {
  // Properties
  public static final String HOST_PROPERTY = "memec.host";
  public static final String PORT_PROPERTY = "memec.port";
  public static final String KEY_SIZE_PROPERTY = "memec.key_size";
  public static final String CHUNK_SIZE_PROPERTY = "memec.chunk_size";
  // Return values
  public static final int OK = 0;
  public static final int ERROR = -1;
  public static final int NOT_FOUND = -2;

  private MemEC memec;

  public void init() throws DBException {
    Properties props = getProperties();
    String host, s;
    int port, keySize, chunkSize;

    host = props.getProperty(HOST_PROPERTY);

    s = props.getProperty(PORT_PROPERTY);
    port = s != null ? Integer.parseInt(s) : MemEC.defaultPort;

    s = props.getProperty(KEY_SIZE_PROPERTY);
    keySize = s != null ? Integer.parseInt(s) : MemEC.defaultKeySize;

    s = props.getProperty(CHUNK_SIZE_PROPERTY);
    chunkSize = s != null ? Integer.parseInt(s) : MemEC.defaultChunkSize;

    int fromId = (int) (Math.random() * Integer.MAX_VALUE);
    int toId = (int) (Math.random() * Integer.MAX_VALUE);
    if (fromId > toId) {
      int tmp = fromId;
      fromId = toId;
      toId = tmp;
    }

    memec = new MemEC(keySize, chunkSize, host, port, fromId, toId);

    if (!memec.connect()) {
      throw new DBException();
    }
  }

  public void cleanup() throws DBException {
    memec.disconnect();
  }

  /* Calculate a hash for a key to store it in an index.  The actual return
  * value of this function is not interesting -- it primarily needs to be
  * fast and scattered along the whole space of doubles.  In a real world
  * scenario one would probably use the ASCII values of the keys.
  */
  private double hash(String key) {
    return key.hashCode();
  }

  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    Status ret = Status.OK;
    for (String f : fields) {
      String value = memec.get(table + ":" + key + ":" + f);
      if (value == null) {
        ret = Status.ERROR;
      }
      result.put(f, new StringByteIterator(value));
    }
    return ret;
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    Status ret = Status.OK;
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      if (!memec.set(table + ":" + key + ":" + entry.getKey(), entry.getValue().toString())) {
        ret = Status.ERROR;
      }
    }
    return ret;
  }

  @Override
  public Status delete(String table, String key) {
    return memec.delete(key) ? Status.OK : Status.ERROR;
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    Status ret = Status.OK;
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      if (!memec.update(table + ":" + key + ":" + entry.getKey(), entry.getValue().toString(), 0)) {
        ret = Status.ERROR;
      }
    }
    return ret;
  }

  @Override
  public Status scan(String table, String startKey, int recordCount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

}
