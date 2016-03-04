/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/**
 * Redis cluster client binding for YCSB.
 *
 * Extension of the single-node redis client in YCSB
 */

package com.yahoo.ycsb.db;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;
import java.util.Vector;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.HostAndPort;

/**
 * RedisClusterClient for YCSB.
 *
 * Extension of the single-node redis client in YCSB
 */
public class RedisClusterClient extends DB {

  private JedisCluster jedis;

  public static final String HOST_PROPERTY = "redis.host";
  public static final String PORT_PROPERTY = "redis.port";
  public static final String SERVER_COUNT = "redis.serverCount";

  public static final String INDEX_KEY = "_indices";

  public void init() throws DBException {
    Properties props = getProperties();
    int port, serverCount;
    HashSet<HostAndPort> servers = new HashSet<HostAndPort>();

    String serverCountString = props.getProperty(SERVER_COUNT);
    if (serverCountString != null) {
      serverCount = Integer.parseInt(serverCountString);
    } else {
      serverCount = 1;
    }

    // connect to servers
    for (int i = 0, count = 0; count < serverCount; i++) {
      String portString = props.getProperty(PORT_PROPERTY.concat(Integer.toString(count)));
      if (portString != null) {
        port = Integer.parseInt(portString);
      } else {
        port = Protocol.DEFAULT_PORT;
      }

      String host = props.getProperty(HOST_PROPERTY.concat(Integer.toString(count)));
      if (host == null && serverCount > 1) {
        if (i > serverCount * 10) {
          throw new DBException("Not enough server info provided / server naming is too sparse!\n");
        }
        continue;
      }

      servers.add(new HostAndPort(host, port));
      //System.out.format("Added server %d at %s:%d\n", count, host, port );

      // TODO support password-protected cluster
      count++;
    }

    jedis = new JedisCluster(servers);
  }

  public void cleanup() throws DBException {
    jedis.close();
  }

  /* Calculate a hash for a key to store it in an index.  The actual return
   * value of this function is not interesting -- it primarily needs to be
   * fast and scattered along the whole space of doubles.  In a real world
   * scenario one would probably use the ASCII values of the keys.
   */
  private int hash(String key) {
    return key.hashCode();
  }

  //XXX jedis.select(int index) to switch to `table`

  @Override
  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    if (fields == null) {
      StringByteIterator.putAllAsByteIterators(result, jedis.hgetAll(key));
    } else {
      String[] fieldArray = (String[])fields.toArray(new String[fields.size()]);
      List<String> values = jedis.hmget(key, fieldArray);

      Iterator<String> fieldIterator = fields.iterator();
      Iterator<String> valueIterator = values.iterator();

      while (fieldIterator.hasNext() && valueIterator.hasNext()) {
        result.put(fieldIterator.next(), new StringByteIterator(valueIterator.next()));
      }
      assert !fieldIterator.hasNext() && !valueIterator.hasNext();
    }
    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    //System.out.format("key %s hash to server %d\n", key, hashSlot(key));
    if (jedis.hmset(key, StringByteIterator.getStringMap(values)).equals("OK")) {
      jedis.zadd(INDEX_KEY, hash(key), key);
      return Status.OK;
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    return jedis.del(key) == 0
      && jedis.zrem(INDEX_KEY, key) == 0
         ? Status.ERROR : Status.OK;
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    return jedis.hmset(key, StringByteIterator.getStringMap(values)).equals("OK") ? Status.OK : Status.NOT_FOUND;
  }


  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    Set<String> keys = jedis.zrangeByScore(INDEX_KEY, hash(startkey),
                Double.POSITIVE_INFINITY, 0, recordcount);
    HashMap<String, ByteIterator> values;
    for (String key : keys) {
      values = new HashMap<String, ByteIterator>();
      read(table, key, fields, values);
      result.add(values);
    }
    return Status.OK;
  }

}
