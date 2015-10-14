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
 * Implementation with reference to redis-rb-cluster:
 * https://github.com/antirez/redis-rb-cluster/
 */

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

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

public class RedisClusterClient extends DB {

    private Vector<Jedis> jedis = new Vector<Jedis>();

    public static final String HOST_PROPERTY = "redis.host";
    public static final String PORT_PROPERTY = "redis.port";
    public static final String PASSWORD_PROPERTY = "redis.password";
    public static final String SERVER_COUNT = "redis.serverCount";

    public static final String INDEX_KEY = "_indices";

	private static final int[] crc16HashValues = {
		0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
        0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
        0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
        0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
        0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
        0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
        0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
        0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
        0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
        0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
        0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
        0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
        0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
        0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
        0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
        0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
        0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
        0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
        0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
        0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
        0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
        0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
        0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
        0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
        0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
        0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
        0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
        0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
        0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
        0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
        0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
        0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0
	};

	private int hashCrc16(String key) {
		int crc = 0;
		for (int i = 0; i < key.length(); i++ ) {
			crc = ( (crc<<8) & 0xffff ) ^ crc16HashValues[ ( (crc>>8)^key.charAt(i) ) & 0xff ];
		}
		return crc;
	}

	private int hashSlot(String key) {
		return hashCrc16(key) % jedis.size();
	}

	private Jedis getJedis(int index) {
		return jedis.elementAt(index);
	}

    public void init() throws DBException {
        Properties props = getProperties();
        int port, serverCount;

		String serverCountString = props.getProperty(SERVER_COUNT);
		if (serverCountString != null ) {
			serverCount = Integer.parseInt(serverCountString);
		} else {
			serverCount = 1;
		}

		// connect to each server on list
		for (int i = 0, count = 0; count < serverCount; i++) {
			String portString = props.getProperty(PORT_PROPERTY.concat(Integer.toString(count)));
			if (portString != null) {
				port = Integer.parseInt(portString);
			} else {
				port = Protocol.DEFAULT_PORT;
			}

			String host = props.getProperty(HOST_PROPERTY.concat(Integer.toString(count)));
			if (host == null && serverCount > 1) {
				if ( i > serverCount * 10 ) {
					throw new DBException("Not encough server info provided / server naming is too sparse!\n");
				}
				continue;
			}

			jedis.add(new Jedis(host, port));
			getJedis(count).connect();
			//System.out.format("Connected to server %d at %s:%d\n", count, host, port );

			String password = props.getProperty(PASSWORD_PROPERTY.concat(Integer.toString(count)));
			if (password != null) {
				getJedis(count).auth(password);
			}

			count++;
		}
    }

    public void cleanup() throws DBException {
		for (int i = 0; i < jedis.size(); i++)
			getJedis(i).disconnect();
		jedis.removeAllElements();
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
    public int read(String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) {
        if (fields == null) {
            StringByteIterator.putAllAsByteIterators(result, getJedis(hashSlot(key)).hgetAll(key));
        }
        else {
            String[] fieldArray = (String[])fields.toArray(new String[fields.size()]);
            List<String> values = getJedis(hashSlot(key)).hmget(key, fieldArray);

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
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
		//System.out.format("key %s hash to server %d\n", key, hashSlot(key));
        if (getJedis(hashSlot(key)).hmset(key, StringByteIterator.getStringMap(values)).equals("OK")) {
            getJedis(hashSlot(key)).zadd(INDEX_KEY, hash(key), key);
            return 0;
        }
        return 1;
    }

    @Override
    public int delete(String table, String key) {
        return getJedis(hashSlot(key)).del(key) == 0
            && getJedis(hashSlot(key)).zrem(INDEX_KEY, key) == 0
               ? 1 : 0;
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        return getJedis(hashSlot(key)).hmset(key, StringByteIterator.getStringMap(values)).equals("OK") ? 0 : 1;
    }

    @Override
	/* NOT SUPPORTED */
    public int scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        //Set<String> keys = getJedis(hashSlot(startkey)).zrangeByScore(INDEX_KEY, hash(startkey),
        //                        Double.POSITIVE_INFINITY, 0, recordcount);

        //HashMap<String, ByteIterator> values;
        //for (String key : keys) {
        //    values = new HashMap<String, ByteIterator>();
        //    read(table, key, fields, values);
        //    result.add(values);
        //}
        return -1;
    }

}
