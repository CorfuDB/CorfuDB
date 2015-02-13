/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.corfudb.client.sequencers;

import org.corfudb.client.IServerProtocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisSequencerProtocol implements IServerProtocol, ISimpleSequencer
{
    private String host;
    private String port;
    private String fullString;
    private String key;

    private JedisPool pool;

    private Logger log = LoggerFactory.getLogger(RedisSequencerProtocol.class);

    public static String getProtocolString()
    {
        return "redisseq";
    }

    public static IServerProtocol protocolFactory(String host, String port, String fullString)
    {
        String[] options = fullString.split(",");
        String key = null;
        for (String kvpair: options)
        {
            String[] pairsplit = kvpair.split("=");
            if (pairsplit.length > 1)
            {
                String K = pairsplit[0];
                String V = pairsplit[1];
                if (K.equals("key"))
                {
                    key =V;
                }
            }
        }
        if (key == null) { throw new RuntimeException("Key not specified!"); }
        return new RedisSequencerProtocol(host, port, fullString, key);
    }

    private RedisSequencerProtocol(String host, String port, String fullString, String key)
    {
        this.host = host;
        this.port = port;
        this.fullString = fullString;
        this.key = key;

        try
        {
            this.pool = new JedisPool(host, Integer.parseInt(port));
        }
        catch (Exception ex)
        {
            log.warn("Failed to connect to endpoint " + fullString);
            throw new RuntimeException("Failed to connect to endpoint");
        }
    }

    public String getFullString()
    {
        return getProtocolString() + "://" + host + ":" + port + ",key=" + key;
    }

    public long sequenceGetNext()
    throws Exception
    {
        try (Jedis jedis = pool.getResource())
        {
            return jedis.incr(key);
        }
    }

    public boolean ping()
    {
        try (Jedis jedis = pool.getResource())
        {
            if (jedis.ping().equals("PONG"))
            {
                return true;
            }
            return false;
        }
        catch (Exception e)
        {
            return false;
        }
    }
}


