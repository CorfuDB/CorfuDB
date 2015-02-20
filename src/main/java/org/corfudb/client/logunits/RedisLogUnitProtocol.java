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

package org.corfudb.client.logunits;

import org.corfudb.client.IServerProtocol;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Response;

import java.util.Map;
import java.util.ArrayList;
import java.util.List;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.corfudb.client.NetworkException;
import org.corfudb.client.UnwrittenException;
import org.corfudb.client.TrimmedException;
import org.corfudb.client.OverwriteException;

public class RedisLogUnitProtocol implements IServerProtocol, IWriteOnceLogUnit
{
    private String host;
    private Integer port;
    private Map<String,String> options;
    private Long epoch;
    private JedisPool pool;

    private Logger log = LoggerFactory.getLogger(RedisLogUnitProtocol.class);

     public static String getProtocolString()
    {
        return "redislu";
    }

    public Integer getPort()
    {
        return port;
    }

    public String getHost()
    {
        return host;
    }

    public Map<String,String> getOptions()
    {
        return options;
    }

    public static IServerProtocol protocolFactory(String host, Integer port, Map<String,String> options, Long epoch)
    {
        return new RedisLogUnitProtocol(host, port, options, epoch);
    }

    public RedisLogUnitProtocol(String host, Integer port, Map<String,String> options, Long epoch)
    {
        this.host = host;
        this.port = port;
        this.options = options;
        this.epoch = epoch;

        try
        {
            this.pool = new JedisPool(host, port);
        }
        catch (Exception ex)
        {
            log.warn("Failed to connect to endpoint " + getFullString());
            throw new RuntimeException("Failed to connect to endpoint");
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

    @SuppressWarnings("unchecked")
    public void write(long address, byte[] data)
    throws OverwriteException, TrimmedException, NetworkException
    {
        try (BinaryJedis jedis = pool.getResource())
        {
           String script = "if redis.call('GET', KEYS[1]) == ARGV[1] then if redis.call('SETNX', KEYS[2], ARGV[2]) == 1 then return 0 else return 1 end else return 2 end";
           List<byte[]> keys = new ArrayList<byte[]>();
           keys.add(options.get("meta").getBytes());
           keys.add((options.get("prefix")+ "_" + address).getBytes());
           List<byte[]> args = new ArrayList<byte[]>();
           args.add(epoch.toString().getBytes());
           args.add(data);

           Long responses = (Long) jedis.eval(script.getBytes(), keys, args);
           if (responses == 1)
           {
                throw new OverwriteException("Key already exists!", address);
           }
           if (responses ==2)
           {
               throw new NetworkException("Server is in the wrong epoch!", this);
           }
        }
    }

    public byte[] read(long address) throws UnwrittenException, TrimmedException, NetworkException
    {
        try (BinaryJedis jedis = pool.getResource())
        {
            Transaction t = jedis.multi();
            Response<byte[]> repoch = t.get(options.get("meta").getBytes());
            Response<byte[]> rdata = t.get((options.get("prefix")+ "_" + address).getBytes());
            t.exec();
            long epoch = Long.parseLong(new String(repoch.get()));
            byte[] data = rdata.get();
            if (data == null) { throw new UnwrittenException("Data not present", address);}
            return data;
        }
    }

    public void trim(long address) throws NetworkException
    {
        return;
    }

    public void setEpoch(long epoch)
    {
        Long epochn = epoch;
        try (BinaryJedis jedis = pool.getResource())
        {
           String script = "local key=redis.call('GET', KEYS[1]) if key==false then redis.call('SET', KEYS[1], ARGV[1]) return 0 elseif key == ARGV[1] then return 1 elseif key < ARGV[1] then redis.call('SET', KEYS[1], ARGV[1]) return 2 else return 3 end";
            List<byte[]> keys = new ArrayList<byte[]>();
           keys.add(options.get("meta").getBytes());
           List<byte[]> args = new ArrayList<byte[]>();
           args.add(epochn.toString().getBytes());

           Long responses = (Long) jedis.eval(script.getBytes(), keys, args);
        }

    }

}
