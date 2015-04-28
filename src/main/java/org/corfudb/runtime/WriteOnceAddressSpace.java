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
package org.corfudb.runtime;

import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.UnwrittenException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.io.Serializable;


import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;

import org.corfudb.client.CorfuDBClient;
import org.corfudb.runtime.collections.*;
import org.corfudb.client.view.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the write-once address space providing storage for the shared log.
 */
public interface WriteOnceAddressSpace
{
    /**
     * Writes an entry at a particular position. Throws an exception if
     * the entry is already written to.
     *
     * @param pos
     * @param bs
     */
    void write(long pos, BufferStack bs); //todo: throw exception

    void write(long pos, Serializable s);
    /**
     * Reads the entry at a particular position. Throws exceptions if the entry
     * is unwritten or trimmed.
     *
     * @param pos
     */
    BufferStack read(long pos); //todo: throw exception
    Object readObject(long pos);
    /**
     * Trims the prefix of the address space before the passed in position.
     *
     * @param pos position before which all entries are trimmed
     */
    void prefixTrim(long pos);

    int getID();
}

class CacheMap<K,V> extends LinkedHashMap<K,V>
{
    int numentries;
    public CacheMap(int tnumentries)
    {
        super();
        numentries = tnumentries;
    }
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest)
    {
        if(size()>numentries)
            return true;
        return false;
    }
}

/**
 * Implements the write-once address space over the default Corfu shared log implementation.
 */
class CorfuLogAddressSpace implements WriteOnceAddressSpace
{
    Logger dbglog = LoggerFactory.getLogger(CorfuLogAddressSpace.class);

    int ID;

    final boolean caching = true;
    final int cachesize = 10000;
    CacheMap<Long, byte[]> cache = new CacheMap(cachesize);

    CorfuDBClient cl;
    org.corfudb.client.view.ObjectCachedWriteOnceAddressSpace cwoas;

    public CorfuLogAddressSpace(CorfuDBClient tcl, int ID)
    {
        this.ID = ID;
        cl = tcl;
        cwoas = new org.corfudb.client.view.ObjectCachedWriteOnceAddressSpace(cl);
    }

    public void write(long pos, BufferStack bs)
    {
        try
        {
            cwoas.write(pos, bs.flatten());
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void write(long pos, Serializable o)
    {
        try {
        cwoas.write(pos, o);
        } catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public Object readObject(long pos)
    {
        Object ret = null;
        int retrycounter = 0;
        final int retrymax = 12;
        while(true)
        {
            try
            {
                long difftime = -1;

                long startts = System.currentTimeMillis();

                ret = cwoas.readObject(pos);
                long stopts = System.currentTimeMillis();
                difftime = stopts-startts;

                //for now, copy to a byte array and return
              //  dbglog.debug("read back {} bytes, took {} ms", ret.length, difftime);
                break;
            }
            catch (UnwrittenException uce)
            {
                //encountered a hole -- try again
//                System.out.println("Hole..." + pos);
                retrycounter++;
                if(retrycounter==retrymax) throw new RuntimeException("Encountered non-transient hole at " + pos + "...");
                try
                {
                    int sleepms = (int)Math.pow(2, retrycounter);
                    dbglog.debug("Encountered hole; sleeping for {} ms...", sleepms);
                    //exponential backoff
                    Thread.sleep(sleepms);
                }
                catch(InterruptedException e)
                {
                    //ignore
                }
            }
            catch(Exception e)
            {
                throw new RuntimeException(e);
            }

        }
        return ret;
    }

    public BufferStack read(long pos)
    {
        dbglog.debug("Reading {}", pos);
        byte[] ret = null;
        int retrycounter = 0;
        final int retrymax = 12;
        while(true)
        {
            try
            {
                long difftime = -1;

                long startts = System.currentTimeMillis();

                ret = cwoas.read(pos);
                long stopts = System.currentTimeMillis();
                difftime = stopts-startts;

                //for now, copy to a byte array and return
                dbglog.debug("read back {} bytes, took {} ms", ret.length, difftime);
                break;
            }
            //reactivate this code block once michael throws exceptions on unwritten
            catch (UnwrittenException uce)
            {
                //encountered a hole -- try again
//                System.out.println("Hole..." + pos);
                retrycounter++;
                if(retrycounter==retrymax) throw new RuntimeException("Encountered non-transient hole at " + pos + "...");
                try
                {
                    int sleepms = (int)Math.pow(2, retrycounter);
                    dbglog.debug("Encountered hole; sleeping for {} ms...", sleepms);
                    //exponential backoff
                    Thread.sleep(sleepms);
                }
                catch(InterruptedException e)
                {
                    //ignore
                }
            }
            catch(Exception e)
            {
                throw new RuntimeException(e);
            }

        }
        return new BufferStack(ret);

    }

    @Override
    public void prefixTrim(long pos)
    {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public int getID()
    {
        return ID;
    }
}

class BufferStack implements Serializable //todo: custom serialization
{
    private Stack<byte[]> buffers;
    private int totalsize;
    public BufferStack()
    {
        buffers = new Stack<byte[]>();
        totalsize = 0;
    }
    public BufferStack(byte[] initialbuf)
    {
        this();
        push(initialbuf);
    }
    public void push(byte[] buf)
    {
        buffers.push(buf);
        totalsize += buf.length;
    }
    public byte[] pop()
    {
        byte[] ret = buffers.pop();
        if(ret!=null)
            totalsize -= ret.length;
        return ret;
    }
    public byte[] peek()
    {
        return buffers.peek();
    }
    public int flatten(byte[] buf)
    {
        if(buffers.size()==0) return 0;
        if(buf.length<totalsize) throw new RuntimeException("buffer not big enough!");
        if(buffers.size()>1) throw new RuntimeException("unimplemented");
        System.arraycopy(buffers.peek(), 0, buf, 0, buffers.peek().length);
        return buffers.peek().length;
    }
    public byte[] flatten()
    {
        if(buffers.size()==1) return buffers.peek();
        else throw new RuntimeException("unimplemented");
    }
    public int numBufs()
    {
        return buffers.size();
    }
    public int numBytes()
    {
        return totalsize;
    }
    public static BufferStack serialize(Serializable obj)
    {
        try {
        return new BufferStack(Serializer.serialize_compressed(obj));
        } catch (Exception e) { throw new RuntimeException(e); }
        /*
        try
        {
            //todo: custom serialization
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(obj);
            byte b[] = baos.toByteArray();
            oos.close();
            return new BufferStack(b);
        }
        catch(IOException e)
        {
            throw new RuntimeException(e);
        }*/
    }
    public Object deserialize()
    {
        try {
        return Serializer.deserialize_compressed(this.flatten());
        } catch (Exception e) { throw new RuntimeException(e); }

        /*
        try
        {
            //todo: custom deserialization
            ByteArrayInputStream bais = new ByteArrayInputStream(this.flatten());
            ObjectInputStream ois = new ObjectInputStream(bais);
            Object obj = ois.readObject();
            return obj;
        }
        catch(IOException e)
        {
            throw new RuntimeException(e);
        }
        catch(ClassNotFoundException ce)
        {
            throw new RuntimeException(ce);
        }*/
    }
    //todo: this is a terribly inefficient translation from the buffer
    //representation at the runtime layer to the one used by the logging layer
    //we need a unified representation
    public List<ByteBuffer> asList()
    {
        List<ByteBuffer> L = new LinkedList<ByteBuffer>();
        L.add(ByteBuffer.wrap(this.flatten()));
        return L;
    }
}



