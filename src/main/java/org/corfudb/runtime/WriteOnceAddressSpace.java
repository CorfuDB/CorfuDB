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

import org.corfudb.sharedlog.ClientLib;
import org.corfudb.sharedlog.CorfuException;
import org.corfudb.sharedlog.ExtntWrap;
import org.corfudb.sharedlog.UnwrittenCorfuException;

import java.nio.ByteBuffer;
import java.util.LinkedList;

/**
 * This is the write-once address space providing storage for the shared log.
 */
interface WriteOnceAddressSpace
{
    /**
     * Writes an entry at a particular position. Throws an exception if
     * the entry is already written to.
     *
     * @param pos
     * @param bs
     */
    void write(long pos, BufferStack bs); //todo: throw exception

    /**
     * Reads the entry at a particular position. Throws exceptions if the entry
     * is unwritten or trimmed.
     *
     * @param pos
     */
    BufferStack read(long pos); //todo: throw exception

    /**
     * Trims the prefix of the address space before the passed in position.
     *
     * @param pos position before which all entries are trimmed
     */
    void prefixTrim(long pos);
}

/**
 * Implements the write-once address space over the default Corfu shared log implementation.
 */
class CorfuLogAddressSpace implements WriteOnceAddressSpace
{
    ClientLib cl;

    public CorfuLogAddressSpace(ClientLib tcl)
    {
        cl = tcl;
    }

    //todo we are currently synchronizing on 'cl' because ClientLib crashes on concurrent access;
    public void write(long pos, BufferStack bs)
    {
        try
        {
            //convert to a linked list of extent-sized bytebuffers, which is what the logging layer wants
            if(bs.numBytes()>cl.grainsize())
                throw new RuntimeException("entry too big at " + bs.numBytes() + " bytes; multi-entry writes not yet implemented");
            LinkedList<ByteBuffer> buflist = new LinkedList<ByteBuffer>();
            byte[] payload = new byte[cl.grainsize()];
            bs.flatten(payload);
            buflist.add(ByteBuffer.wrap(payload));
            synchronized(cl)
            {
                cl.writeExtnt(pos, buflist);
            }
        }
        catch(CorfuException ce)
        {
            throw new RuntimeException(ce);
        }
    }

    public BufferStack read(long pos)
    {
        System.out.println("Reading..." + pos);
        byte[] ret = null;
        int retrycounter = 0;
        final int retrymax = 1000;
        while(true)
        {
            try
            {
                ExtntWrap ew = null;
                synchronized(cl)
                {
                    ew = cl.readExtnt(pos);
                }
                //for now, copy to a byte array and return
                System.out.println("read back " + ew.getCtntSize() + " bytes");
                ret = new byte[4096 * 10]; //hack --- fix this
                ByteBuffer bb = ByteBuffer.wrap(ret);
                java.util.Iterator<ByteBuffer> it = ew.getCtntIterator();
                while (it.hasNext())
                {
                    ByteBuffer btemp = it.next();
                    bb.put(btemp);
                }
                break;
            }
            catch (UnwrittenCorfuException uce)
            {
                //encountered a hole -- try again
//                System.out.println("Hole..." + pos);
                try
                {
                    Thread.sleep(1000);
                }
                catch(InterruptedException e)
                {
                    //ignore
                }
                retrycounter++;
                if(retrycounter==retrymax) throw new RuntimeException("Encountered non-transient hole at " + pos + "...");
            }
            catch (CorfuException e)
            {
                throw new RuntimeException(e);
            }
        }
        System.out.println("Done Reading..." + pos);
        return new BufferStack(ret);

    }

    @Override
    public void prefixTrim(long pos)
    {
        try
        {
            synchronized(cl)
            {
                cl.trim(pos);
            }
        }
        catch (CorfuException e)
        {
            throw new RuntimeException(e);
        }
    }
}
