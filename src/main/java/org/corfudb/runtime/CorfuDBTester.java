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

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
import org.corfudb.sharedlog.ClientLib;
import org.corfudb.sharedlog.CorfuException;



/**
 * Tester code for the CorfuDB runtime stack
 *
 *
 */

public class CorfuDBTester
{
    /**
     * @param args
     */
    public static void main(String[] args) throws Exception
    {

        if (args.length == 0)
        {
            System.out.println("usage: java CorfuDBTester masterURL");
            System.out.println("e.g. masterURL: http://localhost:8000/corfu");
            return;
        }

        String masternode = args[0];

        ClientLib crf;

        try
        {
            crf = new ClientLib(masternode);
        }
        catch (CorfuException e)
        {
            throw e;
        }


        int numthreads;


        List<Long> streams = new LinkedList<Long>();
        streams.add(new Long(1234)); //hardcoded hack
        streams.add(new Long(2345)); //hardcoded hack

        StreamBundle sb = new StreamBundleImpl(streams, new CorfuStreamingSequencer(crf), new CorfuLogAddressSpace(crf));

        //trim the stream to get rid of entries from previous tests
        sb.prefixTrim(sb.checkTail());

        //turn on to test stream bundle in isolation
/*
		numthreads = 2;
		for(int i=0;i<numthreads;i++)
		{
			Thread T = new Thread(new StreamBundleTester(sb));
			T.start();
		}
		Thread.sleep(10000);
		if(true) return;
*/

        TXRuntime TR = new TXRuntime(new SMREngine(sb));
        //SimpleRuntime TR = new SimpleRuntime(sb);


        //counter test
        //CorfuDBObject cob = new CorfuDBCounter(TR, 1234);
        //map test
        CorfuDBMap<Integer, Integer> cob1 = new CorfuDBMap<Integer, Integer>(TR, 2345);
        CorfuDBMap<Integer, Integer> cob2 = new CorfuDBMap<Integer, Integer>(TR, 2346);


        numthreads = 2;
        Thread[] threads = new Thread[numthreads];
        for (int i = 0; i < numthreads; i++)
        {
            //linearizable tester
            //Thread T = new Thread(new CorfuDBTester(cob));
            //transactional tester
            threads[i] = new Thread(new TXTesterThread(cob1, cob2, TR));
            threads[i].start();
        }
        for(int i=0;i<numthreads;i++)
            threads[i].join();
        System.out.println("Test done! Checking consistency...");
        TXTesterThread tx = new TXTesterThread(cob1, cob2, TR);
        if(tx.check_consistency())
            System.out.println("Consistency check passed --- test successful!");
        else
            System.out.println("Consistency check failed!");

        System.exit(0);

    }


}


/**
 * This tester implements a bipartite graph over two maps, where an edge exists between integers map1:X and map2:Y
 * iff map1:X == Y and map2:Y==X. The code uses transactions across the maps to add and remove edges,
 * ensuring that the graph is always in a consistent state.
 */
class TXTesterThread implements Runnable
{
    TXRuntime cr;
    CorfuDBMap<Integer, Integer> map1;
    CorfuDBMap<Integer, Integer> map2;
    int numkeys;
    int numops;

    public TXTesterThread(CorfuDBMap<Integer, Integer> tmap1, CorfuDBMap<Integer, Integer> tmap2, TXRuntime tcr)
    {
        this(tmap1, tmap2, tcr, 10, 100);
    }

    public TXTesterThread(CorfuDBMap<Integer, Integer> tmap1, CorfuDBMap<Integer, Integer> tmap2, TXRuntime tcr, int tnumkeys, int tnumops)
    {
        map1 = tmap1;
        map2 = tmap2;
        cr = tcr;
        numkeys = tnumkeys;
        numops = tnumops;
    }

    public boolean check_consistency()
    {
        boolean consistent = true;
        cr.BeginTX();
        for(int i=0;i<numkeys;i++)
        {
            if(map1.containsKey(i))
            {
                if(!map2.containsKey(map1.get(i)) || map2.get(map1.get(i))!=i)
                {
                    consistent = false;
                    break;
                }
            }
            if(map2.containsKey(i))
            {
                if(!map1.containsKey(map2.get(i)) || map1.get(map2.get(i))!=i)
                {
                    consistent = false;
                    break;
                }
            }
        }
        //todo: fix this -- it'll fail with multiple clients
        if(!cr.EndTX()) throw new RuntimeException("Consistency check aborted...");
        return consistent;
    }

    public void run()
    {
        int numcommits = 0;
        System.out.println("starting thread");
        if(numkeys<2) throw new RuntimeException("minimum number of keys for test is 2");
        for(int i=0;i<numops;i++)
        {
            int x = (int) (Math.random() * numkeys);
            int y = x;
            while(y==x)
                y = (int) (Math.random() * numkeys);
            System.out.println("Creating an edge between " + x + " and " + y);
            cr.BeginTX();
            if(map1.containsKey(x)) //if x is occupied, delete the edge from x
            {
                map2.remove(map1.get(x));
                map1.remove(x);
            }
            else if(map2.containsKey(y)) //if y is occupied, delete the edge from y
            {
                map1.remove(map2.get(y));
                map2.remove(y);
            }
            else
            {
                map1.put(x, y);
                map2.put(y, x);
            }
            if(cr.EndTX()) numcommits++;
/*            try
            {
                Thread.sleep((int)(Math.random()*1000.0));
            }
            catch(Exception e)
            {
                throw new RuntimeException(e);
            }*/
        }
        System.out.println("Tester thread is done: " + numcommits + " commits out of " + numops);
    }

}




class TesterThread implements Runnable
{

    CorfuDBObject cob;
    public TesterThread(CorfuDBObject tcob)
    {
        cob = tcob;
    }

    public void run()
    {
        System.out.println("starting thread");
        while(true)
        {
            if(cob instanceof CorfuDBCounter)
            {
                CorfuDBCounter ctr = (CorfuDBCounter)cob;
                ctr.increment();
                System.out.println("counter value = " + ctr.read());
            }
            else if(cob instanceof CorfuDBMap)
            {
                CorfuDBMap<Integer, String> cmap = (CorfuDBMap<Integer, String>)cob; //can't do instanceof on generics, have to guess
                int x = (int) (Math.random() * 1000.0);
                System.out.println("changing key " + x + " from " + cmap.put(x, "ABCD") + " to " + cmap.get(x));
            }
            try
            {
                Thread.sleep((int)(Math.random()*1000.0));
            }
            catch(Exception e)
            {
                throw new RuntimeException(e);
            }

        }
    }
}

//todo: custom serialization
class Pair<X, Y> implements Serializable
{
    final X first;
    final Y second;
    Pair(X f, Y s)
    {
        first = f;
        second = s;
    }

    public boolean equals(Pair<X,Y> otherP)
    {
        if(otherP==null) return false;
        if(((first==null && otherP.first==null) || first.equals(otherP.first)) //first matches up
                && ((second==null && otherP.second==null) || (second.equals(otherP.second)))) //second matches up
            return true;
        return false;
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
        }
    }
    public Object deserialize()
    {
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
        }
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



class StreamBundleTester implements Runnable
{
    StreamBundle sb;
    public StreamBundleTester(StreamBundle tsb)
    {
        sb = tsb;
    }
    public void run()
    {
        System.out.println("starting sb tester thread");
        while(true)
        {
            int op = 0;
            if(op==0)
            {
                byte x[] = new byte[5];
                Set<Long> T = new HashSet<Long>();
                T.add(new Long(5));
                sb.append(new BufferStack(x), T);
            }
            else continue;
        }
    }
}




class CorfuDBCounter extends CorfuDBObject
{
    //backing state of the counter
    int value;


    public CorfuDBCounter(AbstractRuntime tTR, long toid)
    {
        super(tTR, toid);
        value = 0;
        TR = tTR;
        oid = toid;
        TR.registerObject(this);
    }
    public void apply(Object bs)
    {
        //System.out.println("dummyupcall");
        System.out.println("CorfuDBCounter received upcall");
        CounterCommand cc = (CounterCommand)bs;
        lock(true);
        if(cc.getCmdType()==CounterCommand.CMD_DEC)
            value--;
        else if(cc.getCmdType()==CounterCommand.CMD_INC)
            value++;
        else
        {
            unlock(true);
            throw new RuntimeException("Unrecognized command in stream!");
        }
        unlock(true);
        System.out.println("Counter value is " + value);
    }
    public void increment()
    {
        HashSet<Long> H = new HashSet<Long>(); H.add(this.getID());
        TR.update_helper(this, new CounterCommand(CounterCommand.CMD_INC));
    }
    public int read()
    {
        TR.query_helper(this);
        //what if the value changes between queryhelper and the actual read?
        //in the linearizable case, we are safe because we see a later version that strictly required
        //in the transactional case, the tx will spuriously abort, but safety will not be violated...
        //todo: is there a more elegant API?
        lock(false);
        int ret = value;
        unlock(false);
        return ret;
    }

}

class CounterCommand implements Serializable
{
    int cmdtype;
    static final int CMD_DEC = 0;
    static final int CMD_INC = 1;
    public CounterCommand(int tcmdtype)
    {
        cmdtype = tcmdtype;
    }
    public int getCmdType()
    {
        return cmdtype;
    }
};

/*class CorfuDBRegister implements CorfuDBObject
{
	ByteBuffer converter;
	int registervalue;
	CorfuDBRuntime TR;
	long oid;
	public long getID()
	{
		return oid;
	}

	public CorfuDBRegister(CorfuDBRuntime tTR, long toid)
	{
		registervalue = 0;
		TR = tTR;
		converter = ByteBuffer.wrap(new byte[minbufsize]); //hardcoded
		oid = toid;
		TR.registerObject(this);
	}
	public void upcall(BufferStack update)
	{
//		System.out.println("dummyupcall");
		converter.put(update.pop());
		converter.rewind();
		registervalue = converter.getInt();
		converter.rewind();
	}
	public void write(int newvalue)
	{
//		System.out.println("dummywrite");
		converter.putInt(newvalue);
		byte b[] = new byte[minbufsize]; //hardcoded
		converter.rewind();
		converter.get(b);
		converter.rewind();
		TR.updatehelper(new BufferStack(b), oid);
	}
	public int read()
	{
//		System.out.println("dummyread");
		TR.queryhelper(oid);
		return registervalue;
	}
	public int readStale()
	{
		return registervalue;
	}
}
*/


