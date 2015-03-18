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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CyclicBarrier;

import gnu.getopt.Getopt;
import org.apache.zookeeper.CreateMode;
import org.corfudb.client.CorfuDBClient;
import org.corfudb.runtime.collections.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* Tester code for the CorfuDB runtime stack
*
*
*/

public class CorfuDBTester
{

    static Logger dbglog = LoggerFactory.getLogger(CorfuDBTester.class);

    static void print_usage()
    {
        System.out.println("usage: java CorfuDBTester");
        System.out.println("\t-m masternode");
        System.out.println("\t[-a testtype] (0==TXTest|1==LinMapTest|2==StreamTest|3==MultiClientTXTest|4==LinCounterTest)");
        System.out.println("\t[-t number of threads]");
        System.out.println("\t[-n number of ops]");
        System.out.println("\t[-k number of keys used in list tests]");
        System.out.println("\t[-l number of lists used in list tests]");
        System.out.println("\t[-p rpcport]");
        System.out.println("\t[-e expernum (for MultiClientTXTest)]");
        System.out.println("\t[-c numclients (for MultiClientTXTest)]");
        System.out.println("\t[-k numkeys (for TXTest)]");
        System.out.println("\t[-v verbose mode...]");
        System.out.println("\t[-x extreme debug mode (requires -v)]");
        System.out.println("\t[-r read write pct (double)]");
        System.out.println("\t[-T test case [functional|multifunctional|concurrent|tx]]\n");

//        if(dbglog instanceof SimpleLogger)
//            System.out.println("using SimpleLogger: run with -Dorg.slf4j.simpleLogger.defaultLogLevel=debug to " +
//                    "enable debug printouts");
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception
    {
        final int TXTEST=0;
        final int LINTEST=1;
        final int STREAMTEST=2;
        final int MULTICLIENTTXTEST=3;
        final int LINCTRTEST=4;
        final int REMOBJTEST=5;
        final int TXLOGICALLIST=6;
        final int TXLINKEDLIST=7;
        final int TXDOUBLYLINKEDLIST=8;
        final int LINZK=9;
        final int TXLOGICALBTREE = 10;
        final int TXPHYSICALBTREE = 11;

        int c;
        int numclients = 2;
        int expernum = 1; //used by the barrier code
        String strArg;
        int numthreads = 1;
        int numops = 1000;
        int numkeys = 100;
        int numlists = 2;
        int testnum = 0;
        int rpcport = 9090;
        String masternode = null;
        boolean verbose = false;
        double rwpct = 0.25;
        String testCase = "functional";

        if(args.length==0)
        {
            print_usage();
            return;
        }

        Getopt g = new Getopt("CorfuDBTester", args, "a:m:t:n:p:e:k:c:l:r:vxT:");
        while ((c = g.getopt()) != -1)
        {
            switch(c)
            {
                case 'T':
                    testCase = g.getOptarg();
                    break;
                case 'x':
                    TXListTester.extremeDebug = true;
                    BTreeTester.extremeDebug = true;
                    BTreeTester.trackOps = true;
                    break;
                case 'v':
                    verbose = true;
                    break;
                case 'a':
                    strArg = g.getOptarg();
                    System.out.println("testtype = "+ strArg);
                    testnum = Integer.parseInt(strArg);
                    break;
                case 'r':
                    strArg = g.getOptarg();
                    System.out.println("rwpct = "+ strArg);
                    rwpct = Double.parseDouble(strArg);
                    break;
                case 'm':
                    masternode = g.getOptarg();
                    masternode = masternode.trim();
                    System.out.println("master = " + masternode);
                    break;
                case 't':
                    strArg = g.getOptarg();
                    System.out.println("numthreads = "+ strArg);
                    numthreads = Integer.parseInt(strArg);
                    break;
                case 'n':
                    strArg = g.getOptarg();
                    System.out.println("numops = "+ strArg);
                    numops = Integer.parseInt(strArg);
                    break;
                case 'k':
                    strArg = g.getOptarg();
                    System.out.println("numkeys = "+ strArg);
                    numkeys = Integer.parseInt(strArg);
                    break;
                case 'l':
                    strArg = g.getOptarg();
                    System.out.println("numlists = "+ strArg);
                    numlists = Integer.parseInt(strArg);
                    break;
                case 'p':
                    strArg = g.getOptarg();
                    System.out.println("rpcport = "+ strArg);
                    rpcport = Integer.parseInt(strArg);
                    break;
                case 'c':
                    strArg = g.getOptarg();
                    System.out.println("numbarrier = " + strArg);
                    numclients = Integer.parseInt(strArg);
                    break;
                case 'e':
                    strArg = g.getOptarg();
                    System.out.println("expernum = " + strArg);
                    expernum = Integer.parseInt(strArg);
                default:
                    System.out.print("getopt() returned " + c + "\n");
            }
        }

        if(masternode == null)
            throw new Exception("must provide master http address using -m flag");
        if(numthreads < 1)
            throw new Exception("need at least one thread!");
        if(numops < 1)
            throw new Exception("need at least one op!");


        String rpchostname;

        try
        {
            rpchostname = InetAddress.getLocalHost().getHostName();
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }


        CorfuDBClient crf;

        crf = new CorfuDBClient(masternode);
        crf.startViewManager();
        crf.waitForViewReady();

        Thread[] threads = new Thread[numthreads];

        StreamFactory sf = new StreamFactoryImpl(new CorfuLogAddressSpace(crf, 0), new CorfuStreamingSequencer(crf)); //todo: fill in the right logid

        long starttime = System.currentTimeMillis();

        AbstractRuntime TR = null;
        DirectoryService DS = null;
        CorfuDBCounter barrier=null;

        if(testnum==LINTEST)
        {
            Map<Integer, Integer> cob1 = null;
            int numpartitions = 10;
            if(numpartitions==0)
            {
                TR = new SimpleRuntime(sf, DirectoryService.getUniqueID(sf), rpchostname, rpcport);
                cob1 = new CorfuDBMap<Integer, Integer>(TR, DirectoryService.getUniqueID(sf));
            }
            else
            {
                TR = new TXRuntime(sf, DirectoryService.getUniqueID(sf), rpchostname, rpcport);
                CDBLinkedList<Long> partitionlist = new CDBLinkedList<>(TR, sf, DirectoryService.getUniqueID(sf));
                while(true)
                {
                    TR.BeginTX();
                    if(partitionlist.size()!=numpartitions)
                    {
                        for (int i = 0; i < numpartitions; i++)
                            partitionlist.add(DirectoryService.getUniqueID(sf));
                    }
                    if(TR.EndTX()) break;
                }
                cob1 = new PartitionedMap<Integer, Integer>(partitionlist, TR);
            }
            for (int i = 0; i < numthreads; i++)
            {
                //linearizable tester
                threads[i] = new Thread(new MapTesterThread(cob1));
                threads[i].start();
            }
            for(int i=0;i<numthreads;i++)
                threads[i].join();
            System.out.println("Test succeeded!");
        }
        else if(testnum==LINCTRTEST)
        {
            TR = new SimpleRuntime(sf, DirectoryService.getUniqueID(sf), rpchostname, rpcport);
            CorfuDBCounter ctr1 = new CorfuDBCounter(TR, DirectoryService.getUniqueID(sf));
            for (int i = 0; i < numthreads; i++)
            {
                //linearizable tester
                threads[i] = new Thread(new CtrTesterThread(ctr1));
                threads[i].start();
            }
            for(int i=0;i<numthreads;i++)
                threads[i].join();
            System.out.println("Test succeeded!");
        }
        else if(testnum==TXTEST)
        {
            int numpartitions = 0;
            boolean perthreadstack = true;
            CorfuDBMap<Integer, Integer> cob1 = null;
            CorfuDBMap<Integer, Integer> cob2 = null;
            if(!perthreadstack)
            {
                TR = new TXRuntime(sf, DirectoryService.getUniqueID(sf), rpchostname, rpcport);

                DS = new DirectoryService(TR);
                cob1 = new CorfuDBMap(TR, DS.nameToStreamID("testmap1"));
                cob2 = new CorfuDBMap(TR, DS.nameToStreamID("testmap2"));
            }
            TXTesterThread firsttester = null;
            for (int i = 0; i < numthreads; i++)
            {
                TXTesterThread ttt = null;
                //transactional tester
                if(perthreadstack)
                    ttt = new TXTesterThread(numkeys, numops, sf, rpchostname, rpcport+i, numpartitions);
                else
                    ttt = new TXTesterThread(cob1, cob2, TR, numkeys, numops);
                if(i==0) firsttester = ttt;
                threads[i] = new Thread(ttt);
            }
            for(int i=0;i<numthreads;i++)
                threads[i].start();
            for(int i=0;i<numthreads;i++)
                threads[i].join();
            System.out.println("Test done! Checking consistency...");
            if(firsttester.check_consistency())
                System.out.println("Consistency check passed --- test successful!");
            else
            {
                System.out.println("Consistency check failed!");
                System.out.println(firsttester.map1);
                System.out.println(firsttester.map2);
            }
            System.out.println(TR);
        }
        else if(testnum==STREAMTEST)
        {
            Stream sb = sf.newStream(1234);

            //trim the stream to get rid of entries from previous tests
            //sb.prefixTrim(sb.checkTail()); //todo: turning off, trim not yet implemented at log level
            for(int i=0;i<numthreads;i++)
            {
                threads[i] = new Thread(new StreamTester(sb, numops));
                threads[i].start();
            }
            for(int i=0;i<numthreads;i++)
                threads[i].join();
        }
        else if(testnum==TXLOGICALLIST) {
            TR = new TXRuntime(sf, DirectoryService.getUniqueID(sf), rpchostname, rpcport);
            TXListTester.<Integer, CDBLogicalList<Integer>>runListTest(
                    TR, sf, numthreads, numlists, numops, numkeys, rwpct, "CDBLogicalList", verbose);
        }
        else if(testnum==TXLINKEDLIST) {
            TR = new TXRuntime(sf, DirectoryService.getUniqueID(sf), rpchostname, rpcport);
            TXListTester.<Integer, CDBLinkedList<Integer>>runListTest(
                    TR, sf, numthreads, numlists, numops, numkeys, rwpct, "CDBLinkedList", verbose);
        }
        else if(testnum==TXDOUBLYLINKEDLIST) {
            TR = new TXRuntime(sf, DirectoryService.getUniqueID(sf), rpchostname, rpcport);
            TXListTester.<Integer, CDBDoublyLinkedList<Integer>>runListTest(
                    TR, sf, numthreads, numlists, numops, numkeys, rwpct, "CDBDoublyLinkedList", verbose);
        }
        else if(testnum==TXLOGICALBTREE) {
            TR = new TXRuntime(sf, DirectoryService.getUniqueID(sf), rpchostname, rpcport);
            BTreeTester.<String, String, CDBLogicalBTree<String, String>>runTest(
                    TR, sf, numthreads, numlists, numops, numkeys, rwpct, "CDBLogicalBTree", testCase, verbose);
        }
        else if(testnum==TXPHYSICALBTREE) {
            TR = new TXRuntime(sf, DirectoryService.getUniqueID(sf), rpchostname, rpcport);
            BTreeTester.<String, String, CDBPhysicalBTree<String, String>>runTest(
                    TR, sf, numthreads, numlists, numops, numkeys, rwpct, "CDBPhysicalBTree", testCase, verbose);
        }
        else if(testnum==REMOBJTEST)
        {
            //create two maps, one local, one remote
            TR = new TXRuntime(sf, DirectoryService.getUniqueID(sf), rpchostname, rpcport);

            DS = new DirectoryService(TR);
            CorfuDBMap<Integer, Integer> cob1 = new CorfuDBMap(TR, DS.nameToStreamID("testmap" + (rpcport%2)));
            CorfuDBMap<Integer, Integer> cob2 = new CorfuDBMap(TR, DS.nameToStreamID("testmap" + ((rpcport+1)%2)), true);
            System.out.println("local map = testmap" + (rpcport%2) + " " + cob1.getID());
            System.out.println("remote map = testmap" + ((rpcport+1)%2) + " " + cob2.getID());


            System.out.println("sleeping");
            Thread.sleep(10000);
            System.out.println("woke up");

            cob1.put(100, 55);
            System.out.println(cob1.size());
            System.out.println(cob2.size());
            Thread.sleep(5000);
            System.out.println("Test succeeded!");
        }
        else if(testnum==MULTICLIENTTXTEST)
        {
            //barrier code
            TR = new TXRuntime(sf, DirectoryService.getUniqueID(sf), rpchostname, rpcport);
            DS = new DirectoryService(TR);
            CorfuDBMap<Integer, Integer> cob1 = new CorfuDBMap(TR, DS.nameToStreamID("testmap" + (rpcport%2)));
            CorfuDBMap<Integer, Integer> cob2 = new CorfuDBMap(TR, DS.nameToStreamID("testmap" + ((rpcport+1)%2)), true);

            barrier = new CorfuDBCounter(TR, DS.nameToStreamID("barrier" + expernum));
            if(barrier.read()>numclients)
            {
                System.out.println("This experiment number has been used before! Use a new number for the -e flag, or check" +
                        " that the -c flag correctly specifies the number of clients.");
                System.exit(0);
            }
            barrier.increment();
            long lastprinttime = System.currentTimeMillis();
            int curnumclients = 0;
            while((curnumclients = barrier.read()) < numclients)
            {
                if(System.currentTimeMillis()-lastprinttime>3000)
                {
                    System.out.println("current number of clients in barrier " + expernum + " = " + curnumclients);
                    lastprinttime = System.currentTimeMillis();
                }
            }
            dbglog.debug("Barrier reached; starting test...");

            System.out.println("local map = testmap" + (rpcport%2) + " " + cob1.getID());
            System.out.println("remote map = testmap" + ((rpcport+1)%2)+ " " + cob2.getID());

            for (int i = 0; i < numthreads; i++)
            {
                //transactional tester
                threads[i] = new Thread(new TXTesterThread(cob1, cob2, TR, numkeys, numops));
                threads[i].start();
            }
            for(int i=0;i<numthreads;i++)
                threads[i].join();
            barrier.increment();
            while(barrier.read() < 2*numclients)
                cob1.size(); //this ensures that we're still processing txints and issuing partial decisions to help other nodes
                             //need a cleaner way to ensure this
            dbglog.debug("second barrier reached; checking consistency...");
            System.out.println("Checking consistency...");
            TXTesterThread tx = new TXTesterThread(cob1, cob2, TR, numkeys, numops);
            if(tx.check_consistency())
                System.out.println("Consistency check passed --- test successful!");
            else
                System.out.println("Consistency check failed!");
            System.out.println(TR);
            barrier.increment();
            while(barrier.read() < 3*numclients);
            dbglog.debug("third barrier reached; test done");
            System.out.println("Test done!");
        }
        else if(testnum==LINZK)
        {
            TR = new TXRuntime(sf, DirectoryService.getUniqueID(sf), rpchostname, rpcport);
            DS = new DirectoryService(TR);
            IZooKeeper zk = new CorfuDBZK(TR, DS.nameToStreamID("zookeeper"), false, null);
            if(zk.exists("/xyz",  true)==null)
                System.out.println(zk.create("/xyz", "ABCD".getBytes(), null, CreateMode.PERSISTENT));
            else
                System.out.println("already exists");
            System.out.println(zk.exists("/xyz",  true));
            zk.setData("/xyz", "AAA".getBytes(), -1);
            System.out.println(new String(zk.getData("/xyz", false, null)));
            zk.delete("/xyz", -1);
            System.out.println(zk.exists("/xyz",  true));
            System.out.println(zk.create("/xyz", "ABCD".getBytes(), null, CreateMode.PERSISTENT));
            Thread.sleep(1000);
            int numzops = 100;
            //synchronous testing
            for(int i=0;i<numzops;i++)
                zk.create("/xyz/" + i, "AAA".getBytes(), null, CreateMode.PERSISTENT);
            for(int i=0;i<numzops;i++)
                zk.setData("/xyz/" + i, "BBB".getBytes(), -1);
            for(int i=0;i<numzops;i++)
                zk.exists("/xyz/" + i, false);
            for(int i=0;i<numzops;i++)
                zk.getData("/xyz/" + i, false, null);
            for(int i=0;i<numzops;i++)
                zk.getChildren("/xyz/" + i, false);
            //atomic rename
            int txretries = 0;
            int moves = 0;
            for(int i=0;i<numzops;i++)
            {
                while(true)
                {
                    TR.BeginTX();
                    String src = "/xyz/" + (int)((Math.random()*(double)numzops));
                    String dest = "/xyz/" + (int)((Math.random()*(double)numzops*2));
                    if(zk.exists(src, false)!=null && zk.exists(dest, false)==null)
                    {
                        moves++;
                        byte[] data = zk.getData(src, false, null);
                        zk.delete(src, -1);
                        zk.create(dest, data, null, CreateMode.PERSISTENT); //take mode from old item?
                    }
                    if (TR.EndTX())
                    {
                        break;
                    }
                    else
                        txretries++;
                }
            }
            System.out.println("atomic renames: " + moves + " moves, " + txretries + " TX retries.");
            for(int i=0;i<numzops;i++)
                zk.delete("/xyz/" + i, -1);
            for(int i=0;i<numzops;i++)
                System.out.println("Sequential --- " + zk.create("/xyzaaa", "qwerty".getBytes(), null, CreateMode.PERSISTENT_SEQUENTIAL));
            List<String> childnodes = zk.getChildren("/", false);
            Iterator<String> it = childnodes.iterator();
            while(it.hasNext())
                zk.delete(it.next(), -1);
            zk.create("/abcd", "QWE".getBytes(), null, CreateMode.PERSISTENT);
        }

        System.out.println("Test done in " + (System.currentTimeMillis()-starttime));

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
    private static Logger dbglog = LoggerFactory.getLogger(TXTesterThread.class);

    AbstractRuntime cr;
    Map<Integer, Integer> map1;
    Map<Integer, Integer> map2;
    int numkeys;
    int numops;

    //creates thread-specific stack
    public TXTesterThread(int tnumkeys, int tnumops, StreamFactory sf, String rpchostname, int rpcport, int numpartitions)
    {
        TXRuntime TR = new TXRuntime(sf, DirectoryService.getUniqueID(sf), rpchostname, rpcport);

        DirectoryService DS = new DirectoryService(TR);
        if(numpartitions==0)
        {
            map1 = new CorfuDBMap(TR, DS.nameToStreamID("testmap1"));
            map2 = new CorfuDBMap(TR, DS.nameToStreamID("testmap2"));
        }
        else
        {
            throw new RuntimeException("unimplemented");
            /*
            Map<Integer, Integer> partmaparray1[] = new Map[numpartitions];
            for (int i = 0; i < partmaparray1.length; i++)
                partmaparray1[i] = new CorfuDBMap<Integer, Integer>(TR, DS.nameToStreamID("testmap1-part" + i));
            map1 = new PartitionedMap<>(partmaparray1);

            Map<Integer, Integer> partmaparray2[] = new Map[numpartitions];
            for (int i = 0; i < partmaparray2.length; i++)
                partmaparray2[i] = new CorfuDBMap<Integer, Integer>(TR, DS.nameToStreamID("testmap2-part" + i));
            map2 = new PartitionedMap<>(partmaparray2);*/
        }

        cr = TR;
        numkeys = tnumkeys;
        numops = tnumops;
    }

    public TXTesterThread(CorfuDBMap<Integer, Integer> tmap1, CorfuDBMap<Integer, Integer> tmap2, AbstractRuntime tcr)
    {
        this(tmap1, tmap2, tcr, 10, 100);
    }

    public TXTesterThread(CorfuDBMap<Integer, Integer> tmap1, CorfuDBMap<Integer, Integer> tmap2, AbstractRuntime tcr, int tnumkeys, int tnumops)
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
        int numretries = 10;
        int j = 0;
        for(j=0;j<numretries;j++)
        {
            cr.BeginTX();
            for (int i = 0; i < numkeys; i++)
            {
                if (map1.containsKey(i))
                {
                    if (!map2.containsKey(map1.get(i)) || map2.get(map1.get(i)) != i)
                    {
                        consistent = false;
                        System.out.println("inconsistency on " + i);
                        break;
                    }
                }
                if (map2.containsKey(i))
                {
                    if (!map1.containsKey(map2.get(i)) || map1.get(map2.get(i)) != i)
                    {
                        consistent = false;
                        System.out.println("inconsistency on " + i);
                        break;
                    }
                }
            }
            if (cr.EndTX())
            {
                break;
            }
            else
            {
                consistent = true;
                System.out.println("abort! retrying consistency check...");
            }
        }
        if(j==numretries) throw new RuntimeException("too many aborts on consistency check");
        return consistent;
    }

    public void run()
    {
        int numcommits = 0;
        System.out.println("starting thread");
        if(numkeys<2) throw new RuntimeException("minimum number of keys for test is 2");
        for(int i=0;i<numops;i++)
        {
            long curtime = System.currentTimeMillis();
            dbglog.debug("Tx starting...");
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
            dbglog.debug("Tx took {}", (System.currentTimeMillis()-curtime));
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


class CtrTesterThread implements Runnable
{
    CorfuDBCounter ctr;
    public CtrTesterThread(CorfuDBCounter tcob)
    {
        ctr = tcob;
    }

    public void run()
    {
        System.out.println("starting thread");
        for(int i=0;i<100;i++)
        {
            ctr.increment();
            System.out.println("counter value = " + ctr.read());
        }
    }
}


class MapTesterThread implements Runnable
{

    Map cmap;
    public MapTesterThread(Map tcob)
    {
        cmap = tcob;
    }

    public void run()
    {
        System.out.println("starting thread");
        for(int i=0;i<100;i++)
        {
            int x = (int) (Math.random() * 1000.0);
            System.out.println("changing key " + x + " from " + cmap.put(x, "ABCD") + " to " + cmap.get(x));
        }
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



class StreamTester implements Runnable
{
    Stream sb;
    int numops = 10000;
    public StreamTester(Stream tsb, int nops)
    {
        sb = tsb;
        numops = nops;
    }
    public void run()
    {
        System.out.println("starting sb tester thread");
        for(int i=0;i<numops;i++)
        {
            byte x[] = new byte[5];
            Set<Long> T = new HashSet<Long>();
            T.add(new Long(5));
            sb.append(new BufferStack(x), T);
        }
    }
}






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

/*class PerfCounter
{
    String description;
    AtomicLong sum;
    AtomicInteger num;
    public PerfCounter(String desc)
    {
        sum = new AtomicLong();
        num = new AtomicInteger();
        description = desc;
    }
    public long incrementAndGet()
    {
        //it's okay for this to be non-atomic, since these are just perf counters
        //but we do need to get exact numbers eventually, hence the use of atomiclong/integer
        num.incrementAndGet();
        return sum.incrementAndGet();
    }
    public long addAndGet(long val)
    {
        num.incrementAndGet();
        return sum.addAndGet(val);
    }
}*/