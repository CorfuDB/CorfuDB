package org.corfudb.test;

import gnu.getopt.Getopt;

import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.view.Sequencer;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.client.abstractions.SharedLog;
import org.corfudb.client.OutOfSpaceException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class CorfuStreamStress {

    public static void main(String[] args) throws Exception {

        int c;
        String strArg = null;
        String masteraddress = null;
        int numthreads = 1;
        int numappends = 10;
        boolean sharecl = false;
        CorfuDBClient cl = null;

        Getopt g = new Getopt("corfu-stream-stress", args, "m:t:n:p");
        while ((c = g.getopt()) != -1)
        {
            switch(c)
            {
                case 'm':
                    masteraddress = g.getOptarg();
                    masteraddress = masteraddress.trim();
                    System.out.println("master = "+masteraddress);
                    break;
                case 't':
                    strArg = g.getOptarg();
                    System.out.println("numthreads = "+ strArg);
                    numthreads = Integer.parseInt(strArg);
                    break;
                case 'n':
                    strArg = g.getOptarg();
                    System.out.println("numappends = "+ strArg);
                    numappends = Integer.parseInt(strArg);
                    break;
                case 'p':
                    sharecl = true;
                    break;
                default:
                    System.out.print("getopt() returned " + c + "\n");
            }
        }

        if (masteraddress == null)
            throw new Exception("must provide master http address");
        if(numthreads < 1)
            throw new Exception("need at least one thread!");
        if(numappends < 1)
            throw new Exception("need at least one append!");
        if(sharecl)
            cl = new CorfuDBClient(masteraddress);

        Thread[] allthreads = new Thread[numthreads];
        CyclicBarrier startbarrier = new CyclicBarrier(numthreads);
        CyclicBarrier stopbarrier = new CyclicBarrier(numthreads);
        for(int i=0;i<numthreads;i++) {
            Thread T = new Thread(new StreamTester(cl, startbarrier, stopbarrier, masteraddress, i, numappends));
            allthreads[i] = T;
            T.start();
        }

        for(int i=0;i<numthreads;i++)
            allthreads[i].join();

        System.out.println("Test complete.");
    }
}

class StreamTester implements Runnable
{
    CorfuDBClient cl;
    protected int threadnum = 1;
    protected int numappends = 10;
    CyclicBarrier startbarrier = null;
    CyclicBarrier stopbarrier = null;

    public
    StreamTester(
            CorfuDBClient _cl,
            CyclicBarrier _startbarrier,
            CyclicBarrier _stopbarrier,
            String masteraddress,
            int tnum,
            int nappends
            )
            throws Exception
    {
        startbarrier = _startbarrier;
        stopbarrier = _stopbarrier;
        threadnum = tnum;
        numappends = nappends;
        cl = _cl;

        if(cl == null) {
            try {
                cl = new CorfuDBClient(masteraddress);
            } catch (Exception e) {
                e.printStackTrace();
                throw new Exception("cannot initalize command parser with master " + masteraddress);
            }
        }
    }


    public void run() {
        try {
            System.out.println("stream tester thread " + threadnum + ": issuing " + numappends + " appends...");
            SharedLog sl = new SharedLog(new Sequencer(cl), new WriteOnceAddressSpace(cl));
            startbarrier.await();
            for(int i=0;i<numappends;i++) {
                byte x[] = new byte[4096];
                sl.append(x);
            }
            stopbarrier.await();
            System.out.println("stream tester thread " + threadnum + ": starting test");
        } catch (BrokenBarrierException be) {
            throw new RuntimeException(be);
        } catch (java.lang.InterruptedException ie) {
            throw new RuntimeException(ie);
        } catch (OutOfSpaceException oose)
        {
            throw new RuntimeException(oose);
        }
        System.out.println("stream tester thread " + threadnum + ": exiting test");
      }
}
