package org.corfudb.sharedlog.examples;

import org.corfudb.sharedlog.ClientLib;
import org.corfudb.sharedlog.CorfuException;

/**
 * Created by dmalkhi on 1/16/15.
 */
public class CorfuHello {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {

        String masteraddress = null;

        if (args.length >= 1) {
            masteraddress = args[0]; // TODO check arg.length
        } else {
            // throw new Exception("must provide master http address"); // TODO
            masteraddress = "http://localhost:8000/corfu";
        }

        final int numthreads = 1;

        Thread[] allthreads = new Thread[numthreads];
        for(int i=0;i<numthreads;i++)
        {
          Thread T = new Thread(new ClientLibTester(masteraddress, i));
          T.start();
          allthreads[i] = T;
        }

        for(int i=0;i<numthreads;i++)
          allthreads[i].join();

        System.out.println("Successfully completed test!");


    }
}

class ClientLibTester implements Runnable
{
  ClientLib cl;
  int threadnum;
  public ClientLibTester(String masteraddress, int tnum) throws Exception
  {
    threadnum = tnum;
    try
    {
      cl = new ClientLib(masteraddress);
    }
    catch (CorfuException e)
    {
      e.printStackTrace();
      throw new CorfuException("cannot initalize command parser with master " + masteraddress);
    }
  }
  public void run()
  {
    int numappends = 10;
    try
    {
      System.out.println("thread " + threadnum + ": starting test");
      System.out.println("thread " + threadnum + ": issuing " + numappends + " appends...");
      for(int i=0;i<numappends;i++)
      {
          byte x[] = new byte[cl.grainsize()];
          cl.appendExtnt(x,x.length);
      }
    }
    catch(CorfuException ce)
    {
      throw new RuntimeException(ce);
    }
    System.out.println("thread " + threadnum + ": exiting test");
  }
}
