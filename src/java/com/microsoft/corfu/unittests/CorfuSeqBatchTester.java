package com.microsoft.corfu.unittests;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.microsoft.corfu.CorfuConfigManager;
import com.microsoft.corfu.sequencer.CorfuSequencer;

public class CorfuSeqBatchTester implements Runnable {

	/**
	 * @param args
	 */
	static int nrequests = 100000;
	static ArrayBlockingQueue<Integer> tokensneeded;
	static ArrayBlockingQueue<Integer> tokens;
	static AtomicInteger id = new AtomicInteger(0);
	
	CorfuSequencer.Client seqclient;
	TTransport transport;
	TBinaryProtocol protocol = null;

	public CorfuSeqBatchTester(CorfuConfigManager CM) {
			
		int port = CM.getSequencer().getPort();
		String sequencer = CM.getSequencer().getHostname();

		try {
			transport = new TSocket(sequencer, port);
			protocol = new TBinaryProtocol(transport);
			seqclient = new CorfuSequencer.Client(protocol);
			System.out.println("++ open connection with sequencer on " + sequencer + " port " + port);
			transport.open();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void run() {
		int batchsize;
		Collection<Integer> c = new ArrayList<Integer>(1000);
		
		while (true) {
			batchsize = tokensneeded.drainTo(c); c.clear();
		
			// System.out.println("batch tester .. size=" + batchsize);
			if (batchsize <= 0) {
				Thread.yield();
				continue;
			}
			
			// acquire the next 'batchsize' tokens...
			try {
				long off = seqclient.nextpos();
				while (batchsize-- > 0) { 
					tokens.put(1);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {

		CorfuConfigManager CM;
		String config = "./0.aux"; // default config file name
		int nconsumerthreads = 1; // default
		int nproducerthreads = 1;
		
		System.out.println("starting client .. (args: ");
		for (int i = 0; i < args.length; i++) System.out.println(args[i] + " ");
		System.out.println(")");
		
		// parse args
		for (int i = 0; i < args.length; ) {
			if (args[i].startsWith("-consumerthreads") && i < args.length-1) {
				nconsumerthreads = Integer.valueOf(args[i+1]);
				i += 2;
			} else if (args[i].startsWith("-producerthreads") && i < args.length-1) {
					nproducerthreads = Integer.valueOf(args[i+1]);
					i += 2;
			} else if (args[i].startsWith("-repeat") && i < args.length-1) {
				nrequests = Integer.valueOf(args[i+1]);
				System.out.println("repeat count: " + nrequests);
				i += 2;
			} else if (args[i].startsWith("-config") && i < args.length-1) {
				config = args[i+1];
				System.out.println("use config file: " + config);
				i += 2;
			} else {
				throw new Exception("Usage: " + CorfuSequencerTester.class.getName() + 
						" [-config <filename>]" +
						" [-consumerthreads <numthreads>]" + 
						" [-producerthreads <numthreads>]" + 
						" [-repeat <nrepeat>]");
			}
		}
		
		CM = new CorfuConfigManager(new File(config));
		tokensneeded = new ArrayBlockingQueue<Integer>(1000);
		tokens = new ArrayBlockingQueue<Integer>(1000);
		
		ExecutorService executor = 
				Executors.newFixedThreadPool(nconsumerthreads+nproducerthreads);
		
		// execute 'nconsumerthreads' consumer-threads, 
		// each consuming 'nrequests' tokens
		//
		for (int myid = 0; myid < nconsumerthreads; myid++) {
			executor.execute(new Runnable() {
				public void run() {
					long elapsetime = 0;
					long starttime = System.currentTimeMillis();
					int uid = id.incrementAndGet();

					for (int j = 0; j < nrequests; j++) {
						try {
							tokensneeded.put(1);
							tokens.take();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
/*						if (j % 10000 == 0) {
							elapsetime = System.currentTimeMillis();
							System.out.println("++" + uid + "++: " + 
									j + " tokens in " + (elapsetime-starttime) + " ms");
						}
*/					}

					elapsetime = System.currentTimeMillis();
					System.out.println("++" + uid + "++: " + 
							"done" +
							nrequests + " tokens in " + (elapsetime-starttime) + " ms");
				}
			});
		}
		
		// execute one thread which grabs a batch of tokens from the sequencer 
		// and puts them on the blocking queue for the consumer threads
		//
		for (int t = 0; t < nproducerthreads; t++) {
			executor.execute(new CorfuSeqBatchTester(CM));
		}
		
		executor.shutdown();
		executor.awaitTermination(1000, TimeUnit.SECONDS);
	}

}
