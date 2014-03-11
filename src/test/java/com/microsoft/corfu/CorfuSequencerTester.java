package com.microsoft.corfu;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;

import com.microsoft.corfu.CorfuConfigManager;
import com.microsoft.corfu.sequencer.CorfuSequencer;


public class CorfuSequencerTester implements Runnable {
	
	private CorfuConfigManager CM;
	private int myid = -1;
	private int nrequests = 100000;
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {

		CorfuConfigManager CM;
		String config = "./0.aux"; // default config file name
		int nthreads = 1; // default
		int nrequests = 100000; // default
		
		System.out.println("starting client .. (args: ");
		for (int i = 0; i < args.length; i++) System.out.println(args[i] + " ");
		System.out.println(")");
		
		// parse args
		for (int i = 0; i < args.length; ) {
			if (args[i].startsWith("-threads") && i < args.length-1) {
				nthreads = Integer.valueOf(args[i+1]);
				System.out.println("concurrent client threads: " + nthreads);
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
						" [-config <filename>] [-threads <numthreads>] [-repeat <nrepeat>]");
			}
		}
		
		CM = new CorfuConfigManager(new File(config));
		
		ExecutorService executor = Executors.newFixedThreadPool(nthreads);
		for (int i = 0; i < nthreads; i++) {
			Runnable worker = new CorfuSequencerTester(i, nrequests, CM);
			executor.execute(worker);
		}
		
		executor.shutdown();
		executor.awaitTermination(1000, TimeUnit.SECONDS);
	}
		
	public CorfuSequencerTester(int myind, int nrequests, CorfuConfigManager CM) {
		super();
		this.nrequests = nrequests;
		this.CM = CM;
		this.myid = myind; // thread id
	}


	@Override
	public void run() {
		
		long starttime;
		long elapsetime = 0;
		long off;
		
		CorfuSequencer.Client seqclient;
		TTransport transport;
		TBinaryProtocol protocol = null;
			
		int port = CM.getSequencer().getPort();
		String sequencer = CM.getSequencer().getHostname();

		try {
			transport = new TSocket(sequencer, port);
			protocol = new TBinaryProtocol(transport);
			seqclient = new CorfuSequencer.Client(protocol);
			System.out.println("++" + myid + "++ open connection with sequencer on " + sequencer + " port " + port);
			transport.open();

			starttime = System.currentTimeMillis();
			for (int i = 0; i < nrequests; i++) {
				// System.out.println(i + "..");
				off = seqclient.nextpos(1);
				if (i % 10000 == 0) {
					elapsetime = System.currentTimeMillis();
					System.out.println("++" + myid + "++: " + i + " tokens in " + (elapsetime-starttime) + " ms");
				}
			}
			System.out.println("++" + myid + "++ done");
			// crf.explicitclose();

		} catch (TTransportException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
	}
}
