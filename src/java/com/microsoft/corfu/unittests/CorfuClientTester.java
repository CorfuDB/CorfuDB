package com.microsoft.corfu.unittests;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ShutdownChannelGroupException;
import java.util.List;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;

import com.microsoft.corfu.CorfuClientImpl;
import com.microsoft.corfu.CorfuConfigManager;
import com.microsoft.corfu.CorfuErrorCode;
import com.microsoft.corfu.CorfuException;
import com.microsoft.corfu.CorfuExtendedInterface;
import com.microsoft.corfu.CorfuInterface;

public class CorfuClientTester implements Runnable {

	static AtomicInteger commulative = new AtomicInteger(0);
	
	private CorfuConfigManager CM;
	private int myid = -1;
	static private int nrepeat = 100000;
	static private int entsize = 0;
	static private int printfreq = 1000;
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		int nthreads = 5;
		
		CorfuConfigManager CM = new CorfuConfigManager(new File("./0.aux"));

		// parse args
		for (int i = 0; i < args.length; ) {
			if (args[i].startsWith("-repeat") && i < args.length-1) {
				nrepeat = Integer.valueOf(args[i+1]);
				System.out.println("repeat count: " + nrepeat);
				i += 2;
			} else if (args[i].startsWith("-threads") && i < args.length-1) {
				nthreads = Integer.valueOf(args[i+1]);
				System.out.println("concurrent client threads: " + nthreads);
				i += 2;
			} else if (args[i].startsWith("-size") && i < args.length-1) {
				entsize = Integer.valueOf(args[i+1]);
				System.out.println("entry size: " + entsize);
				i += 2;
			} else if (args[i].startsWith("-printfreq") && i < args.length-1) {
				printfreq = Integer.valueOf(args[i+1]);
				System.out.println("print every # appends: " + printfreq);
				i += 2;
			} else {
				System.out.println("unknown param: " + args[i]);
				throw new Exception("Usage: " + CorfuClientTester.class.getName() + 
						" [-threads <numthreads>] [-repeat <nrepeat>] [-size <entry-size>] [-printfreq <frequency>]");
			}
		}
		
		System.out.println("starting client ..");
		ExecutorService executor = Executors.newFixedThreadPool(nthreads);
		for (int i = 0; i < nthreads; i++) {
			Runnable worker = new CorfuClientTester(i, CM);
			executor.execute(worker);
		}
		
		executor.shutdown();
		executor.awaitTermination(1000, TimeUnit.SECONDS);
	}
	
	
	public CorfuClientTester(int myind, CorfuConfigManager CM) {
		super();
		this.CM = CM;
		this.myid = myind; // thread id
	}


	@Override
	public void run() {
		
		long starttime = System.currentTimeMillis();
		long elapsetime = 0;
		long startoff, off = -1, contoff;
		long readoff = 0;
		CorfuExtendedInterface crf;
		String myname = System.getenv("computername");
		
		try {
			crf = new CorfuClientImpl(CM);
		} catch (CorfuException e3) {
			System.out.println("cannot set client conenction, giving up");
			e3.printStackTrace();
			return;
		}
		
		int rpt;
		for (rpt = 0; rpt < nrepeat; rpt ++) {
			try {
				byte[] buf = new byte[entsize];
				off = crf.forceAppend(buf, entsize);
				List<ByteBuffer> ret;
				ret = crf.varRead(off, entsize);
				if (rpt > 0 && rpt % printfreq == 0) {
					int c = commulative.addAndGet(printfreq);
					elapsetime = System.currentTimeMillis();
					System.out.println("+- " + myname + ":" + myid + 
							"-+  " + (rpt+1)/printfreq + "(*" + printfreq + ") appends+reads" +
							" (commulative " + c/printfreq + "(*" + printfreq + ") in " + (elapsetime-starttime)/1000 + " secs");
				}
			} catch (CorfuException e) {
				if (e.er.equals(CorfuErrorCode.ERR_UNWRITTEN)) {
					System.out.println("+- " + myname + ":" + myid + "-+ repairing the log..");
					try {
						((CorfuExtendedInterface)crf).repairLog();
					} catch (CorfuException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
						break;
					}
				} else if (e.er.equals(CorfuErrorCode.ERR_TRIMMED))  {
					System.out.println("+- " + myname + ":" + myid + "+ read failed at " + off);
				} else {
					e.printStackTrace();
					break;
				}
			}
		}

		elapsetime = System.currentTimeMillis();
		int c = commulative.addAndGet(rpt % printfreq);
		System.out.println("FINISHED [" + myname + ":" + myid + "]: " +
				rpt/printfreq + "(*" + printfreq + ") entries (commulative " + c/printfreq + "(*" + printfreq + 
				"*" + entsize/1024 + "KBytes), time= " +
				(elapsetime-starttime)/1000 + " secs");
	}
}