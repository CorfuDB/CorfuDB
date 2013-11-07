package com.microsoft.corfu.unittests;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.microsoft.corfu.CorfuClientImpl;
import com.microsoft.corfu.CorfuConfigManager;
import com.microsoft.corfu.CorfuErrorCode;
import com.microsoft.corfu.CorfuException;
import com.microsoft.corfu.CorfuExtendedInterface;

public class CorfuBulkdataTester implements Runnable {

	static AtomicInteger commulative = new AtomicInteger(0);
	
	static private CorfuConfigManager CM;
	static private int nrepeat = 100000;
	static private int entsize = 0;
	static private int printfreq = 1000;
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		int nwriterthreads = 5;
		int nreaderthreads = 1;
		
		CM = new CorfuConfigManager(new File("./0.aux"));

		// parse args
		for (int i = 0; i < args.length; ) {
			if (args[i].startsWith("-repeat") && i < args.length-1) {
				nrepeat = Integer.valueOf(args[i+1]);
				System.out.println("repeat count: " + nrepeat);
				i += 2;
			} else if (args[i].startsWith("-wthreads") && i < args.length-1) {
				nwriterthreads = Integer.valueOf(args[i+1]);
				System.out.println("concurrent client writer-threads: " + nwriterthreads);
				i += 2;
			} else if (args[i].startsWith("-rthreads") && i < args.length-1) {
				nreaderthreads = Integer.valueOf(args[i+1]);
				System.out.println("concurrent client reader-threads: " + nreaderthreads);
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
				throw new Exception("Usage: " + CorfuBulkdataTester.class.getName() + 
						" [-rthreads <numreaderthreads>] [-wthreads <numwriterthreads>][-repeat <nrepeat>] [-size <entry-size>] [-printfreq <frequency>]");
			}
		}
		
		System.out.println("starting client ..");
		
		ExecutorService executor = Executors.newFixedThreadPool(nwriterthreads + nreaderthreads);
		for (int i = 0; i < nwriterthreads; i++) {
			Runnable worker = new CorfuBulkdataTester(i, false, CM);
			executor.execute(worker);
		}
		for (int i = 0; i < nreaderthreads; i++) {
			Runnable worker = new CorfuBulkdataTester(i, true, CM);
			executor.execute(worker);
		}
		
		executor.shutdown();
		executor.awaitTermination(1000, TimeUnit.SECONDS);
	}
	
	private int myid = -1;
	private boolean isreader = false;
	long starttime = System.currentTimeMillis();
	String myname = System.getenv("computername");
	
	public CorfuBulkdataTester(int myind, boolean isreader, CorfuConfigManager CM) {
		super();
		this.myid = myind; // thread id
		this.isreader = isreader; // thread id
	}

	private void stats(int rpt) {
		long elapsetime;
	
		if (rpt > 0 && rpt % printfreq == 0) {
			int c = commulative.addAndGet(printfreq);
			elapsetime = System.currentTimeMillis();
			System.out.println("+- " + myname + ":" + myid + 
					"-+  " + (rpt+1)/printfreq + "(*" + printfreq + ") appends+reads" +
					" (commulative " + c/printfreq + "(*" + printfreq + ") in " + (elapsetime-starttime)/1000 + " secs");
		}
	}

	@Override
	public void run() {
		
		long startoff, off = -1, contoff;
		long readoff = 0;
		CorfuExtendedInterface crf;
		
		try {
			crf = new CorfuClientImpl(CM);
			System.out.println("first check(): " + crf.check());
		} catch (CorfuException e3) {
			System.out.println("cannot set client conenction, giving up");
			e3.printStackTrace();
			return;
		}
		
		if (isreader) readerloop(crf); else writerloop(crf);
	}
	
	private void readerloop(CorfuExtendedInterface crf) {
		int rpt = 0;
		long cumsize = 0, cumpos = 0;
		List<ByteBuffer> ret;

		while(rpt < nrepeat) {
			try {
				ret = crf.varReadnext();
				stats(rpt++);
				cumsize += ret.size();
				if (cumsize >= CM.getUnitsize()/2) {
					cumpos += CM.getUnitsize()/2;
					cumsize -= CM.getUnitsize()/2;
					crf.trim(cumpos);
				}
			} catch (CorfuException e) {
				System.out.println("read failed");
				if (e.er.equals(CorfuErrorCode.ERR_UNWRITTEN))  // it's ok, but not counted
					{ rpt++; // TODO, for now
					continue; }
			}
		}
	}

	private void writerloop(CorfuExtendedInterface crf) {
		int rpt = 0;
		byte[] bb = new byte[entsize];
		long pos;

		while(rpt < nrepeat) {
			try {
				pos = crf.varAppend(bb, entsize);
				stats(rpt++);
			} catch (CorfuException e) {
				System.out.println("corfu append failed");
				break;
			}
		}
	}

}
