package com.microsoft.corfu.unittests;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.corfu.CorfuClientImpl;
import com.microsoft.corfu.CorfuConfigManager;
import com.microsoft.corfu.CorfuErrorCode;
import com.microsoft.corfu.CorfuException;
import com.microsoft.corfu.CorfuExtendedInterface;
import com.microsoft.corfu.CorfuLogMark;
import com.microsoft.corfu.ExtntInfo;
import com.microsoft.corfu.ExtntWrap;
import com.microsoft.corfu.sunit.CorfuUnitServerImpl;
import com.sun.org.apache.bcel.internal.classfile.CodeException;

public class CorfuBulkdataTester implements Runnable {
	private Logger log = LoggerFactory.getLogger(CorfuBulkdataTester.class);

	static AtomicInteger wcommulative = new AtomicInteger(0);
	static AtomicInteger rcommulative = new AtomicInteger(0);
	
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
			Runnable worker = new CorfuBulkdataTester(false, CM);
			executor.execute(worker);
		}
		for (int i = 0; i < nreaderthreads; i++) {
			Runnable worker = new CorfuBulkdataTester(true, CM);
			executor.execute(worker);
		}
		
		executor.shutdown();
		executor.awaitTermination(1000, TimeUnit.SECONDS);
	}
	
	CorfuExtendedInterface crf;
	private boolean isreader = false;
	long starttime = System.currentTimeMillis();
	String myname = System.getenv("computername");
	
	public CorfuBulkdataTester(boolean isreader, CorfuConfigManager CM) {
		super();
		this.isreader = isreader; // thread id
	}

	private void stats(int rpt, AtomicInteger cumm, String t) {
		long elapsetime;
	
		if (rpt > 0 && rpt % printfreq == 0) {
			int c = cumm.addAndGet(printfreq);
			elapsetime = System.currentTimeMillis();
			log.info("{}*{} (cummulative {}*{}) {}'s in {} secs", 
					(rpt+1)/printfreq, printfreq, 
					c/printfreq, printfreq,
					t, 
					(elapsetime-starttime)/1000);
		}
	}

	@Override
	public void run() {
		
		try {
			crf = new CorfuClientImpl(CM);
			log.info("first check(): " + crf.check());
		} catch (CorfuException e3) {
			log.error("cannot set client conenction, giving up");
			e3.printStackTrace();
			return;
		}
		
		if (isreader) readerloop(); else writerloop();
	}
	
	private void readerloop() {
		int rpt = 0;
		ExtntWrap ret = null;
		long trimpos = 0;
		long lastread = -1, lastwait = -2;

		while(rpt < nrepeat) {

			try {
				ret = crf.readExtnt();
				
				lastread = ret.getInf().getMetaFirstOff();
				if (ret.getCtnt().size() > 0) stats(++rpt, rcommulative, "READ");
				if (lastread - trimpos >= CM.getUnitsize()/2) {
					trimpos = lastread - (lastread % (CM.getUnitsize()/2));
					log.info("reader consumed half-log bulk; trimming log to {}", trimpos);
					crf.trim(trimpos);
				}
			
				
			} catch (CorfuException e) {
				e.printStackTrace();
				if (lastwait == lastread) { // no progress since last wait, problem??
					try {
						crf.repairNext();
					} catch (CorfuException ce) {
						log.error("repairNext failed, shouldn't happen?");
						return;
					}
				} else {
					synchronized(this) {
						try {
							lastwait = lastread;
							wait(1000);
						} catch (InterruptedException ex) {
							log.warn("reader wait interrupted; shouldn't happend..");
						}
					}
				}
			}
		}						
	}
	
	private void writerloop() {
		int rpt = 0;
		byte[] bb = new byte[entsize];
	
		while(rpt < nrepeat) {
			try {
				crf.appendExtnt(bb, entsize);
				synchronized(this) { notify(); }
				stats(++rpt, wcommulative, "WRITE");
			} catch (CorfuException e) {
				if (e.er.equals(CorfuErrorCode.ERR_FULL)) {
					log.info("corfu append failed; out of space. sleeping.");
					synchronized(this) { notify(); }
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				} else if (e.er.equals(CorfuErrorCode.ERR_BADPARAM)) {
					log.error("appendExtnt failed with bad error code, writerloop quitting");
					break;
				}
			}
		}
	}
	
}
