/**
 * @author dalia
 * 
 * unit tester which injects "holes" into the log. 
 * it appends log entries and inject a hole every <holefreq> (runtime parameter) entries.
 * 
 * The main operation is given here; the rest is exception handling, stats printouts, etc:
 * 
 * 		while(rpt < nrepeat) {
 *			try {
 *				if (rpt > 0 && rpt % holefreq == 0) 
 *					crf.grabtokens(1);
 *				else
 *					crf.appendExtnt(bb,	 // the buffer
 *								entsize, // buffer size
 *								true	 // if log is full, request auto-trimming to last checkpoint
 *								);
 *				rpt++;
 *
 */
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

public class CorfuInjectHolesTester implements Runnable {
	private Logger log = LoggerFactory.getLogger(CorfuInjectHolesTester.class);

	static AtomicInteger wcommulative = new AtomicInteger(0);
	
	static private CorfuConfigManager CM;
	static private int nrepeat = 100000;
	static private int holefreq = 1000; // number of healthy log entries in between injected 
	static private int entsize = 0;
	static private int printfreq = 1000;
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		CM = new CorfuConfigManager(new File("./0.aux"));
		entsize = CM.getGrain(); // a default

		// parse args
		for (int i = 0; i < args.length; ) {
			if (args[i].startsWith("-repeat") && i < args.length-1) {
				nrepeat = Integer.valueOf(args[i+1]);
				System.out.println("repeat count: " + nrepeat);
				i += 2;
			} else if (args[i].startsWith("-holefreq") && i < args.length-1) {
				holefreq = Integer.valueOf(args[i+1]);
				System.out.println("inject holes every " + holefreq + " healthy entries");
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
				throw new Exception("Usage: " + CorfuInjectHolesTester.class.getName() + 
						" [-repeat <nrepeat>] [-holefreq <nents>] [-size <entry-size>] [-printfreq <frequency>]");
			}
		}
		
		Thread t1 = new Thread(new CorfuInjectHolesTester(true));
		t1.run();
		Thread t2 = new Thread(new CorfuInjectHolesTester(false));
		t2.run();
	}
	
	public CorfuInjectHolesTester(boolean dowork) {
		super();
		workerflag = dowork;
	}
	
	CorfuExtendedInterface crf;
	private boolean workerflag = false;
	long starttime = System.currentTimeMillis();
	String myname = System.getenv("computername");
	
	private void stats() {
		long starttime = System.currentTimeMillis(), elapsetime;
	
		for (;;) {
			try { Thread.sleep(1000); } catch (InterruptedException e) {}
			elapsetime = System.currentTimeMillis();
			int w = wcommulative.get();
			log.info("{} seconds {} appends {}/sec",
					elapsetime-starttime, w, w/(elapsetime-starttime));
		}
	}

	@Override
	public void run() {
		
		if (!workerflag) { stats(); return; }
		
		try {
			crf = new CorfuClientImpl(CM);
		} catch (CorfuException e) {
			log.error("cannot set client conenction, giving up");
			e.printStackTrace();
			System.exit(1);
		}

		int rpt = 0;
		byte[] bb = new byte[entsize];
		while(rpt < nrepeat) {
			try {
				if (rpt > 0 && rpt % holefreq == 0) 
					crf.grabtokens(1);
				else
					crf.appendExtnt(bb, 	 // the buffer
								entsize, // buffer size
								true	 // if log is full, request auto-trimming to last checkpoint
								);
				rpt++;
				wcommulative.incrementAndGet();
				
			} catch (CorfuException e) {
				if (e.er.equals(CorfuErrorCode.ERR_FULL)) {
					log.info("corfu append failed; out of space...........waiting for readers to consume the log and trim it...");
					try {
						do {
							Thread.sleep(100);
						} while (crf.checkLogMark(CorfuLogMark.TAIL) - crf.checkLogMark(CorfuLogMark.HEAD) >= CM.getCapacity());
						
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (CorfuException e1) {
						log.error("writeloop failed, quitting");
						e1.printStackTrace();
						break;
					}
				} else if (e.er.equals(CorfuErrorCode.ERR_OVERWRITE)) {
					log.warn("writeloop incurred OverwriteCorfuException; continuing");
				} else { // all other errors may not be recoverable 
					log.error("appendExtnt failed with bad error code, writerloop quitting");
					break;
				}
			}
		}
		System.exit(0);
	}
	
}
