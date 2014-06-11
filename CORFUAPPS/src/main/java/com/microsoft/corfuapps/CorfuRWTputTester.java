/**
 * @author dalia
 * 
 * unit tester. 
 * creates <rthreads> (commands line parameter) reader threads and <wthreads> (param) writer threads.
 * 
 * writer-thread append <repeat> entries of size <size> to the log. 
 * it notified reader threads (if any) every time it appends to the log.
 * If the log fills up, it automatically trims it to latest checkpoint to facilitate progress.

 *  without exception handling and so on, the stripped-down writer code is as follows:
 *  
		while(rpt < nrepeat) {
				crf.appendExtnt(bb, 	 // the buffer
								entsize, // buffer size
								true	 // if log is full, request auto-trimming to last checkpoint
								);
				synchronized(this) { notify(); }

				...if (e.er.equals(CorfuErrorCode.ERR_FULL)) {
					log.info("corfu append failed; out of space...........waiting for readers to consume the log and trim it...");
					try {
						do {
							synchronized(this) { notify(); }
							Thread.sleep(1);
						} while (crf.checkLogMark(CorfuLogMark.TAIL) - crf.checkLogMark(CorfuLogMark.HEAD) >= CM.getCapacity());
					}
				}
			}
 *						
 *
 * reader-thread reads <repeat> entries from the log.
 * Whenever it consumes half the log capacity, it trims capacity/2 entries. 
 * If it gets stalled, it tries to repair the log at the point of stalling.
 * Note that, repair may fill the log with a SKIP mark at that problem point,
 * 
 *  without exception handling and so on, the stripped-down reader code is as follows:
 *  
 		while (rpt < nrepeat) {
			ret = crf.readExtnt();
		
			nextread = ret.getInf().getMetaFirstOff() + ret.getInf().getMetaLength();
			if (nextread - trimpos >= CM.getUnitsize()/2) {
				trimpos = nextread - (nextread % (CM.getUnitsize()/2));
				crf.trim(trimpos);
			}
	
			tail = crf.checkLogMark(CorfuLogMark.TAIL);
			if (lastattempted == nextread && (tail > nextread || lasttail == tail)) {
				log.error("reader seems stuck; quitting");
				return;
			}
			lastattempted = nextread;
			if (tail > nextread) {
				crf.repairNext();
			} else {
				lasttail = tail;
				synchronized(this) { wait(1000); }
			}
		}
 *
 */

package com.microsoft.corfuapps;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.microsoft.corfu.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CorfuRWTputTester {
	static private Logger log = LoggerFactory.getLogger(CorfuRWTputTester.class);

	static AtomicInteger wcommulative = new AtomicInteger(0);
	static AtomicInteger rcommulative = new AtomicInteger(0);
	
	static CorfuRWTputTester tster;
	static private int nrepeat = 0;
	static private int entsize = 0;
	static private int printfreq = 1000;
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		int nwriterthreads = 5;
		int nreaderthreads = 1;
		String Usage = "Usage: " + CorfuRWTputTester.class.getName() + 
				" [-rthreads <numreaderthreads>] [-wthreads <numwriterthreads>]-repeat <nrepeat> -size <entry-size> [-printfreq <frequency>]";
		
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
				throw new Exception(Usage);
			}
		}
		if (entsize == 0 || nrepeat == 0) throw new Exception(Usage);
		
		tster = new CorfuRWTputTester();
		
		ExecutorService executor = Executors.newFixedThreadPool(nwriterthreads + nreaderthreads);
		for (int i = 0; i < nwriterthreads; i++) {
			executor.execute(new Runnable() {
				@Override
				public void run() {
					tster.writerloop();
				}
			});
		}
		
		for (int i = 0; i < nreaderthreads; i++) {
			executor.execute(new Runnable() {
				@Override
				public void run() {
					tster.readerloop();
				}
			});
		}
		
		new Thread(new Runnable() {
			@Override
			public void run() {
				tster.stats();
			}}).run();
			
		
		executor.shutdown();
		executor.awaitTermination(1000, TimeUnit.SECONDS);
		
		System.exit(0);
	}
	
	private void stats() {
		long starttime = System.currentTimeMillis(), elapsetime;
		long lastread = 0, lastwrite = 0;

		for (;;) {
			elapsetime = (System.currentTimeMillis() - starttime) / 1000;
			long r = rcommulative.get(), w = wcommulative.get();
			if (elapsetime > 0)
					if (lastread < r || lastwrite < w) {
					log.info("{} secs,		READs {} ({}/sec)   		WRITEs {} ({}/sec) ",
					elapsetime, 
					r, r/elapsetime, 
					w, w/elapsetime);
					lastread = r; lastwrite = w;
				}
			try { Thread.sleep(1000); } catch (Exception e) {}
		}
	}

	private void readerloop() {
		CorfuClientImpl crf;
		
		int rpt = 0;
		ExtntWrap ret = null;
		long trimpos = 0;
		long nextread = 0;
		CorfuConfiguration CM = null;

		try {
			crf = new CorfuClientImpl();
			CM = crf.getConfig();
		} catch (CorfuException e) {
			log.error("reader cannot set connection to Corfu service, quitting");
			return;
		}

		while(rpt < nrepeat) {

			try {
				ret = crf.readExtnt();
				
				nextread = ret.getInf().getMetaFirstOff() + ret.getInf().getMetaLength();
				rcommulative.incrementAndGet();
				rpt++;

				if (nextread - trimpos >= CM.getUnitsize()/2) {
					trimpos = ret.getInf().getMetaFirstOff(); // no! crf.checkLogMark(CorfuLogMark.CONTIG);
					log.info("reader consumed half-log ; trimming log to {}", trimpos);
					crf.trim(trimpos);
				}
			
				
			} catch (CorfuException e) {
				long tail;
				
				try {
					tail = crf.querytail();
					log.debug("read stalled..{}  nextread={} tail={}", e.er, nextread, tail);
					if (tail > nextread) {
						nextread = crf.repairNext();
					} else {
						log.debug("reader waiting for log to fill up");
						synchronized(this) { wait(1); }
					}
				} catch (InterruptedException ex) {
					log.warn("reader wait interrupted; shouldn't happend..");
				} catch (CorfuException ce) {
					log.error("repairNext failed, shouldn't happen?");
					return;
				}
			}
		}						
	}
	
	private void writerloop() {
		int rpt = 0;
		CorfuClientImpl crf;
		CorfuConfiguration CM = null;
		long off, lasthead;
	
		try {
			crf = new CorfuClientImpl();
			CM = crf.getConfig();
			lasthead = crf.queryhead();
		} catch (CorfuException e) {
			log.error("reader cannot set connection to Corfu service, quitting");
			return;
		}
		byte[] bb = new byte[entsize];

		while(rpt < nrepeat) {
			try {
				off = crf.appendExtnt(bb, 	 // the buffer
								entsize, // buffer size
								true	 // if log is full, request auto-trimming to last checkpoint
								);
				rpt++;
				wcommulative.incrementAndGet();
				synchronized(this) { notify(); }
				
				if (off - lasthead > CM.getCapacity()/2) {
					log.info("checkpoint at {}", off);
					crf.ckpoint(off);
				}

			} catch (CorfuException e) {
				if (e.er.equals(CorfuErrorCode.ERR_FULL)) {
					log.info("corfu append failed; out of space...........waiting for readers to consume the log and trim it...");
					try {
						do {
							synchronized(this) { notify(); }
							Thread.sleep(1);
						} while (crf.querytail() - crf.queryhead() >= CM.getCapacity());
						
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (CorfuException e1) {
						log.error("writeloop checkLogMark() failed, quitting");
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
	}
	
}
