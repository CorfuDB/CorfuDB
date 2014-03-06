/**
 * @author dalia
 * 
 * simple read-test. 
 * creates <wthreads> (param) writer threads. Each writer-thread appends <repeat> entries of size <size> to the log. 
 */

package com.microsoft.corfu.unittests;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.corfu.CorfuClientImpl;
import com.microsoft.corfu.CorfuConfigManager;
import com.microsoft.corfu.CorfuException;
import com.microsoft.corfu.ExtntWrap;

public class ReadTester {
	static private Logger log = LoggerFactory.getLogger(ReadTester.class);

	static AtomicInteger rcommulative = new AtomicInteger(0);
	
	static private CorfuConfigManager CM;
	static private int nrepeat = 0;
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		int nreaderthreads = 0;

		// parse args
		for (int i = 0; i < args.length; ) {
			if (args[i].startsWith("-repeat") && i < args.length-1) {
				nrepeat = Integer.valueOf(args[i+1]);
				System.out.println("repeat count: " + nrepeat);
				i += 2;
			} else if (args[i].startsWith("-rthreads") && i < args.length-1) {
				nreaderthreads = Integer.valueOf(args[i+1]);
				System.out.println("concurrent client reader-threads: " + nreaderthreads);
				i += 2;
			} else {
				System.out.println("unknown param: " + args[i]);
				throw new Exception("Usage: " + CorfuRWTester.class.getName() + 
						" [-rthreads <numreaderthreads>][-repeat <nrepeat>]");
			}
		}
		
		if (nrepeat <= 0 || nreaderthreads <= 0) {
			throw new Exception("Usage: " + CorfuRWTester.class.getName() + 
					" [-rthreads <numreaderthreads>][-repeat <nrepeat>]");

		}
		System.out.println("Starting read tester with " +
				nreaderthreads + " threads, each reading " +
				nrepeat + " extents");
		
		
		CM = new CorfuConfigManager(new File("./0.aux"));

		// start a thread pool, each executing the simple run() loop inlined here
		//
		ExecutorService executor = Executors.newFixedThreadPool(nreaderthreads);
		for (int i = 0; i < nreaderthreads; i++) {
			executor.execute(new Runnable() {
				@Override
				public void run() {
					CorfuClientImpl crf;
					
					// establish client connection with Corfu service
					try {
						crf = new CorfuClientImpl(CM);
					} catch (CorfuException e) {
						System.out.println("cannot establish connection to Corfu service, quitting");
						return;
					}

					// append to log
					try {
						for (int r = 0; r < nrepeat; r++) {
							ExtntWrap w = crf.readExtnt();
							rcommulative.incrementAndGet();
						}
					} catch (CorfuException e) {
						System.out.println("Corfu error in appendExtnt: " + e.er + ". Quitting");
						e.printStackTrace();
						return;
					}

				}
			});
		}
		
		// start a thread to print statistics every second
		//
		new Thread(new Runnable() {
			@Override
			public void run() {
				long starttime = System.currentTimeMillis(), elapsetime;
				long lastwrite = 0;

				for (;;) {
					elapsetime = (System.currentTimeMillis() - starttime) / 1000;
					long w = rcommulative.get();
					if (elapsetime > 0)
							if (lastwrite < w) {
							log.info("{} secs, READs {} ({}/sec) ",
							elapsetime, 
							w, w/elapsetime);
							lastwrite = w;
						}
					try { Thread.sleep(1000); } catch (Exception e) {}
				}
			}}).run();
			
		
		executor.shutdown();
		executor.awaitTermination(1000, TimeUnit.SECONDS);
		
		System.exit(0);
	}
}
