/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * @author dalia
 * 
 * simple write-test. 
 * creates <wthreads> (param) writer threads. Each writer-thread appends <repeat> entries of size <size> to the log. 
 */

package com.microsoft.corfuapps;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.corfu.ClientLib;
import com.microsoft.corfu.CorfuException;

public class CorfuAppendTputTester {
	static private Logger log = LoggerFactory.getLogger(CorfuAppendTputTester.class);

	static AtomicInteger wcommulative = new AtomicInteger(0);
	
	static private int nrepeat = 0;
	static private int entsize = 0;
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		int nwriterthreads = 0;

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
			} else if (args[i].startsWith("-size") && i < args.length-1) {
				entsize = Integer.valueOf(args[i+1]);
				System.out.println("entry size: " + entsize);
				i += 2;
			} else {
				System.out.println("unknown param: " + args[i]);
				throw new Exception("Usage: " + CorfuAppendTputTester.class.getName() + 
						" [-wthreads <numwriterthreads>][-repeat <nrepeat>] [-size <entry-size>]");
			}
		}
		
		if (nrepeat <= 0 || entsize <= 0 || nwriterthreads <= 0) {
			throw new Exception("Usage: " + CorfuAppendTputTester.class.getName() + 
					" [-wthreads <numwriterthreads>][-repeat <nrepeat>] [-size <entry-size>]");

		}
		System.out.println("Starting write tester with " +
				nwriterthreads + " threads, each writing " +
				nrepeat + " extents of size " +
				entsize + " each.");
		
		
		// start a thread pool, each executing the simple run() loop inlined here
		//
		ExecutorService executor = Executors.newFixedThreadPool(nwriterthreads);
		for (int i = 0; i < nwriterthreads; i++) {
			executor.execute(new Runnable() {
				@Override
				public void run() {
					ClientLib crf;
					
					// establish client connection with Corfu service
					try {
						crf = new ClientLib("localhost");
					} catch (CorfuException e) {
						System.out.println("cannot establish connection to Corfu service, quitting");
						return;
					}

					// append to log
					try {
						byte[] bb = new byte[entsize];
						for (int r = 0; r < nrepeat; r++) {
							crf.appendExtnt(bb, entsize);
							wcommulative.incrementAndGet();
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
					long w = wcommulative.get();
					if (elapsetime > 0)
							if (lastwrite < w) {
							log.info("{} secs, WRITEs {} ({}/sec) ",
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
