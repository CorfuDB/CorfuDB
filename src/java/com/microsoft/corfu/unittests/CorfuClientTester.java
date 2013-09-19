package com.microsoft.corfu.unittests;

import java.io.File;
import java.io.FileInputStream;
import java.nio.channels.ShutdownChannelGroupException;
import java.util.List;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;

import com.microsoft.corfu.CorfuClientImpl;
import com.microsoft.corfu.CorfuConfigManager;
import com.microsoft.corfu.CorfuErrorCode;
import com.microsoft.corfu.CorfuException;
import com.microsoft.corfu.CorfuExtendedInterface;
import com.microsoft.corfu.CorfuInterface;
import com.microsoft.corfu.CorfuStandaloneClientImpl;

public class CorfuClientTester implements Runnable {

	private CorfuConfigManager CM;
	private boolean sa = false; // flag: use standalone CORFU service
	private int myid = -1;
	private int nrepeat = 2;
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		boolean sa = false;
		int nthreads = 5;

		CorfuConfigManager CM = new CorfuConfigManager(new File("./0.aux"));

		// parse args
		for (int i = 0; i < args.length; ) {
			if (args[i].equals("-sa")) {
				System.out.println("using standalone Corfu");
				sa = true;
				i++;
			} else if (args[i].startsWith("-threads") && i < args.length-1) {
				nthreads = Integer.valueOf(args[i+1]);
				System.out.println("concurrent client threads: " + nthreads);
				i += 2;
			} else {
				throw new Exception("Usage: " + CorfuClientTester.class.getName() + 
						" [-sa] [-threads <numthreads>] [-repeat <nrepeat>]");
			}
		}
		
		System.out.println("starting client ..");
		ExecutorService executor = Executors.newFixedThreadPool(nthreads);
		for (int i = 0; i < nthreads; i++) {
			Runnable worker = new CorfuClientTester(i, sa, CM);
			executor.execute(worker);
		}
		
		executor.shutdown();
		executor.awaitTermination(1000, TimeUnit.SECONDS);
	}
	
	
	public CorfuClientTester(int myind, boolean sa, CorfuConfigManager CM) {
		super();
		this.sa = sa;
		this.CM = CM;
		this.myid = myind; // thread id
	}


	@Override
	public void run() {
		
		long starttime = System.currentTimeMillis();
		long elapsetime = 0;
		long startoff, off = -1, contoff;
		long readoff = 0;
		CorfuInterface crf;
		
		try {
			if (sa) {
				// kluge; assume only one storage-unit for now!
				crf = new CorfuStandaloneClientImpl(CM.getGroupByNumber(0)[0].toString());		
			} else {
				crf = new CorfuClientImpl(CM);
			}
		} catch (CorfuException e3) {
			System.out.println("cannot set client conenction, giving up");
			e3.printStackTrace();
			return;
		}
		
		try {
			startoff = crf.check();
		} catch (CorfuException e) {
			System.out.println("check failed (shouldn't happen)");
			return;
		}

		for (int rpt = 0; rpt < nrepeat; rpt ++) {
			try {                  
				System.out.println("+- " + myid + "-+ start iteration " + rpt + "..");
				for (int i = 0; ; i++) {
					// System.out.println(i + "..");
					byte[] buf = new byte[4096];
					off = crf.append(buf);
					byte[] ret;
					ret = crf.read(off);
					if (i % 1000 == 0) {
						elapsetime = System.currentTimeMillis();
						System.out.println("+- " + myid + "-+  " + i + " appends+reads in " + (elapsetime-starttime) + " ms");
					}
				}
	
			} catch (CorfuException e) {
				if (e.er.equals(CorfuErrorCode.ERR_FULL)) {
					try {
						contoff = crf.check(true);
						System.out.println("+- " + myid + "-+ trimming up to mark " + contoff);
						crf.trim(contoff);
						off = contoff;
						
						// we already reserved the token at the failed position; repait will need to fill it!!
						// byte[] buf = new byte[4096];
						// crf.fill(contoff, buf);
					} catch (CorfuException te) {
						te.printStackTrace();
						// break;
					}
				
				} else if (e.er.equals(CorfuErrorCode.ERR_UNWRITTEN)) {
					System.out.println("+- " + myid + "-+ repairing the log..");
					try {
						((CorfuExtendedInterface)crf).repairlog();
					} catch (CorfuException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				} else if (e.er.equals(CorfuErrorCode.ERR_TRIMMED))  {
					System.out.println("+- " + myid + "+ read failed at " + off);
				} else {
					e.printStackTrace();
					break;
				}

			System.out.println("+- " + myid + "-+ done repetition " + rpt + " (up to offset " + off + ")");
		
			}
		}

	
		try {
			off = crf.check();
		} catch (CorfuException e) {
			System.out.println("check failed (shouldn't happen)");
			return;
		}
		elapsetime = System.currentTimeMillis();
		System.out.println("FINISHED [" + startoff + ".." + off + "]: " + (off-startoff) + " entries, time=" +  (elapsetime-starttime) + " ms");
	}
}