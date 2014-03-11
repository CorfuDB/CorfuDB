// This is just a general performance tester of the MappByteBuffer IO 
// MappedByteBuffers is what CorfuUnitServers use internally, so it gives an idea of the raw throughput one might expect
//


package com.microsoft.corfu;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author dalia
 *
 */
public class SimpleBufTester {

	
	public enum TESTMODE {
		TSTREAD, TSTWRITE
	};
	
	static long BUFSIZE = 4096*10;
	static long FILESIZE = 1024 * 1024;
	
	static FileChannel ch;
	
	static private void RamToStore(long Off, ByteBuffer buf) {
		buf.rewind();
		
		try {
//			if (Off > 0 && Off % (1024*1024*1024) == 0) {
//				System.out.println("Off= " + Off + " " + Off%(1024*1024) + " " + Off%(1024*1024*1024) + ": force sync");
//				ch.force(false);
//				System.out.println("done sync");
//			}
			ch.write(buf, Off);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	static private void StoreToRam(long relOff, ByteBuffer buf)  {

		if (!buf.hasArray()) 
			ByteBuffer.allocate((int) BUFSIZE);
		buf.rewind();
		
		try {
			ch.read(buf, relOff);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		String filename = null;

		for (int i = 0; i < args.length; ) {
			if (args[i].startsWith("-filesize") && i < args.length-1) {
				FILESIZE = Long.valueOf(args[i+1]) * 1024;
				System.out.println("filesize: " + FILESIZE/(1024*1024) + " MBytes");
				i += 2;
			} else if (args[i].startsWith("-filename") && i < args.length-1) {
				filename = args[i+1];
				System.out.println("filename: " + filename);
				i += 2;
			} else {
				System.out.println("unknown param: " + args[i]);
				throw new Exception("Usage: " + MappedBufTester.class.getName() + 
						" [-filesize <# KBytes>] -filename <filename>");
			}
		}
		if (filename == null) {
			throw new Exception("Usage: " + MappedBufTester.class.getName() + 
					" [-filesize <# MBytes>] -filename <filename>");
		}
		
		try {
			   ch = new RandomAccessFile(filename, "rw").getChannel();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				stats();
				
			}
		}).start();
		
		new Thread(new Runnable() {
			@Override
			public void run() {
				for(;;) {
					try {
						ch.force(false);
						Thread.sleep(1);
					} catch(Exception e) {
						e.printStackTrace();
					}
				}
			}
		}).start();

		for (int rpt = 0; rpt < 5; rpt++) {
			runtest(TESTMODE.TSTWRITE);
			runtest(TESTMODE.TSTREAD);
		}
				
	}
	
	static int xput = 0; // in BUFSIZE units
	
	private static void stats() {
		int lastxput = 0;
		long starttime = System.currentTimeMillis();
		
		for (;;) {
			long elapse = (System.currentTimeMillis() - starttime)/1000;
			if (elapse > 0)
				System.out.println((xput -lastxput) * BUFSIZE / (1024*1024) + 
					" MB/sec...(total " + xput*BUFSIZE/(1024*1024) + " MB in " + elapse + " secs)");
			lastxput = xput;
			try { Thread.sleep(1000); } catch (Exception e) {}
			
		}
	}
	
	private static void runtest(TESTMODE t) {
		long startTime = System.currentTimeMillis(), endTime;
	
		switch (t) {
			case TSTREAD:
				System.out.println("reading file sz =" + FILESIZE/(1024*1024) + " MB");
				break;
			case TSTWRITE:
				System.out.println("write file sz =" + FILESIZE/(1024*1024) + " MB");
				break;
		}

		ByteBuffer bb = ByteBuffer.allocate((int)BUFSIZE);
		ByteBuffer cc =  ByteBuffer.allocate(1);
		for (long i = 0; i*BUFSIZE < FILESIZE; i ++) {
			xput++;
			
			switch (t) {
				case TSTREAD: 
					StoreToRam(i*BUFSIZE, bb);
					break;
					
				case TSTWRITE:
					bb.rewind();
					RamToStore(i*BUFSIZE, bb);
					RamToStore(FILESIZE-BUFSIZE /* ((i+100)-(i%100))*BUFSIZE */, cc);
					break;
			}
		}
		
		endTime = System.currentTimeMillis();
		
		System.out.println("");
		System.out.println(FILESIZE/(1024*1024) + " MB in " + (endTime-startTime) + " milliseconds: ");
	}		
}
