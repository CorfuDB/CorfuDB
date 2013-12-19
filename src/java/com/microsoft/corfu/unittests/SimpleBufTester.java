// This is just a general performance tester of the MappByteBuffer IO 
// MappedByteBuffers is what CorfuUnitServers use internally, so it gives an idea of the raw throughput one might expect
//


package com.microsoft.corfu.unittests;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.sql.Time;
import java.util.ArrayList;
import java.util.BitSet;

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
	static long RAMSIZE = BUFSIZE;
	
	static FileChannel ch;
	
	static private void RamToStore(long relOff, ByteBuffer buf) {
		buf.rewind();
		
		try {
			ch.write(buf, relOff);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	static private void StoreToRam(long relOff, ByteBuffer buf)  {

		if (!buf.hasArray()) 
			buf.allocate((int) BUFSIZE);
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
				RAMSIZE = Math.min(Integer.MAX_VALUE, FILESIZE);
				System.out.println("filesize: " + FILESIZE/(1024*1024) + " MBytes");
				i += 2;
			} else if (args[i].startsWith("-ramsize") && i < args.length-1) {
					RAMSIZE =  Math.min(Integer.MAX_VALUE, Long.valueOf(args[i+1]) * 1024);
					System.out.println("RAMSIZE: " + RAMSIZE/1024 + " KBytes");
					i += 2;
			} else if (args[i].startsWith("-filename") && i < args.length-1) {
				filename = args[i+1];
				System.out.println("filename: " + filename);
				i += 2;
			} else {
				System.out.println("unknown param: " + args[i]);
				throw new Exception("Usage: " + MappedBufTester.class.getName() + 
						" [-filesize <# KBytes>] [-ramsize <# KBytes>] -filename <filename>");
			}
		}
		if (filename == null) {
			throw new Exception("Usage: " + MappedBufTester.class.getName() + 
					" [-filesize <# MBytes>] [-RAMSIZE <# KBytes>] -filename <filename>");
		}
		if (RAMSIZE % BUFSIZE != 0) {
			throw new Exception("Usage: RAMSIZE must be a multiple of " + BUFSIZE);
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
			System.out.println((xput -lastxput) * BUFSIZE / (1024*1024) + 
					" MB/sec...(total " + xput*BUFSIZE/(1024*1024) + " MB in " + (System.currentTimeMillis() - starttime)/1000 + " secs)");
			lastxput = xput;
			try { Thread.sleep(1000); } catch (Exception e) {}
		}
	}
	
	private static void runtest(TESTMODE t) {
		long startTime = System.currentTimeMillis(), endTime;
	
		switch (t) {
			case TSTREAD:
				System.out.println("reading file sz =" + FILESIZE/(1024) +
						" KBytes, using mapped buffers of size =" + RAMSIZE/1024 + " Kbytes");
				break;
			case TSTWRITE:
				System.out.println("write file sz =" + FILESIZE/(1024) +
						" KBytes, using mapped buffers of size =" + RAMSIZE/1024 + " Kbytes");
				break;
		}

		ByteBuffer bb = ByteBuffer.allocate((int)BUFSIZE);
		for (long i = 0; i*BUFSIZE < FILESIZE; i ++) {
			xput++;
			
			switch (t) {
				case TSTREAD: 
					StoreToRam(i*BUFSIZE, bb);
					break;
					
				case TSTWRITE:
					bb.rewind();
					RamToStore(i*BUFSIZE, bb);
					break;
			}
		}
		
		endTime = System.currentTimeMillis();
		
		System.out.println("");
		System.out.println(" total time in seconds: " + (endTime-startTime)/1000);
		System.out.println(" throughoput MByte/sec: " + FILESIZE/(endTime-startTime)/1024);
	}		
}
