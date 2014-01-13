// This is just a general performance tester of the MappByteBuffer IO 
// MappedByteBuffers is what CorfuUnitServers use internally, so it gives an idea of the raw throughput one might expect
//


package com.microsoft.corfu.unittests;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

/**
 * @author dalia
 *
 */
public class MappedBufTesterB{

	
	public enum TESTMODE {
		TSTREAD, TSTWRITE
	};
	
	static int BUFSIZE = 4096;
	static long FILESIZE = 1024 * 1024;
	static int RAMSIZE = BUFSIZE;
	
	static FileChannel DriveChannel = null;
	static MappedByteBuffer DriveMemoryMap = null;
	static long DriveMapWindow = -1; // indicates the current drive "windows" mapped by DriveMemoryMap

	static private void getStoreMap(long relOff) {
		
		if (DriveMapWindow != (int) (relOff/RAMSIZE))
			try {
				/* if (DriveMemoryMap != null) { DriveMemoryMap.force(); DriveMemoryMap.limit(0);  } */
				DriveMemoryMap = DriveChannel.map(MapMode.READ_WRITE, relOff, RAMSIZE);
				DriveMemoryMap.load();
				DriveMemoryMap.rewind(); 
				DriveMapWindow = (int) (relOff/RAMSIZE);
	
			} catch (IOException e) {
				System.out.println("failure to sync drive to memory");
				e.printStackTrace();
				System.exit(-1);
			}

		DriveMemoryMap.position((int) (relOff % RAMSIZE));
	}

	static private void RamToStore(long relOff, ByteBuffer buf) {
		getStoreMap(relOff);
		assert (DriveMemoryMap.capacity() >= buf.capacity()); 
		buf.rewind();
		DriveMemoryMap.put(buf.array());
	}
	
	static private void StoreToRam(long relOff, ByteBuffer buf)  {

		getStoreMap(relOff);

		if (!buf.hasArray()) 
			ByteBuffer.allocate(BUFSIZE);
		assert (DriveMemoryMap.capacity() >= buf.capacity()); 

		buf.rewind();
		DriveMemoryMap.get(buf.array());
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		String filename = null;

		for (int i = 0; i < args.length; ) {
			if (args[i].startsWith("-filesize") && i < args.length-1) {
				FILESIZE = Long.valueOf(args[i+1]) * 1024 * 1024;
				RAMSIZE = (int) Math.min(Integer.MAX_VALUE, FILESIZE);
				System.out.println("filesize: " + FILESIZE/(1024*1024) + " MBytes");
				i += 2;
			} else if (args[i].startsWith("-ramsize") && i < args.length-1) {
					RAMSIZE =  Math.min(Integer.MAX_VALUE, Integer.valueOf(args[i+1]) * 1024);
					System.out.println("RAMSIZE: " + RAMSIZE/1024 + " KBytes");
					i += 2;
			} else if (args[i].startsWith("-filename") && i < args.length-1) {
				filename = args[i+1];
				System.out.println("filename: " + filename);
				i += 2;
			} else {
				System.out.println("unknown param: " + args[i]);
				throw new Exception("Usage: " + MappedBufTesterB.class.getName() + 
						" [-filesize <# MBytes>] [-ramsize <# KBytes>] -filename <filename>");
			}
		}
		if (filename == null) {
			throw new Exception("Usage: " + MappedBufTesterB.class.getName() + 
					" [-filesize <# MBytes>] [-RAMSIZE <# KBytes>] -filename <filename>");
		}
		
		try {
			RandomAccessFile f = new RandomAccessFile(filename, "rw");
			f.setLength(FILESIZE);
			DriveChannel = f.getChannel();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		for (int rpt = 0; rpt < 5; rpt++) {
			runtest(TESTMODE.TSTWRITE);
			runtest(TESTMODE.TSTREAD);
		}
	}
	
	private static void runtest(TESTMODE t) {
		long startTime = System.currentTimeMillis(), endTime;
	
		switch (t) {
			case TSTREAD:
				System.out.println("reading file sz =" + FILESIZE/(1024*1024) +
						" MBytes, using mapped buffers of size =" + RAMSIZE/1024 + " Kbytes");
				break;
			case TSTWRITE:
				System.out.println("write file sz =" + FILESIZE/(1024*1024) +
						" MBytes, using mapped buffers of size =" + RAMSIZE/1024 + " Kbytes");
				break;
		}

		ByteBuffer bb = ByteBuffer.allocate(BUFSIZE);
		byte c;
		long p = 0; long q = 0;
		for (long i = 0; i*BUFSIZE < FILESIZE; i ++) {
			if ((i-p)*BUFSIZE >= 1024*1024*20) { // print every 20MB ...
				p = i;
				System.out.print((i*BUFSIZE)/(1024*1024) + "MBytes.."); if (++q % 10 == 0) System.out.println("");
			}

			switch (t) {
				case TSTREAD: 
					StoreToRam(i*BUFSIZE, bb);
					bb.rewind();
					for (int b =0; b < BUFSIZE; b++) {
						if (bb.remaining() <= 0)
							System.out.println("not more remaining in buf");
						c = bb.get();
					}
					break;
					
				case TSTWRITE:
					bb.rewind();
					c = (byte)i;
					for (int b =0; b < BUFSIZE; b++) {
						if (bb.remaining() <= 0)
							System.out.println("not more remaining in buf");
						bb.put(c);
					}
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
