// This is just a general performance tester of the MappByteBuffer IO 
// MappedByteBuffers is what CorfuUnitServers use internally, so it gives an idea of the raw throughput one might expect
//


package com.microsoft.corfu.unittests;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
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
public class MappedBufTester {

	
	public enum TESTMODE {
		TSTREAD, TSTWRITE
	};
	
	static int BUFSIZE = 4096;
	static int FILESIZE = 1024 * 1024;
	static int RAMSIZE = BUFSIZE;
	
	static FileChannel DriveChannel = null;
	static ArrayList<MappedByteBuffer> DriveMemoryMap = null;
	

	static private MappedByteBuffer getStoreMap(int relOff) {
		int mapind = relOff/RAMSIZE;
		MappedByteBuffer mb  = null;
		
		if (DriveMemoryMap.size() <= mapind)
			try {
				mb = DriveChannel.map(MapMode.READ_WRITE, relOff, RAMSIZE);
				DriveMemoryMap.add(mapind, mb);
				mb.load();
				mb.rewind(); 
	
			} catch (IOException e) {
				System.out.println("failure to sync drive to memory");
				e.printStackTrace();
				System.exit(-1);
			}
		else
			mb = DriveMemoryMap.get(mapind);

		mb.position(relOff % RAMSIZE);
		return mb;
	}

	static private void RamToStore(int relOff, ByteBuffer buf) {
		MappedByteBuffer mb = getStoreMap(relOff);
		assert (mb.capacity() >= buf.capacity()); 
		buf.rewind();
		mb.put(buf.array());
	}
	
	static private void StoreToRam(int relOff, ByteBuffer buf)  {

		MappedByteBuffer mb = getStoreMap(relOff);

		if (!buf.hasArray()) 
			buf.allocate(BUFSIZE);
		assert (mb.capacity() >= buf.capacity()); 

		buf.rewind();
		mb.get(buf.array());
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		String filename = null;

		for (int i = 0; i < args.length; ) {
			if (args[i].startsWith("-filesize") && i < args.length-1) {
				RAMSIZE = FILESIZE = Integer.valueOf(args[i+1]) * 1024 * 1024;
				System.out.println("filesize: " + FILESIZE/(1024*1024) + " MBytes");
				i += 2;
			} else if (args[i].startsWith("-ramsize") && i < args.length-1) {
					RAMSIZE = Integer.valueOf(args[i+1]) * 1024;
					System.out.println("RAMSIZE: " + RAMSIZE/1024 + " KBytes");
					i += 2;
			} else if (args[i].startsWith("-filename") && i < args.length-1) {
				filename = args[i+1];
				System.out.println("filename: " + filename);
				i += 2;
			} else {
				System.out.println("unknown param: " + args[i]);
				throw new Exception("Usage: " + CorfuClientTester.class.getName() + 
						" [-filesize <# MBytes>] [-ramsize <# KBytes>] -filename <filename>");
			}
		}
		if (filename == null) {
			throw new Exception("Usage: " + CorfuClientTester.class.getName() + 
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
		
		DriveMemoryMap = new ArrayList<MappedByteBuffer>(FILESIZE/RAMSIZE);
		System.out.println(DriveMemoryMap.size() + " Mapped BybteBuffers");


		for (int rpt = 0; rpt < 5; rpt++) {
			runtest(TESTMODE.TSTREAD);
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
		int p = 0;
		for (int i = 0; i*BUFSIZE < FILESIZE; i ++) {
			if ((i-p)*BUFSIZE >= 1024*1024*20) { // print every 20MB ...
				p = i;
				System.out.print((i*BUFSIZE)/(1024*1024) + "MBytes..");
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
