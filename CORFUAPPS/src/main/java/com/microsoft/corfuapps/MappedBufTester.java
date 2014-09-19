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
// This is just a general performance tester of the MappByteBuffer IO 
// MappedByteBuffers is what CorfuUnitServers use internally, so it gives an idea of the raw throughput one might expect
//


package com.microsoft.corfuapps;

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
public class MappedBufTester {

	
	public enum TESTMODE {
		TSTREAD, TSTWRITE
	};
	
	static long BUFSIZE = 4096;
	static long FILESIZE = 1024 * 1024;
	static long RAMSIZE = BUFSIZE;
	static int CIRCSIZE = 3; // we maintain a circular list of mapped-buffer of this length
	static int curmapind = 0;
	static int[] circ = new int[CIRCSIZE];
	
	static FileChannel DriveChannel = null;
	static MappedByteBuffer[] DriveMemoryMap = null;
	

	static private MappedByteBuffer getStoreMap(long relOff) {
				
		int mapind = (int) (relOff/RAMSIZE);

		MappedByteBuffer mb  = DriveMemoryMap[0 /* mapind */];
			
		if (mapind != circ[curmapind]) {
			curmapind = (curmapind+1) % CIRCSIZE;
			int c = (curmapind+1)%CIRCSIZE;
			if (circ[c] >= 0) {
				// System.out.println("drop ind " + circ[c]);
				DriveMemoryMap[0 /* circ[c] */].force();
				// DriveMemoryMap[circ[c]] = null;
				// System.out.println("done");
			}
			circ[curmapind] = mapind;
		}
	
		if (mb == null)
			try {
				// System.out.println("map ind " + mapind);
				mb = DriveMemoryMap[0 /* mapind */] = DriveChannel.map(MapMode.READ_WRITE, relOff, RAMSIZE);
				mb.load();
				mb.rewind(); 
				// System.out.println("done");
	
			} catch (IOException e) {
				System.out.println("failure to sync drive to memory");
				e.printStackTrace();
				System.exit(-1);
			}
	
		mb.position((int) (relOff % RAMSIZE));
		return mb;
	}

	static private void RamToStore(long relOff, ByteBuffer buf) {
		MappedByteBuffer mb = getStoreMap(relOff);
		// System.out.println(mb.capacity() + " " + mb.position() + " " + mb.remaining());
		assert (mb.remaining() >= buf.capacity()); 
		buf.rewind();
		mb.put(buf.array());
	}
	
	static private void StoreToRam(long relOff, ByteBuffer buf)  {

		MappedByteBuffer mb = getStoreMap(relOff);

		if (!buf.hasArray()) 
			ByteBuffer.allocate((int) BUFSIZE);
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
			RandomAccessFile f = new RandomAccessFile(filename, "rw");
			f.setLength(FILESIZE);
			DriveChannel = f.getChannel();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		DriveMemoryMap = new MappedByteBuffer[(int)(FILESIZE/RAMSIZE)+1];
		System.out.println(DriveMemoryMap.length + " Mapped BybteBuffers");
		
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
					bb.rewind();
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
