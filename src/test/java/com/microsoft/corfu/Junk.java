package com.microsoft.corfu;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.microsoft.corfu.CorfuUtil;

public class Junk implements Runnable {

	class Serializer {
		ByteArrayOutputStream bb;
		ObjectOutputStream oo;
		
		ByteArrayInputStream bi;
		ObjectInputStream oi;
		
		public Serializer() {
			try {
				bb = new ByteArrayOutputStream();
				oo = new ObjectOutputStream(bb);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		public Serializer(byte[] buf) {
			bi = new ByteArrayInputStream(buf);
			try {
				oi = new ObjectInputStream(bi);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		void dos(Object o) {
			try {
				oo.writeObject(o);
				System.out.println("dos size:" + bb.toByteArray().length);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		Object sod() {
			Object o = null;
			try {
				o = oi.readObject();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return o;
		}
	}
	/**
	 * @param args
	 */
	
	static Object lck = new Object();
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		new Thread(new Runnable() {
			public void run() {
				synchronized(lck) {
					System.out.println("waiting..");
					try { lck.wait(); } catch (InterruptedException e) { System.out.println("interrupted");}
				}
				System.out.println("done waiting..");
			}
		}).start();

		new Thread(new Junk()).start();
		
	}
	
	@SuppressWarnings("resource")
	@Override
	public void run() {
		long trimmark =4;
		FileChannel DriveChannel = null;
		int UNITCAPACITY = 1000;
		int ENTRYSIZE = 1280;

		byte[] ser = null;
		try {
			DriveChannel = new RandomAccessFile("C:\\temp\\bar.txt", "rw").getChannel();
/*			ser = CorfuUtil.ObjectSerialize(new Long(trimmark));
			DriveChannel.position(UNITCAPACITY*ENTRYSIZE+ UNITCAPACITY*2/8);
			DriveChannel.write(ByteBuffer.wrap(ser));
*/
			int sz = CorfuUtil.ObjectSerialize(new Long(0)).length;
			ByteBuffer tb = ByteBuffer.allocate(sz);
			DriveChannel.position(UNITCAPACITY*ENTRYSIZE+ UNITCAPACITY*2/8);
			if (DriveChannel.read(tb) != sz) {
				System.out.println("cannot read");
				return;
			}
			trimmark = ((Long)CorfuUtil.ObjectDeserialize(tb.array())).longValue();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("deserialized: " + trimmark);
		
		synchronized(lck) { lck.notify(); }

	}
}

