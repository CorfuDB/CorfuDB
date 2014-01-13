package com.microsoft.corfu.unittests;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

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
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		new Thread(new Junk()).start();
	}
	
	@Override
	public void run() {
		byte b = (1 << 3);
		
		b &= ~(1<<3);
		System.out.println(Integer.toHexString(b));

	}
}

