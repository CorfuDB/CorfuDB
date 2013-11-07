package com.microsoft.corfu.unittests;

import com.microsoft.corfu.*;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.microsoft.corfu.LogHeader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Junk {
	static class info {
		public info(int a, int b) {
			super();
			this.a = a;
			this.b = b;
		}

		int a, b;
		
		public info(info another) {
			this.a = another.a;
			this.b = another.b;
		}
	}

	static void foo(info inf) {
		info newinf = new info(1, 5);
		inf = new info(newinf);
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Logger log = LoggerFactory.getLogger(Junk.class);
		
		info inf = new info(0,0);
		log.info("before " + inf.a + ", " + inf.b);
		foo(inf); 
		log.info("after " + inf.a + ", " + inf.b);
		
/*		
		ArrayList<ByteBuffer> ar;		
		ar = new ArrayList<ByteBuffer>(10);
		
		System.out.println("ar.size() " + ar.size());
		ar.add(ByteBuffer.allocate(5));
		
		ByteBuffer md = ar.get(0);
		System.out.println("buf at pos 0 capacity: " + md.capacity());
		ar.set(0, ByteBuffer.allocate(100));
		System.out.println("md capacity: " + md.capacity());
		System.out.println("final buf at pos 0 capacity: " + ar.get(0).capacity());

		com.microsoft.corfu.LogHeader lh = new com.microsoft.corfu.LogHeader();
		System.out.println("header: " + lh.off + ", " + lh.ngrains);
		foo(ar, lh);
		System.out.println("ar size: " + ar.size());
		System.out.println("buf at pos 1 capacity: " + ar.get(1).capacity());
		System.out.println("header: " + lh.off + ", " + lh.ngrains);
		
		ByteArrayOutputStream bio = new ByteArrayOutputStream();
		ObjectOutputStream oo = new ObjectOutputStream(bio);
		ar.add(ByteBuffer.wrap(bio.toByteArray()));
		System.out.println("buf at pos 2 capacity: " + ar.get(2).capacity());
		
		class rr implements Serializable {
			long l;
		} 
		
		rr RR = new rr();
		oo.writeObject(RR);
		
		oo.writeObject(lh);
		oo.close();
		
		ar.add(ByteBuffer.wrap(bio.toByteArray()));
		System.out.println("ar size: " + ar.size());
		System.out.println("buf at pos 3 capacity: " + ar.get(3).capacity());
*/		
	}

}
