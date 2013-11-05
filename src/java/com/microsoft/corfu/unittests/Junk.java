package com.microsoft.corfu.unittests;

import com.microsoft.corfu.*;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.microsoft.corfu.LogHeader;

public class Junk {
	int a , b;
	class foo {
		Junk j;
		int bar;
	}
	
	static void foo(ArrayList<ByteBuffer> p, com.microsoft.corfu.LogHeader R) {
		p.add(ByteBuffer.allocate(50));
		R.off = 1; R.ngrains = 1;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		MetaInfo inf = new MetaInfo();
		LogEntryWrap le = new LogEntryWrap(CorfuErrorCode.OK, new MetaInfo(), null);
		le.nextinf.metaFirstOff = 1;
		System.out.println("le firstoff=" + le.nextinf.metaFirstOff);
		inf = le.nextinf;
		System.out.println("inf firstoff=" + inf.metaFirstOff);

		le = new LogEntryWrap(CorfuErrorCode.OK, new MetaInfo(), null);
		le.nextinf.metaFirstOff = 2;
		System.out.println("le firstoff=" + le.nextinf.metaFirstOff);
		System.out.println("inf firstoff=" + inf.metaFirstOff);
		
		
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
		
	}

}
