package com.microsoft.corfu.unittests;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class Junk {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ArrayList<ByteBuffer> ar;
		
		ar = new ArrayList<ByteBuffer>(10);
		
		System.out.println("ar.size() " + ar.size());
		ar.add(ByteBuffer.allocate(5));
		
		ByteBuffer md = ar.get(0);
		System.out.println("buf at pos 0 capacity: " + md.capacity());
		ar.set(0, ByteBuffer.allocate(100));
		System.out.println("md capacity: " + md.capacity());
		System.out.println("final buf at pos 0 capacity: " + ar.get(0).capacity());
	}

}
