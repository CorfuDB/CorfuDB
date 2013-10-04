package com.microsoft.corfu;

import java.nio.ByteBuffer;
import java.util.BitSet;

public class jnk {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		BitSet bs = new BitSet(10);
		
		bs.set(4);
		
		for (int i = 0; i < 10; i++)
			System.out.println("next set bit (" + i + ") = " + bs.nextSetBit(i));
	}
}
