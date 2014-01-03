package com.microsoft.corfu.unittests;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.corfu.ExtntInfo;

public class Junk implements Runnable {
	int i;

	private static int UNITCAPACITY = 1000; // capacity in ENTRYSIZE units, i.e. UNITCAPACITY*ENTRYSIZE bytes
	private static int ENTRYSIZE = 128;
	
	private static HashMap<Integer, ExtntInfo> inmemoryMeta; // store for the meta data which would go at the end of each disk-block
	
	class CyclicBitSet extends BitSet {
		private int cap = 0;
		
		public CyclicBitSet() { super(); }
		public CyclicBitSet(int size) { super(size); cap = size; }
		
		int ci(int ind) { return (ind +cap) % cap; }
		public boolean get(int ind) { return super.get(ci(ind)); }
		
		/**
		 * return the index of the next set-bit subsequent to fromInd, wrapping around the end of bit-range if needed.
		 */
		public int nextSetBit(int fromInd) { 
			int ni = super.nextSetBit(ci(fromInd)); 
			if (ni < 0) // wraparound
				ni = super.nextSetBit(0);
			return ni;
		}

		/**
		 * return the index of the next clear-bit subsequent to fromInd, wrapping around the end of bit-range if needed.
		 * if none found, return -1 (this is different from BitSet.nextClearBit(), which returns one above the highest defined-index, 
		 * and is meaningless here since we wrap-around).
		 */
		public int nextClearBit(int fromInd) { 
			int nc = super.nextClearBit(ci(fromInd)); 
			if (nc >= cap) { // wraparound
				nc = super.nextClearBit(0);
				if (nc >= fromInd) return -1;
			}
			return nc;
		}
		
		/**
		 * check if the specified range (potentially wrapping around the end of bit-range) is completely clear
		 * @param fr first index (incl)
		 * @param to last index (excl)
		 * @return true if the entire checked range is clear
		 */
		public boolean isRangeClear(int fr, int to) {
			fr = ci(fr); to = ci(to);
			int i = nextSetBit(fr);
			if (to <= fr) return (i < 0 || (i >= to && i < fr));
			else			return (i < fr || i >= to);
		}
		
		/**
		 * check if the specified range (potentially wrapping around the end of bit-range) is completely set
		 * @param fr first index (incl)
		 * @param to last index (excl)
		 * @return true if the entire checked range is set
		 */
		public boolean isRangeSet(int fr, int to) {
			fr = ci(fr); to = ci(to);
			int i = nextClearBit(fr);
			if (to <= fr) return (i < 0 || (i >= to && i < fr));
			else			return (i < 0 || i >= to);
		}
		
		public void set(int ind) { super.set(ci(ind)); }
		
		public void set(int fr, int to) { 
			fr = ci(fr); to = ci(to);
			if (to <= fr) {
				super.set(fr, cap);
				super.set(0, to);
			} else
				super.set(fr, to); 
		}
		public void clear(int ind) { super.clear(ci(ind)); }
		public void clear(int fr, int to) { 			
			fr = ci(fr); to = ci(to);
			if (to <= fr) {
				super.clear(fr, cap);
				super.clear(0, to);
			} else
				super.clear(fr, to); 
		}
	}
	private static CyclicBitSet storeMap;
	
	private static ByteBuffer getbitrange(int fr, int to) {
		fr = (fr/8) *8;
		if (to % 8 != 0) to = (to+1)/8*8;
		return ByteBuffer.wrap(storeMap.get(fr, to).toByteArray());
	}
	
	// mark the range from 'from' (incl) to 'to' (excl) as occupied by one extent
	// 
	// an extent's bitmap starts with [101] and ends with [011]; in between all entries are set to [111]
	// so the entire range looks like this: [101 111 111 111 ... 111 011]
	//
	private static void setbitrange(int fr, int to) {
		int firstInd = 3*fr;  // incl
		int lastInd = 3*to;	// excl
		storeMap.set(firstInd, lastInd);

		// clear the second-bit on first and last bit-pairs 
		storeMap.clear(firstInd+1); 
		storeMap.clear(lastInd-2); 
	}
	
	// mark the range from 'from' (incl) to 'to' (excl) a one "skipped" extent
	// 
	// a skipped extent's bitmap starts with [100] and ends with [010]; in between all entries are clear [000]
	// so the entire range looks like this: [100 000 000 000 ... 000 010]
	//
	private static void skipbitrange(int fr, int to) {
		int firstInd = 3*fr; // incl
		int lastInd = 3*to;  // excl
		
		storeMap.clear(firstInd, lastInd);

		// set the first-bit on the first and last bit-pairs 
		storeMap.set(firstInd); 
		storeMap.set(lastInd+1); 
	}
	
	private static boolean isExtntClear(int fr, int to) {
		int firstInd = 3*fr;
		int lastInd = 3*to;
		return storeMap.isRangeClear(firstInd, lastInd);
	}
	
	private static boolean isExtntSet(int fr, int to) {
		int firstInd = 3*fr; // incl
		int lastInd = 3*to; // excl

		return (
				// verify begin and end markers
				storeMap.get(firstInd) == true &&
				storeMap.get(firstInd+1) == false &&
				storeMap.get(lastInd-2) == false &&
				storeMap.get(lastInd-1) == true &&
				// verify inner range is all set
				storeMap.isRangeSet(firstInd+2, lastInd-2)
				);
	}
	
	private static boolean isExtntSkipped(int fr, int to) {
		int firstInd = 3*fr;
		int lastInd = 3*to;

		return (
				// verify begin and end markers
				storeMap.get(firstInd) == true &&
				storeMap.get(firstInd+1) == false &&
				storeMap.get(lastInd-2) == false &&
				storeMap.get(lastInd-1) == true &&
				// verify inner range is all clear 
				storeMap.isRangeClear(firstInd+2, lastInd-2)
				);
	}
	
	private static void addmetainfo(int fr, int to, ExtntInfo inf) {
		inmemoryMeta.put(fr, inf);
	}
	
	private static ExtntInfo getmetainfo(int ind) {
		return inmemoryMeta.get(ind);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		new Thread(new Junk()).start();
	}
	
	public void run() {

		ExtntInfo i = new ExtntInfo(1000000,0,0);

		inmemoryMeta = new HashMap<Integer, ExtntInfo>();
		storeMap = new CyclicBitSet(3* (int) UNITCAPACITY); // TODO if UNITCAPACITY is more than MAXINT, 
													// make storeMap a list of bitmaps, each one of MAXINT size
		int fr, to;
		int sz = 1;
	
		// emulate 100 writes of ten etries each
		for (int k = 0; k < 10; k++) {
			fr = k * sz;
			to = fr + sz;
			i.setMetaFirstOff(fr);
			i.setMetaLength(sz);
			System.out.println("verify-clear(" + fr + ".." + to + "): " + isExtntClear(fr,  to));
			addmetainfo(fr,  to,  i);
			setbitrange(fr,  to);
		}
		for (int k = 0; k < 10; k++) {
			fr = k*sz;
			to = fr + sz;
			System.out.println("verify-set(" + fr + ".." + to + "): " + isExtntSet(fr,  to));
		}
		
		for (int k = 0; k < 10; k++) {
			fr = (UNITCAPACITY-sz + k*sz ) % UNITCAPACITY;
			to = (fr + sz) % UNITCAPACITY;
			System.out.println("verify-clear(" + fr + ".." + to + "): " + isExtntClear(fr, to));
			setbitrange(fr, to);
		}
		for (int k = 0; k < 10; k++) {
			fr = (UNITCAPACITY-sz + k*sz ) % UNITCAPACITY;
			to = (fr + sz) % UNITCAPACITY;
			System.out.println("verify-set(" + fr + ".." + to + "): " + isExtntSet(fr,  to));
		}

		fr = 0; to = 0;
		setbitrange(fr,  to);
		System.out.println("verify-set(" + fr + ".." + to + "): " + isExtntSet(fr,  to));
	}
}

