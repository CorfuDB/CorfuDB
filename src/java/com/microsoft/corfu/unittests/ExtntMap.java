package com.microsoft.corfu.unittests;


import java.nio.ByteBuffer;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.corfu.ExtntInfo;
import com.microsoft.corfu.ExtntMarkType;

public class ExtntMap implements Runnable {
	private Logger log = LoggerFactory.getLogger(ExtntMap.class);

	private static int UNITCAPACITY = 1000; // capacity in ENTRYSIZE units, i.e. UNITCAPACITY*ENTRYSIZE bytes
	private static int ENTRYSIZE = 128;
	private static long trimmark = 0;
	
	private TreeMap<Long, ExtntInfo> inmemoryMeta = new TreeMap<Long, ExtntInfo>(); 
	
	class CyclicBitSet {
		private int cap = 0;
		private int leng = 0;
		private byte[] map = null;
		
		public CyclicBitSet(int size) throws Exception { 
			if (size % 8 != 0) throw new Exception("CyclicBitSet size must be a multiple of 8");
			leng = size/8;
			map = new byte[leng];
			cap = size; 
		}
		
		public CyclicBitSet(byte[] initmap, int size) throws Exception { 
			if (size % 8 != 0) throw new Exception("CyclicBitSet size must be a multiple of 8");
			map = initmap;
			cap = size; 
		}
		
		public byte[] toByteArray() { return map; }
		public ByteBuffer toArray(int fr, int to) { 
			int find = fr/8;
			int tind = (int)Math.ceil((double)to/8);
			return ByteBuffer.wrap(map, find, tind-find); 
		}
		
		int ci(int ind) { return (ind+cap) % cap; }
		public boolean get(int ind) { 
			int ci = ci(ind);
			return ((map[ci/8] >> (7-(ci % 8))) & 1) != 0;
		}
		
		/**
		 * return the index of the next set-bit subsequent to fromInd, wrapping around the end of bit-range if needed.
		 */
		public int nextSetBit(int fromInd) {
			int ind = ci(fromInd)/8; byte b = map[ind];
			for (int j = fromInd%8; j < 8; j++)
				if (((b >> (7-j)) & 1) != 0) return (ind*8+j);
			
			for (int k = ind+1; k != ind; k = (k+1)%leng) {
				if ((b = map[k]) == 0) continue;
				for (int j = 0; j < 8; j++)
					if (((b >> (7-j)) & 1) != 0) return (k*8+j);
			}
			
			b = map[ind];
			for (int j = 0; j < 8; j++)
				if (((b >> (7-j)) & 1) != 0) return (ind*8+j);

			return -1;
		}

		/**
		 * return the index of the next clear-bit subsequent to fromInd, wrapping around the end of bit-range if needed.
		 * if none found, return -1 (this is different from BitSet.nextClearBit(), which returns one above the highest defined-index, 
		 * and is meaningless here since we wrap-around).
		 */
		public int nextClearBit(int fromInd) { 
			int ind = ci(fromInd)/8; byte b = map[ind];
			for (int j = fromInd%8; j < 8; j++)
				if (((b >> (7-j)) & 1) == 0) return (ind*8+j);
			
			for (int k = ind+1; k != ind; k = (k+1)%leng) {
				if ((b = map[k]) == 0) continue;
				for (int j = 0; j < 8; j++)
					if (((b >> (7-j)) & 1) == 0) return (k*8+j);
			}
			
			b = map[ind];
			for (int j = 0; j < 8; j++)
				if (((b >> (7-j)) & 1) == 0) return (ind*8+j);

			return -1;
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
		
		public void set(int ind) { 
			int ci = ci(ind);
			map[ci/8] |= 1 << (7-(ci % 8));
		}
		
		public void set(int fr, int to) { 
			fr = ci(fr); to = ci(to);
			
			int ind = fr/8;
			for (int j = ind%8; j < 8; j++)
				map[ind] |= 1 << (7-j);
				
			for (int k = ind+1; k != to; k = (k+1)%leng) {
				map[k] = (byte)0xFF;
			}
			
			for (int j = 0; j < to%8; j++)
				map[to] |= 1 << (7-j);
		}

		public void clear(int ind) { 
			map[(ci(ind)/8)] &= ~(1 << (7-(ind%8)));
		}

		public void clear(int fr, int to) { 			
			fr = ci(fr); to = ci(to);
			
			int ind = fr/8;
			for (int j = ind%8; j < 8; j++)
				map[ind] &= ~(1 << (7-j));
				
			for (int k = ind+1; k != to; k = (k+1)%leng) {
				map[k] = 0;
			}
			
			for (int j = 0; j < to%8; j++)
				map[to] &= ~(1 << (7-j));
		}
	}
	
	private CyclicBitSet storeMap;
	
	// mark the range from 'from' (incl) to 'to' (excl) as occupied by one extent.
	// there are several types of extent marks on a storage unit: 
	//   - global start filled : we mark the beginning with bits [11]
	//   - global middle : we mark the beginning with bits [10]
	//   - global skipped : we mark the beginning with bits [01]
	// (a range starting with [00] is therefore left unwritten)
	// 
	// all bits within the range up to the next extent mark remain clear.
	//
	private void markExtntSet(int fr, int to, ExtntMarkType et) {
		int firstInd = 2*fr;  // incl
		int lastInd = 2*to;	// excl
		
		// mark the beginning
		//
		switch (et) {
		
		case EX_BEGIN:
			storeMap.set(firstInd);
			storeMap.set(firstInd+1);
			break;
			
		case EX_MIDDLE:
			storeMap.set(firstInd);
			storeMap.clear(firstInd+1);
			break;
			
		case EX_SKIP:
			storeMap.clear(firstInd);
			storeMap.set(firstInd+1);
			break;
			
		}
		
		// now mark the ending 
		//
		
		if (storeMap.get(lastInd) || storeMap.get(lastInd+1)) // there is already an extent marked here
			return;
		storeMap.set(lastInd); // by default, mark it as MIDDLE; this may be overwritten by either BEGIN or SKIPPED if needed
	}
	
	private void markRangeClear(int fr, int to) {
		int firstInd = 2*fr; // incl
		int lastInd = 2*to; // excl
		storeMap.clear(firstInd, lastInd);
	}
	
	/**
	 * verify that a range is clear for re-writing
	 * the beginning may be marked with a MIDDLE mark and considered clear, because the beginning of 
	 * a range determines if it is clear or not.
	 *
	 * @param fr beginning (incl) of range for verifying 
	 * @param to end (excl) of range
	 * @return
	 */
	private boolean isExtntClear(int fr, int to) {
		int firstInd = 2*fr;
		int lastInd = 2*to;
		return storeMap.isRangeClear(firstInd+1, lastInd);
	}
	
	/**
	 * verify that a range has been set as a BEGIN or MIDDLE extent 
	 *
	 * @param fr beginning (incl) of range for verifying 
	 * @param to end (excl) of range
	 * @return
	 */
	private boolean isExtntSet(int fr, int to) {
		int firstInd = 2*fr; // incl
		int lastInd = 2*to; // excl
		
	    return (
				// verify begin and end markers
				storeMap.get(firstInd) &&
				( storeMap.get(lastInd) || storeMap.get(lastInd+1) ) &&
				// verify inner range is all clear 
				storeMap.isRangeClear(firstInd+2, lastInd)
				);
	}
	
	/**
	 * verify that a range has been set as a SKIPPED extent 
	 *
	 * @param fr beginning (incl) of range for verifying 
	 * @param to end (excl) of range
	 * @return
	 */
	private boolean isExtntSkipped(int fr, int to) {
		int firstInd = 2*fr;
		int lastInd = 2*to;

	    return (
				// verify begin and end markers
				( !storeMap.get(firstInd) && storeMap.get(firstInd+1)) &&
				( storeMap.get(lastInd) || storeMap.get(lastInd+1) ) &&
				// verify inner range is all clear 
				storeMap.isRangeClear(firstInd+2, lastInd)
				);
	}
	
	private void addmetainfo(ExtntInfo inf) {
		inmemoryMeta.put(inf.getMetaFirstOff(), inf);
	}
	
	private ExtntInfo getmetainfo(long ind) {
		return inmemoryMeta.get(ind);
	}
	
	private void trimmetainfo(long offset) {
		SortedMap<Long, ExtntInfo> hm = inmemoryMeta.headMap(offset);
		Set<Long> ks = inmemoryMeta.keySet();
		CopyOnWriteArrayList<Long> hs = new CopyOnWriteArrayList<Long>(hm.keySet());
		ks.removeAll(hs);
	}
	
	private void reconstructExtntMap() {
		int off;
		
		int start, next;
		int fr, to;
		ExtntMarkType et = ExtntMarkType.EX_BEGIN;
		ExtntInfo inf;
		off = 2* (int) (trimmark % UNITCAPACITY);
		next = storeMap.nextSetBit(off);

		do {
			start = next;
			// log.debug("next set bit: {}", start);
			if (start < 0) break;

			fr = start/2;	
			if (getmetainfo(fr) != null) break; // wrapped-around!
			
			if (storeMap.get(2*fr) && storeMap.get(2*fr+1)) et = ExtntMarkType.EX_BEGIN;
			if (storeMap.get(2*fr) && !storeMap.get(2*fr+1)) et = ExtntMarkType.EX_MIDDLE;
			if (!storeMap.get(2*fr) && storeMap.get(2*fr+1)) et = ExtntMarkType.EX_SKIP;
			
			next = storeMap.nextSetBit(start+2);
			if (next == start) {
				log.info("reconstructExtntMap problem, extent starting at {} has no ending ", fr);
				break;
			}

			to = next/2;
			// log.debug("verifying range [{}..{}]", fr, to);
			if (to <= fr)
				inf = new ExtntInfo(fr,  to+UNITCAPACITY-fr, et);
			else
				inf = new ExtntInfo(fr,  to-fr,  et);
			addmetainfo(inf);
			log.info("reconstructed extent {}", inf);
		} while (true) ;
	}

	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		new Thread(new ExtntMap()).start();
	}
	
	@Override
	public void run() {

		try {
			storeMap = new CyclicBitSet(2* UNITCAPACITY);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		} // TODO if UNITCAPACITY is more than MAXINT, 
													// make storeMap a list of bitmaps, each one of MAXINT size
		int fr, to;
		int sz = 10;
	
		// emulate 100 writes of ten etries each
		for (int k = 0; k < 10; k++) {
			fr = k * sz;
			to = fr + sz;
			log.info("verify-clear({}..{}): {}", fr, to, isExtntClear(fr,  to));
			markExtntSet(fr,  to, ExtntMarkType.EX_BEGIN);
		}
		for (int k = 0; k < 10; k++) {
			fr = k*sz;
			to = fr + sz;
			log.info("verify-set({}..{}): {}", fr, to, isExtntSet(fr,  to));
		}
		
		for (int k = 0; k < 10; k++) {
			fr = (UNITCAPACITY-sz + k*sz ) % UNITCAPACITY;
			to = (fr + sz) % UNITCAPACITY;
			log.info("verify-clear({}..{}): {}", fr, to, isExtntClear(fr,  to));
			markExtntSet(fr, to, ExtntMarkType.EX_BEGIN);
		}
		for (int k = 0; k < 10; k++) {
			fr = (UNITCAPACITY-sz + k*sz ) % UNITCAPACITY;
			to = (fr + sz) % UNITCAPACITY;
			log.info("verify-set({}..{}): {}", fr, to, isExtntSet(fr,  to));
		}
		reconstructExtntMap();
		
		inmemoryMeta.clear();
		fr = 0; to = 0;
		markExtntSet(fr,  to, ExtntMarkType.EX_BEGIN);
		log.info("verify-set({}..{}): {}", fr, to, isExtntSet(fr,  to));
		
		reconstructExtntMap();
	}
}

