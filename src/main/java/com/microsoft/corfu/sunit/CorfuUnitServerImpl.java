package com.microsoft.corfu.sunit;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;

import org.slf4j.*;

import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import com.microsoft.corfu.*;
import com.microsoft.corfu.sunit.CorfuUnitServer;

public class CorfuUnitServerImpl implements CorfuUnitServer.Iface {
	private static Logger slog = LoggerFactory.getLogger(CorfuUnitServerImpl.class);
	private Logger log = LoggerFactory.getLogger(CorfuUnitServerImpl.class);

	private static int UNITCAPACITY; // taken from configuration: capacity in ENTRYSIZE units, i.e. UNITCAPACITY*ENTRYSIZE bytes
	private static int ENTRYSIZE;	// taken from configuration: individual log-entry size in bytes
	private static int PORT;		// taken from configuration: port number this unit listens on
	private static String DRIVENAME = null; // command line argument: where to persist data (unless rammode is on)
	private static boolean RAMMODE = false; // command line switch: work in memory (no data persistence)
	private static boolean RECOVERY = false; // command line switch: indicate whether we load log from disk on startup

	private ByteBuffer[] inmemoryStore; // use in rammode
	
	private TreeMap<Integer, ExtntInfo> inmemoryMeta = new TreeMap<Integer, ExtntInfo>(); // map of extents on this unit, for efficient lookup 
		
	private long trimmark = 0; // log has been trimmed up to this position (excl)
	private int ckmark = 0; // start offset of latest checkpoint. TODO: persist!!

	private FileChannel DriveChannel = null;
	private Object DriveLck = new Object();
	
	/**
	 * utility class. 
	 * Maintain a bit-set with standard bitSet operations, 
	 * whose index-range wraps around automatically when we reach 'cap', the end of the range.
	 *
	 */
	class CyclicBitSet {
		private int cap = 0;  // capacity in bits
		private int leng = 0; // a convenience: length in bytes
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
			leng = size/8;
		}
		
		public byte[] toByteArray() { return map; }
		public ByteBuffer toArray(int fr, int to) { 
			int find = fr/8;
			int tind = (int)Math.ceil((double)to/8);
			return ByteBuffer.wrap(map, find, tind-find); 
		}
		
		int ci(int ind) { return (ind+cap) % cap; } // the cyclic-index converter
		
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
			
			for (int k = (ind+1)%leng; k != ind; k = (k+1)%leng) {
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
			
			for (int k = (ind+1)%leng; k != ind; k = (k+1)%leng) {
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
			
			int find = fr/8; int tind = to/8;
			for (int j = fr%8; j < 8; j++)
				map[find] |= 1 << (7-j);
				
			for (int k = find+1; k != tind; k = (k+1)%leng) {
				map[k] = (byte)0xFF;
			}
			
			for (int j = 0; j < to%8; j++)
				map[tind] |= 1 << (7-j);
		}

		public void clear(int ind) { 
			map[(ci(ind)/8)] &= ~(1 << (7-(ind%8)));
		}

		public void clear(int fr, int to) { 			
			fr = ci(fr); to = ci(to);
			
			int find = fr/8; int tind = to/8;
			for (int j = fr%8; j < 8; j++)
				map[find] &= ~(1 << (7-j));
				
			for (int k = find+1; k != tind; k = (k+1)%leng) {
				map[k] = 0;
			}
			
			for (int j = 0; j < (to%8); j++)
				map[tind] &= ~(1 << (7-j));
		}
	}
	
	private CyclicBitSet storeMap;
	
	// mark the range from 'from' (incl) to 'to' (excl) as occupied by one extent.
	// there are several types of extent marks on a storage unit: 
	//   - global start filled : we mark the beginning with bits [111]
	//   - global middle : we mark the beginning with bits [101]
	//   - global skipped : we mark the beginning with bits [011]
	//   - empty : [100]
	// (a range starting with [000] is unwritten)
	// 
	// all bits within the range up to the next extent mark remain clear.
	//
	private void markExtntSet(int fr, int to, ExtntMarkType et) {
		int firstInd = 3* fr ;   // incl
		int lastInd = 3* to ;	// excl
		
		// mark the beginning
		//
		switch (et) {
		
		case EX_EMPTY:
			storeMap.set(firstInd);
			storeMap.clear(firstInd+1);
			storeMap.clear(firstInd+2);
			break;
		
		case EX_BEGIN:
			storeMap.set(firstInd);
			storeMap.set(firstInd+1);
			storeMap.set(firstInd+2);
			break;
			
		case EX_MIDDLE:
			storeMap.set(firstInd);
			storeMap.clear(firstInd+1);
			storeMap.set(firstInd+2);
			break;
			
		case EX_SKIP:
			storeMap.clear(firstInd);
			storeMap.set(firstInd+1);
			storeMap.set(firstInd+2);
			break;
			
		}
		
		// now mark the ending 
		//
		
		if (storeMap.get(lastInd) || storeMap.get(lastInd+1)) // there is already an extent marked here
			return;
		storeMap.set(lastInd); // mark it as EMPTY
	}
	
	private void markRangeClear(int fr, int to) {
		int firstInd = 3* fr; // incl
		int lastInd = 3*to; // excl
		storeMap.clear(firstInd, lastInd);
	}
	
	private void addmetainfo(long fromOff, int leng, ExtntMarkType et) {
		inmemoryMeta.put((int) (fromOff % UNITCAPACITY), new ExtntInfo(fromOff,  leng,  et));
	}
	
	private ExtntInfo getmetainfo(int off) {
		return inmemoryMeta.get(off);
	}
	
	private boolean ismetaconflict(int from, int to) {
		
		Entry<Integer, ExtntInfo> conflct;
		int f;
		int t;
		conflct = inmemoryMeta.floorEntry(from);
		
		if (to <= from) {
			while (conflct != null) {
				t = (int)conflct.getValue().getMetaFirstOff() + conflct.getValue().getMetaLength();
				if (conflct.getValue().getFlag() != ExtntMarkType.EX_SKIP &&
					(t > from)) return true;
				conflct = inmemoryMeta.ceilingEntry(t);
			}
			conflct = inmemoryMeta.ceilingEntry(0);

			while (conflct != null &&
				(f = (int)conflct.getValue().getMetaFirstOff()) < to) {
				if (conflct.getValue().getFlag() != ExtntMarkType.EX_SKIP) return true;
				conflct = inmemoryMeta.ceilingEntry(f + conflct.getValue().getMetaLength());
			}
		}
		else {
			while (conflct != null &&
					(f = (int)conflct.getValue().getMetaFirstOff()) < to) {
				if (conflct.getValue().getFlag() != ExtntMarkType.EX_SKIP &&
						f >= from) return true;
				conflct = inmemoryMeta.ceilingEntry(f + conflct.getValue().getMetaLength());
			}
		}
		return false;
	}
	
	private void trimmetainfo(int fr, int to) {
		Set<Integer> ks = inmemoryMeta.keySet();
		SortedMap<Integer, ExtntInfo> hm;
		CopyOnWriteArrayList<Integer> hs;
		if (to <= fr) {
			hm = inmemoryMeta.subMap(fr, UNITCAPACITY);
			hs = new CopyOnWriteArrayList<Integer>(hm.keySet());
			ks.removeAll(hs);
			hm = inmemoryMeta.subMap(0, to);
			hs = new CopyOnWriteArrayList<Integer>(hm.keySet());
			ks.removeAll(hs);
		}
		else {
			hm = inmemoryMeta.subMap(fr, to);
			hs = new CopyOnWriteArrayList<Integer>(hm.keySet());
			ks.removeAll(hs);
		}
	}
	
	private void reconstructExtntMap() {
		int off;
		
		int start, next;
		int fr, to;
		ExtntMarkType et = ExtntMarkType.EX_BEGIN;
		ExtntInfo inf;
		off = 3* (int) (trimmark % UNITCAPACITY);
		next = storeMap.nextSetBit(off); 
		assert(next/3 == (int) (trimmark % UNITCAPACITY));

		do {
			start = next;
			log.debug("start set bit: {}", start);
			if (start < 0) break;

			fr = start/3;	
			
			if (!storeMap.get(3*fr+2)) et = ExtntMarkType.EX_EMPTY;
			else if (storeMap.get(3*fr) && storeMap.get(3*fr+1)) et = ExtntMarkType.EX_BEGIN;
			else if (storeMap.get(3*fr) && !storeMap.get(3*fr+1)) et = ExtntMarkType.EX_MIDDLE;
			else if (!storeMap.get(3*fr) && storeMap.get(3*fr+1)) et = ExtntMarkType.EX_SKIP;
			log.debug("extent mark {} (bits are {} {} {} )", et, storeMap.get(3*fr), storeMap.get(3*fr+1),storeMap.get(3*fr+2));
			
			next = storeMap.nextSetBit(3*fr+3);
			log.debug("next set bit: {}", start);

			to = next/3;
			if (et != ExtntMarkType.EX_SKIP && et != ExtntMarkType.EX_EMPTY)
				if (to < fr)
					addmetainfo(trimmark+fr, to+UNITCAPACITY-fr, et);
				else
					addmetainfo(trimmark+fr, to-fr, et);
			log.info("reconstructed extent {}..{} type={}", fr, to, et);
			
			if (to == (int) (trimmark % UNITCAPACITY)) break; // wrapped-around, done!
		} while (true) ;
	}
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		CorfuConfigManager CM = new CorfuConfigManager(new File("./0.aux"));
		int sid = -1;
		ENTRYSIZE = CM.getGrain();
		UNITCAPACITY = CM.getUnitsize(); 
		String Usage = "Usage: " + CorfuUnitServer.class.getName() + "-unit <unitnumber> " +
				"<-rammode> | <-drivename <name> [-recover]>";
		
		for (int i = 0; i < args.length; ) {
			if (args[i].startsWith("-recover")) {
				RECOVERY = true;
				slog.info("recovery mode");
				i += 1;
			} else if (args[i].startsWith("-rammode")) {
				RAMMODE = true;
				slog.info("working in RAM mode");
				i += 1;
			} else if (args[i].startsWith("-unit") && i < args.length-1) {
				sid = Integer.valueOf(args[i+1]);
				slog.info("unit number: " + sid);
				i += 2;
			} else if (args[i].startsWith("-drivename") && i < args.length-1) {
				DRIVENAME = args[i+1];
				slog.info("drivename: " + DRIVENAME);
				i += 2;
			} else {
				slog.error("unknown param: " + args[i]);
				throw new Exception(Usage);
			}
		}
		
		if ((!RAMMODE && DRIVENAME == null) || sid < 0) {
			slog.error("missing drive name or unit number!");
			throw new Exception(Usage);
		}
		if (RAMMODE && RECOVERY) {
			slog.error("cannot do recovery in rammode!");
			throw new Exception(Usage);
		}
		
		CorfuNode[] cn = CM.getGroupByNumber(0);
		if (cn.length < sid) {
			slog.error("unit id {} exceeds group size {}; quitting", sid, cn.length);
			throw new Exception("bad sunit #"); 
		}		
		PORT = cn[sid].getPort();
		
		slog.info("unit server #{} starting; port={}, entsize={} capacity={}",
				sid, PORT, ENTRYSIZE, UNITCAPACITY);
		
		new Thread(new Runnable() {
			@Override
			public void run() {
				
				TServer server;
				TServerSocket serverTransport;
				CorfuUnitServer.Processor<CorfuUnitServerImpl> processor; 
				System.out.println("run..");
		
				try {
					serverTransport = new TServerSocket(CorfuUnitServerImpl.PORT);
					processor = 
							new CorfuUnitServer.Processor<CorfuUnitServerImpl>(new CorfuUnitServerImpl());
					server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
					System.out.println("Starting Corfu storage unit server on port " + CorfuUnitServerImpl.PORT);
					
					server.serve();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}}).run();
	
		}
	
	////////////////////////////////////////////////////////////////////////////////////
	
	public CorfuUnitServerImpl() throws Exception {
		
		log.warn("CurfuClientImpl logging level = dbg?{} info?{} warn?{} err?{}", 
				log.isDebugEnabled(), log.isInfoEnabled(), log.isWarnEnabled(), log.isErrorEnabled());

		if (!RAMMODE) {
			try {
				DriveChannel = new RandomAccessFile(DRIVENAME, "rw").getChannel();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				System.exit(1); // not much to do without storage...
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1); // not much to do without storage...
			}

			// fork off a thread to constantly force syncing to disk
			//
			new Thread(new Runnable() {
				@Override
				public void run() {
					for(;;) {
						try {
							DriveChannel.force(false);
							synchronized(DriveLck) { DriveLck.notifyAll(); }
							Thread.sleep(1);
						} catch(Exception e) {
							e.printStackTrace();
						}
					}
				}
			}).start();
		}
		else {	
			inmemoryStore = new ByteBuffer[UNITCAPACITY]; 
		}
		
		if (RECOVERY) {
			recover();
		} else {
			storeMap = new CyclicBitSet(3*UNITCAPACITY); // TODO if UNITCAPACITY is more than MAXINT, 
			markExtntSet(0, UNITCAPACITY, ExtntMarkType.EX_EMPTY);
			if (!RAMMODE) writebitmap(0, 1);
		}
		// make storeMap a list of bitmaps, each one of MAXINT size

	}
	
	private void writebitmap(int from, int to) throws IOException {
		if (to <= from) {
			DriveChannel.position(UNITCAPACITY*ENTRYSIZE+(3*from/8));
			DriveChannel.write( storeMap.toArray(3*from, 3*UNITCAPACITY) );
	
			DriveChannel.position(UNITCAPACITY*ENTRYSIZE+0);
			DriveChannel.write( storeMap.toArray(0, 3*to) );
	
		} else {
			DriveChannel.position(UNITCAPACITY*ENTRYSIZE+(3*from/8));
			DriveChannel.write( storeMap.toArray(3*from, 3*to) );
		}
	}
	
	private void writetrimmark() throws IOException {
		DriveChannel.position(UNITCAPACITY*ENTRYSIZE+ UNITCAPACITY*3/8);
		byte[] ser = CorfuUtil.ObjectSerialize(new Long(trimmark));
		log.debug("trimmark serialization sz={} ctnt={}", ser.length, ser);
		DriveChannel.write(ByteBuffer.wrap(ser));
	}
	
	private void recover() throws Exception {
		
		long filesz =  DriveChannel.size();
		int sz = CorfuUtil.ObjectSerialize(new Long(0)).length; // size of extra info after bitmap
		log.debug("expecting drive sz={} + bitmap sz={} + trimmark sz={} : found {}",
				UNITCAPACITY*ENTRYSIZE, UNITCAPACITY*3/8, sz, filesz);
	
		DriveChannel.position(UNITCAPACITY*ENTRYSIZE);
		ByteBuffer bb = ByteBuffer.allocate(UNITCAPACITY*3/8);
		DriveChannel.read(bb);
/*		log.debug("recovery bitmap: {}", bb);
		for (int k = 0; k < bb.capacity(); k++) log.debug("bb[{}] {}", k, bb.get(k));
*/		
		ByteBuffer tb = ByteBuffer.allocate(sz);
		DriveChannel.position(UNITCAPACITY*ENTRYSIZE+ UNITCAPACITY*3/8);
		if (DriveChannel.read(tb) == sz) {
			log.debug("trimmark deseralize read {} bytes: {}", sz, tb.array());
			trimmark = ((Long)CorfuUtil.ObjectDeserialize(tb.array())).longValue();
		} else {
			log.info("no trimmark saved, setting initial trim=0");
			trimmark=0;
		}
		storeMap = new CyclicBitSet(bb.array(), UNITCAPACITY*2);
		reconstructExtntMap();
	}
	
	private void writebufs(int from, int to, List<ByteBuffer> wbufs) throws IOException {
		DriveChannel.position(from*ENTRYSIZE);
		int global_ind = from, array_ind = 0;
		
		if ( (to <= from && UNITCAPACITY+to-from < wbufs.size() ) ||
				(to > from && to-from < wbufs.size()) ) {
			log.error("internal error writebufs parameter has too many entries");
		}
		
		while (array_ind < wbufs.size()) {
			DriveChannel.write(wbufs.get(array_ind));
			array_ind++;
			if (++global_ind >= UNITCAPACITY) {
				DriveChannel.position(0);
				global_ind = 0;
			}
		}
	}
	
	private ArrayList<ByteBuffer> readbufs(int from, int to, int length) throws IOException {
		ArrayList<ByteBuffer> bb = new ArrayList<ByteBuffer>(length);
		if (to <= from) {
			ByteBuffer buf1 = ByteBuffer.allocate((UNITCAPACITY-from)*ENTRYSIZE),
					buf2 = ByteBuffer.allocate(to*ENTRYSIZE);
			DriveChannel.read(buf1, from*ENTRYSIZE);
			for (int i= 0; i < (UNITCAPACITY-from); i++) 
				bb.add(ByteBuffer.wrap(buf1.array(), i*ENTRYSIZE, ENTRYSIZE));
			DriveChannel.read(buf2, 0);
			for (int i= 0; i < to; i++) 
				bb.add(ByteBuffer.wrap(buf2.array(), i*ENTRYSIZE, ENTRYSIZE));

		} else {
			ByteBuffer buf  = ByteBuffer.allocate(length*ENTRYSIZE);
			DriveChannel.read(buf, from*ENTRYSIZE);
			for (int i= 0; i < length; i++) 
				bb.add(ByteBuffer.wrap(buf.array(), i*ENTRYSIZE, ENTRYSIZE));
		}
		
		return bb;
	}
		
	private void addrambufs(int from, int to, List<ByteBuffer> wbufs) {
		int array_ind = 0, global_ind = from;
		int sz;

		if (to <= from) 
			sz = UNITCAPACITY+to-from;
		else
			sz = to-from;
		
		while (array_ind < sz ) {
			if (wbufs.size() <= array_ind) inmemoryStore[global_ind] = ByteBuffer.allocate(0);
			else inmemoryStore[global_ind] = wbufs.get(array_ind); 
			array_ind++;
			if (++global_ind >= UNITCAPACITY) global_ind = 0;
		}
	}

	private ArrayList<ByteBuffer> extractrambufs(int from, int to, int length) {
		ArrayList<ByteBuffer> ret = new ArrayList<ByteBuffer>();

		if (to <= from) {
			for (int i= from; i < UNITCAPACITY; i++) 
				ret.add(inmemoryStore[i]);
			for (int i= 0; i < to; i++) 
				ret.add(inmemoryStore[i]);
		} else {
			for (int i= from; i < to; i++) 
				ret.add(inmemoryStore[i]);
		}
		return ret;
	}
		
	/**
	 * utility function to handle incoming log extent. depending on mode, if RAMMODE, it holds a pointer to the entry buffer in memory, 
	 * otherwise, it copies it into store.
	 * 
	 * @param relOff the physical offset to write to
	 * @param buf the buffer-array to store
	 * @param inf meta information on the extent that this entry belongs to
	 */
	private void RamToStore(List<ByteBuffer> wbufs, ExtntInfo inf) {
		
		int from = (int) (inf.getMetaFirstOff()%UNITCAPACITY);
		int to = (int) ((inf.getMetaFirstOff()+inf.getMetaLength())%UNITCAPACITY);
		ExtntMarkType et = inf.getFlag();
		if (RAMMODE) {
			if (et != ExtntMarkType.EX_SKIP) addrambufs(from, to, wbufs);
			markExtntSet(from, to, et);
		} else {
			try {
				if (et != ExtntMarkType.EX_SKIP) writebufs(from, to, wbufs);
				markExtntSet(from, to, et);
				writebitmap(from, to+1);
			} catch (IOException e) {
				log.warn("cannot write entry {} to store, IO error; quitting", inf);
				e.printStackTrace();
				System.exit(1);
			}
		}
		addmetainfo(inf.getMetaFirstOff(), inf.getMetaLength(), et);
	}
		
	/**
	 * utility function to bring into memory a log entry. depending on mode, if RAMMODE, simply return a point to the in-memory buffer, 
	 * otherwise, it read the buffer from store.
	 * we also fill-up 'inf' here with the meta-info of the extent that this entry belongs to.
	 * 
	 * @param relOff the log-offset we would like to obtain
	 * @return an entryplus object with pointers to a ByteBuffer containing the content and an ExtntInfo containing meta-ingo
	 */
	private ArrayList<ByteBuffer> StoreToRam(ExtntInfo inf)  {
		int from = (int) (inf.getMetaFirstOff()%UNITCAPACITY);
		int to = (int) ((inf.getMetaFirstOff()+inf.getMetaLength())%UNITCAPACITY);
		ArrayList<ByteBuffer> ret = null;

		if (RAMMODE) {
			ret = extractrambufs(from, to, inf.getMetaLength());		
		} else {
			try {
				ret = readbufs(from, to, inf.getMetaLength());
			} catch (IOException e) {
				log.warn("cannot retrieve entry {} from store, IO error; quitting", inf);
				e.printStackTrace();
				System.exit(1);
			}
		}
		return ret;
	}
			
	/* (non-Javadoc)
	 * implements to CorfuUnitServer.Iface write() method.
	 * @see com.microsoft.corfu.sunit.CorfuUnitServer.Iface#write(com.microsoft.corfu.ExtntWrap)
	 * 
	 * we make great effort for the write to either succeed in full, or not leave any partial garbage behind. 
	 * this means that we first check if all the pages to be written are free, and that the incoming entry contains content for each page.
	 * in the event of some error in the middle, we reset any values we already set.
	 */
	@Override
	synchronized public CorfuErrorCode write(ExtntInfo inf, List<ByteBuffer> ctnt) throws org.apache.thrift.TException {
		long fromOff = inf.getMetaFirstOff(), toOff = fromOff + inf.getMetaLength();
		
		log.debug("write({})", inf);
		
		// ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
		// from here until the next '^^^^..' mark : 
		// code to verify that there is room to write the entire multi-page entry in one shot, and not overwrite any filled pages
		//
		if (toOff - trimmark > UNITCAPACITY) {
			log.warn("unit full ! trimmark= {} fill[{}..{}]", trimmark, fromOff, toOff);
			return CorfuErrorCode.ERR_FULL; 
		}
		
		if (fromOff < trimmark) {
			log.info("attempt to overwrite trimmed! [{}..{}]", fromOff, toOff);
			return CorfuErrorCode.ERR_OVERWRITE; 
		}
		
		int relFromOff = (int) (fromOff % UNITCAPACITY), relToOff = (int) (toOff % UNITCAPACITY);
		
		if (ismetaconflict(relFromOff, relToOff)) {
			log.info("write({}) overwrites data, rejected; trimmark={} ", inf, trimmark);
			return CorfuErrorCode.ERR_OVERWRITE;
		}
		// ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

		RamToStore(ctnt, inf);
		return CorfuErrorCode.OK; 
    }
	
	/**
	 * mark an extent 'skipped'
	 * @param inf the extent 
	 * @return OK if succeeds in marking the extent for 'skip'
	 * 		ERROR_TRIMMED if the extent-range has already been trimmed
	 * 		ERROR_OVERWRITE if the extent is occupied (could be a good thing)
	 * 		ERROR_FULL if the extent spills over the capacity of the log
	 * @throws TException 
	 */
	@Override
	synchronized public CorfuErrorCode fix(ExtntInfo inf) throws TException {
		return write(inf, new ArrayList<ByteBuffer>());
	}

	/**
	 * a great portion of the unit-server read() method is dedicated to error-handling,
	 * so the ErrorHelper class is an attempt to reduce repetition of error-handling code
	 *
	 * @param <T> the return type from read()
	 */
	interface ErrorHelper<T> {
		/** helper to construct a ExtntWrap record and return in case of read error
		 * @param er the returned error-code
		 * @param args expects three longs: from-offset, to-offset, and bad-offset
		 * @return a ExtntWrap with empty contents and the requested error-code
		 */
		T badoffHelper(CorfuErrorCode er, long... args);
		
		/** helper to construct a ExtntWrap record and return in case of meta-info problem 
		 * @param reqinfo the meta-info requested for this read
		 * @param ckinf the meta-info retrieved (and causing the problem)
		 * @return a ExtntWrap with empty contents and an appropriate error-code
		 */
		T badmetaHelper(ExtntInfo reqinfo, ExtntInfo ckinf);
	}
	ErrorHelper<ExtntWrap> retval = new ErrorHelper<ExtntWrap>() {
		@Override
		public ExtntWrap badoffHelper(CorfuErrorCode er, long... args) {
			log.debug("error reading [{}..{}] errval={}", 
					args[0] /* from Off */, 
					args[1] /* to Off */, 
					er);
			return new ExtntWrap(er, new ExtntInfo(0, 0, ExtntMarkType.EX_EMPTY), new ArrayList<ByteBuffer>());
		}

		@Override
		public ExtntWrap badmetaHelper(ExtntInfo reqinfo, ExtntInfo ckinf) {
			if (ckinf.getFlag() == ExtntMarkType.EX_SKIP) {
				log.debug("read({}): SKIP", ckinf);
				return new ExtntWrap(CorfuErrorCode.OK_SKIP, null, null);
			} else {
				log.info("ExtntInfo mismatch expecting {} received {}", reqinfo, ckinf);
				return new ExtntWrap(CorfuErrorCode.ERR_BADPARAM, new ExtntInfo(0, 0, ExtntMarkType.EX_EMPTY), new ArrayList<ByteBuffer>());
			}

		}
	};
	
	/* (non-Javadoc)
	 * @see com.microsoft.corfu.sunit.CorfuUnitServer.Iface#read(com.microsoft.corfu.CorfuHeader, com.microsoft.corfu.ExtntInfo)
	 * 
	 * this method performs actual reading of a range of pages.
	 * it fails if any page within range has not been written.
	 * it returns OK_SKIP if it finds any page within range which has been junk-filled (i.e., the entire range becomes junked).
	 * 
	 * the method also reads-ahead the subsequent meta-info entry if hdr.readnext is set.
	 * if the next meta info record is not available, it returns the current meta-info structure
	 * 
	 *  @param a CorfuHeader describing the range to read
	 */
	@Override
	synchronized public ExtntWrap read(CorfuHeader hdr) throws org.apache.thrift.TException {
		
		ExtntInfo inf = hdr.getExtntInf();
		long fromOff = inf.getMetaFirstOff(), toOff = fromOff + inf.getMetaLength();
		log.debug("read [{}..{}] trim={} CAPACITY={}", fromOff, toOff, trimmark, UNITCAPACITY);
		
		// check that we can satisfy this request in full, up to '^^^^^^^^^^^^^' mark
		//
		
		if ((toOff - trimmark) > UNITCAPACITY) 
			return retval.badoffHelper(CorfuErrorCode.ERR_UNWRITTEN, fromOff, toOff);
		
		if (fromOff < trimmark)
			return retval.badoffHelper(CorfuErrorCode.ERR_TRIMMED, fromOff, toOff, fromOff);
		
		int relFromOff = (int) (fromOff % UNITCAPACITY), relToOff = (int) (toOff % UNITCAPACITY);
		ExtntInfo mapinf = getmetainfo(relFromOff);
		if (mapinf == null) 
			return retval.badoffHelper(CorfuErrorCode.ERR_UNWRITTEN, fromOff, toOff);
		if (mapinf.getMetaLength() != inf.getMetaLength())
			return retval.badoffHelper(CorfuErrorCode.ERR_BADPARAM, fromOff, toOff);

		// ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
		// actual reading starts here (we already checked that entire range is written)
		
		ArrayList<ByteBuffer> wbufs = StoreToRam(inf);

		ExtntInfo prefetch;
		if (hdr.isPrefetch() && (prefetch = getmetainfo((int) (hdr.getPrefetchOff() % UNITCAPACITY ))) != null) {
			return new ExtntWrap(CorfuErrorCode.OK, prefetch, wbufs); 
		} else {
			log.debug("read prefetch {} not available, returning {}", hdr.getPrefetchOff(), inf);
			return new ExtntWrap(CorfuErrorCode.OK, inf, wbufs);
		}
	}
	
	/* read the meta-info record at specified offset
	 * 
	 * @param off- the offset to read from
	 * @return the meta-info record "wrapped" in ExtntWrap. 
	 *         The wrapping contains error code: UNWRITTEN if reading beyond the tail of the log
	 * 
	 * (non-Javadoc)
	 * @see com.microsoft.corfu.sunit.CorfuUnitServer.Iface#readmeta(long)
	 */
	@Override
	synchronized public ExtntWrap readmeta(long off) {
		log.debug("readmeta({})", off);
		if (off < trimmark)
			return retval.badoffHelper(CorfuErrorCode.ERR_TRIMMED, off, off, off);
		if ((off - trimmark) >= UNITCAPACITY )
			return retval.badoffHelper(CorfuErrorCode.ERR_UNWRITTEN, off, off, off );
		
		ExtntInfo inf = getmetainfo((int) (off % UNITCAPACITY));
		if (inf == null ) 
			return retval.badoffHelper(CorfuErrorCode.ERR_UNWRITTEN, off, off, off );

		return new ExtntWrap(CorfuErrorCode.OK, inf, new ArrayList<ByteBuffer>());
	}

	/**
	 * wait until any previously written log entries have been forced to persistent store
	 */
    @Override
	synchronized public void sync() throws org.apache.thrift.TException {
    	synchronized(DriveLck) { try { DriveLck.wait(); } catch (Exception e) {
    		log.error("forcing sync to persistent store failed, quitting");
    		System.exit(1);
    	}}
    }

	@Override
	synchronized public long querytrim() {	return trimmark; } 
	
	@Override
	synchronized public long queryck() {	return ckmark; } 
	
	@Override
	synchronized public boolean trim(long mark) throws org.apache.thrift.TException {
		
		if (mark <= trimmark) return true;	
		if (mark - trimmark > UNITCAPACITY) {
			log.warn("attempt to trim beyond log capacity from {} to {}", trimmark, mark);
			return false;
		}
		
		int relMark = (int) (mark % UNITCAPACITY), relTrimmark = (int) (trimmark % UNITCAPACITY);
		
		if (getmetainfo(relMark) == null) {
			log.warn("attempt to trim at offset {} and no extent starts there", mark);
			return false;
		}
     	log.info("log trimmed from {} to {}", trimmark, mark);
   
    	markExtntSet(relTrimmark, relMark, ExtntMarkType.EX_EMPTY);
    	trimmetainfo(relTrimmark, relMark);
    	
    	trimmark = mark;
    	if (!RAMMODE) {
	    	try {
	    	   	writebitmap(relTrimmark, relMark);
	    	   	log.debug("forcing bitmap and trimmark to disk");
	    	   	synchronized(DriveLck) {
	    	   		try { DriveLck.wait(); } catch (InterruptedException e) {	    	   	
		        		log.error("forcing sync to persistent store failed, quitting");
		        		System.exit(1);
	    	   		}
	        	}
	    	    writetrimmark();
	        	log.info("trimmark persisted to disk");
			} catch (IOException e) {
				log.error("writing trimmark failed");
				e.printStackTrace();
				return false;
			}
    	}
    	return true;
	}
	
	@Override
    synchronized public void ckpoint(long off) throws org.apache.thrift.TException {
		log.info("mark latest checkpoint offset={}", off);
		if (off > ckmark) ckmark = (int) (off % UNITCAPACITY);
	}

}
