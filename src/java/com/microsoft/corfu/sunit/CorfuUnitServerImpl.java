package com.microsoft.corfu.sunit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;

import org.slf4j.*;

import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import com.microsoft.corfu.*;
import com.microsoft.corfu.sunit.CorfuUnitServer;
import com.microsoft.corfu.sunit.CorfuUnitServer.Iface;
import com.microsoft.corfu.sunit.CorfuUnitServer.Processor;
import com.sun.xml.internal.bind.v2.runtime.reflect.ListIterator;

public class CorfuUnitServerImpl implements CorfuUnitServer.Iface {
	private static Logger slog = LoggerFactory.getLogger(CorfuUnitServerImpl.class);
	private Logger log = LoggerFactory.getLogger(CorfuUnitServerImpl.class);

	private static long UNITCAPACITY; // capacity in ENTRYSIZE units, i.e. UNITCAPACITY*ENTRYSIZE bytes
	private static int ENTRYSIZE;
	private static int PORT;
	private static String DRIVENAME = null;
	private static boolean RAMMODE = false;

	private ByteBuffer[] inmemoryStore;
		
	private HashMap<Long, ExtntInfo> inmemoryMeta; // store for the meta data which would go at the end of each disk-block
	private long trimmark = 0; // log has been trimmed up to this position, non-inclusive
	private long ckmark = 0; // start offset of latest checkpoint. TODO: persist!!
	private FileChannel DriveChannel = null;
	
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
	private CyclicBitSet storeMap;
	
	private ByteBuffer getbitrange(int fr, int to) {
		fr = (fr/8) *8;
		if (to % 8 != 0) to = (to+1)/8*8;
		return ByteBuffer.wrap(storeMap.get(fr, to).toByteArray());
	}
	
	// mark the range from 'from' (incl) to 'to' (excl) as occupied by one extent
	// 
	// each position in the range has a status bit-triplet. 
	// in the first triplet in the range, the second bit is off (unless it is also the last triplet in a single-offset extent);
	// in the last triplet in the range, the first bit is off (unless it is also the first triplet in a single-offset extent). 
	// in the middle (if any), all bits are set. 
	//
	// an entire extent's bitmap looks like this: [101 111 111 111 ... 111 011]
	// a single offset extent looks like this: [111]
	// a two-offsets extent looks like this: [101] [011]
	//
	private void markExtntSet(int fr, int to) {
		int firstInd = 3*fr;  // incl
		int lastInd = 3*to;	// excl
		storeMap.set(firstInd+2, lastInd);

		// clear the second-bit on first and last bit-pairs 
		storeMap.clear(lastInd-3);
		storeMap.set(lastInd-2);
		storeMap.set(firstInd); 
	}
	
	private void markRangeClear(int fr, int to) {
		int firstInd = 3*fr; // incl
		int lastInd = 3*to; // excl
		storeMap.clear(firstInd, lastInd);
	}
	
	// mark the range from 'from' (incl) to 'to' (excl) a one "skipped" extent
	// 
	// a skipped extent's bitmap starts with [100] and ends with [010]; in between all entries are clear [000]
	// so the entire range looks like this: [100 000 000 000 ... 000 010]
	// or if it is a single-offset skipped-extent, it looks like this: [110] 
	//
	private void markExtntSkipped(int fr, int to) {
		int firstInd = 3*fr; // incl
		int lastInd = 3*to;  // excl
		
		storeMap.clear(firstInd, lastInd);

		// set the first-bit on the first and last bit-pairs 
		storeMap.set(firstInd); 
		storeMap.set(lastInd-1); 
	}
	
	private boolean isExtntClear(int fr, int to) {
		int firstInd = 3*fr;
		int lastInd = 3*to;
		return storeMap.isRangeClear(firstInd, lastInd);
	}
	
	private boolean isExtntSet(int fr, int to) {
		int firstInd = 3*fr; // incl
		int lastInd = 3*to; // excl
		
		if (to != (fr+1)%UNITCAPACITY)

		    return (
				// verify begin and end markers
				storeMap.get(firstInd) == true &&
				storeMap.get(lastInd-1) == true &&
				// verify inner range is all set
				storeMap.isRangeSet(firstInd+2, lastInd-3)
				);
		else
			return storeMap.isRangeSet(firstInd, firstInd+3);
	}
	
	private boolean isExtntSkipped(int fr, int to) {
		int firstInd = 3*fr;
		int lastInd = 3*to;

		if (to != (fr+1)%UNITCAPACITY)

		    return (
				// verify begin and end markers
				storeMap.get(firstInd) == true &&
				storeMap.get(lastInd-1) == true &&
				// verify inner range is all clear 
				storeMap.isRangeClear(firstInd+2, lastInd-2)
				);
		else
			return (
				storeMap.get(firstInd) == true &&
				storeMap.get(firstInd+1) == true &&
				storeMap.get(firstInd) == false);
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
		
		for (int i = 0; i < args.length; ) {
			if (args[i].startsWith("-rammode")) {
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
				throw new Exception("Usage: " + CorfuUnitServer.class.getName() + 
						" [-rammode] -drivename <name> -unit <unitnumber>");
			}
		}
		
		if ((!RAMMODE && DRIVENAME == null) || sid < 0) {
			slog.error("missing arguments!");
			throw new Exception("Usage: " + CorfuUnitServer.class.getName() + 
					" [-rammode] [-drivename <filename>] -unit <unitnumber>");
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
				} catch (TTransportException e) {
					e.printStackTrace();
				}
			}}).run();
	
		}
	
	////////////////////////////////////////////////////////////////////////////////////
	
	public CorfuUnitServerImpl() {
		
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
				public void run() {
					for(;;) {
						try {
							DriveChannel.force(false);
							synchronized(this) { this.notifyAll(); }
							Thread.sleep(1);
						} catch(Exception e) {
							e.printStackTrace();
						}
					}
				}
			}).start();
		}
		else {	
			inmemoryStore = new ByteBuffer[(int) UNITCAPACITY]; 
		}
		
		inmemoryMeta = new HashMap<Long, ExtntInfo>();
		storeMap = new CyclicBitSet(3* (int) UNITCAPACITY); // TODO if UNITCAPACITY is more than MAXINT, 
													// make storeMap a list of bitmaps, each one of MAXINT size
	}
	
	private void writebitmap(int from, int to) throws IOException {
		if (to <= from) {
			DriveChannel.position(UNITCAPACITY*ENTRYSIZE+(from/8));
			DriveChannel.write( getbitrange(3*from, 3*(int)UNITCAPACITY-1) );
	
			DriveChannel.position(UNITCAPACITY*ENTRYSIZE+0);
			DriveChannel.write( getbitrange(0, 3*to-1) );
	
		} else {
			DriveChannel.position(UNITCAPACITY*ENTRYSIZE+(from/8));
			DriveChannel.write( getbitrange(3*from, 3*to-1) );
		}
		
	}
	private void writebufs(int from, int to, List<ByteBuffer> wbufs) throws IOException {
		if (to <= from) {
			DriveChannel.position(from*ENTRYSIZE);
			int i;
			for (i = 0; i < (int)UNITCAPACITY-from; i++)
				DriveChannel.write(wbufs.get(i));
			DriveChannel.position(0);
			for (; i < wbufs.size(); i++)
				DriveChannel.write(wbufs.get(i));
		} else {
			DriveChannel.position(from*ENTRYSIZE);
			log.debug("writing {} bufs", wbufs.size());
			for (int i = 0; i < wbufs.size(); i++)
				DriveChannel.write(wbufs.get(i));
		}
	}
	private ArrayList<ByteBuffer> readbufs(int from, int to, int length) throws IOException {
		ArrayList<ByteBuffer> bb = new ArrayList<ByteBuffer>(length);
		if (to <= from) {
			ByteBuffer buf1 = ByteBuffer.allocate(((int)UNITCAPACITY-from)*ENTRYSIZE),
					buf2 = ByteBuffer.allocate(to*ENTRYSIZE);
			DriveChannel.read(buf1, from*ENTRYSIZE);
			for (int i= 0; i < ((int)UNITCAPACITY-from); i++) 
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
	
	private void addmetainfo(ExtntInfo inf) {
		inmemoryMeta.put(inf.getMetaFirstOff(), inf);
	}
	
	private ExtntInfo getmetainfo(long ind) {
		return inmemoryMeta.get(ind);
	}
	
	private void addrambufs(int from, int to, List<ByteBuffer> wbufs) {
		if (to <= from) {
			for (int i = from; i < UNITCAPACITY; i++)
				inmemoryStore[i] = wbufs.get(i); 
			from = 0;
		}
		for (int i = from; i < to; i++)
			inmemoryStore[i] = wbufs.get(i);
	}
	private ArrayList<ByteBuffer> extractrambufs(int from, int to, int length) {
		ArrayList<ByteBuffer> ret = new ArrayList<ByteBuffer>();

		if (to <= from) {
			for (int i= from; i < (int)UNITCAPACITY; i++) 
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
		if (RAMMODE) {
			markExtntSet(from, to);
			addrambufs(from, to, wbufs);
			addmetainfo(inf);
		} else {
			try {
				writebufs(from, to, wbufs);
				writebitmap(from, to);
				markExtntSet(from, to);
				addmetainfo(inf);
			} catch (IOException e) {
				log.warn("cannot write entry {} to store, IO error; quitting", inf);
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
	
	private void SkipToStore(ExtntInfo inf) {
		int from = (int) (inf.getMetaFirstOff()%UNITCAPACITY);
		int to = (int) ((inf.getMetaFirstOff()+inf.getMetaLength())%UNITCAPACITY);
		if (RAMMODE) {
			markExtntSkipped(from, to);
			addmetainfo(inf);
		} else {
			try {
				writebitmap(from, to);
				markExtntSkipped(from, to);
				addmetainfo(inf);
			} catch (IOException e) {
				log.warn("cannot write entry {} to store, IO error; quitting", inf);
				e.printStackTrace();
				System.exit(1);
			}
		}
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
		
		if (ctnt.size() != inf.getMetaLength()) {
			log.warn("internal problem in write({}) ctnt.size()={}", inf, ctnt.size());
			return CorfuErrorCode.ERR_BADPARAM;
		}
		// TODO check that each buf inside ctnt has size ENTRYSIZE ??
		
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

		long relFromOff = fromOff % UNITCAPACITY, relToOff = toOff % UNITCAPACITY;
		
		if (!isExtntClear((int)relFromOff, (int)relToOff)) {
			log.info("write({}) overwrites data, rejected; trimmark={} ", inf, trimmark);
			return CorfuErrorCode.ERR_OVERWRITE;
		}
		// ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

		RamToStore(ctnt, inf);
		return CorfuErrorCode.OK; 
    }
	
	/**
	 * mark an extent 'skiped'
	 * @param inf the extent 
	 * @return OK if succeeds in marking the extent for 'skip'
	 * 		ERROR_TRIMMED if the extent-range has already been trimmed
	 * 		ERROR_OVERWRITE if the extent is occupied (could be a good thing)
	 * 		ERROR_FULL if the extent spills over the capacity of the log
	 */
	synchronized public CorfuErrorCode fix(long pos, ExtntInfo inf) {
		long fromOff = inf.getMetaFirstOff(), toOff = fromOff + inf.getMetaLength();
		
		log.debug("fix({})", inf);
		
		// ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
		// from here until the next '^^^^..' mark : 
		// code to verify that there is room to write the entire multi-page entry in one shot, and not overwrite any filled pages
		//
		if (toOff - trimmark > UNITCAPACITY) {
			log.warn("unit full ! trimmark= {} fix[{}..{}]", trimmark, fromOff, toOff);
			return CorfuErrorCode.ERR_FULL; 
		}
		
		if (fromOff < trimmark) {
			log.info("attempt to skip trimmed! [{}..{}]", fromOff, toOff);
			return CorfuErrorCode.ERR_OVERWRITE; 
		}

		long relFromOff = fromOff % UNITCAPACITY, relToOff = toOff % UNITCAPACITY;
		
		if (!isExtntClear((int)relFromOff, (int)relToOff)) {
			log.info("fix({}) overwrites data, rejected; trimmark={} ", inf, trimmark);
			return CorfuErrorCode.ERR_OVERWRITE;
		}
		// ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

		SkipToStore(inf);
		return CorfuErrorCode.OK; 
	}

	/**
	 * @author dalia
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
			return new ExtntWrap(er, new ExtntInfo(0, 0, 0), new ArrayList<ByteBuffer>());
		}

		@Override
		public ExtntWrap badmetaHelper(ExtntInfo reqinfo, ExtntInfo ckinf) {
			if (ckinf.getFlag() == commonConstants.SKIPFLAG ) {
				log.debug("read({}): SKIP", ckinf);
				return new ExtntWrap(CorfuErrorCode.OK_SKIP, null, null);
			} else {
				log.info("ExtntInfo mismatch expecting {} received {}", reqinfo, ckinf);
				return new ExtntWrap(CorfuErrorCode.ERR_BADPARAM, new ExtntInfo(0, 0, 0), new ArrayList<ByteBuffer>());
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

		long fromOff = hdr.getExtntInf().getMetaFirstOff(), toOff = fromOff + hdr.getExtntInf().getMetaLength();
		log.debug("read [{}..{}] trim={} CAPACITY={}", fromOff, toOff, trimmark, UNITCAPACITY);
		
		// check that we can satisfy this request in full, up to '^^^^^^^^^^^^^' mark
		//
		
		if ((toOff - trimmark) > UNITCAPACITY) 
			return retval.badoffHelper(CorfuErrorCode.ERR_UNWRITTEN, fromOff, toOff);
		
		if (fromOff < trimmark)
			return retval.badoffHelper(CorfuErrorCode.ERR_TRIMMED, fromOff, toOff, fromOff);
		
		long relFromOff = fromOff % UNITCAPACITY, relToOff = toOff % UNITCAPACITY;
		if (!isExtntSet((int) relFromOff, (int) relToOff)) {
				return retval.badoffHelper(CorfuErrorCode.ERR_UNWRITTEN, fromOff, toOff);
		}
		// ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
		// actual reading starts here (we already checked that entire range is written)
		
		ArrayList<ByteBuffer> wbufs = StoreToRam(hdr.getExtntInf());

		ExtntInfo prefetch;
		if (hdr.isPrefetch() && (prefetch = getmetainfo(hdr.getPrefetchOff() )) != null) {
			return new ExtntWrap(CorfuErrorCode.OK, prefetch, wbufs); 
		} else {
			log.debug("read prefetch {} not available, returning {}", hdr.getPrefetchOff(), hdr.getExtntInf());
			return new ExtntWrap(CorfuErrorCode.OK, hdr.getExtntInf(), wbufs);
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
		
		ExtntInfo m = getmetainfo(off);
		if (m == null ) 
			return retval.badoffHelper(CorfuErrorCode.ERR_UNWRITTEN, off, off, off );

		return new ExtntWrap(CorfuErrorCode.OK, m, new ArrayList<ByteBuffer>());
	}

	/**
	 * wait until any previously written log entries have been forced to persistent store
	 */
    synchronized public void sync() throws org.apache.thrift.TException {
    	try { this.wait(); } catch (Exception e) {
    		log.error("forcing sync to persistent store failed, quitting");
    		System.exit(1);
    	}
    }

	
	synchronized public ExtntWrap dbg(long off) {
		ExtntWrap ret = new ExtntWrap(CorfuErrorCode.ERR_BADPARAM, null, null);
		
		if (off < trimmark) {
			ret.setErr(CorfuErrorCode.ERR_TRIMMED);
			return ret;
		}
		if ((off - trimmark) >= UNITCAPACITY) {
			ret.setErr(CorfuErrorCode.ERR_FULL);
			return ret;
		}
		ExtntInfo inf = getmetainfo(off);
		if (inf == null)
			ret.setInf(new ExtntInfo(-1,  0,  0));
		else
			ret.setInf(inf);
		ret.setCtnt(new ArrayList<ByteBuffer>());
		ret.setErr(CorfuErrorCode.OK);
		
		return ret;
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
   
    	markRangeClear((int) (trimmark % UNITCAPACITY), (int) (mark % UNITCAPACITY) );
    	log.info("log trimmed from {} to {}", trimmark, mark);
    	trimmark = mark;
    	
    	// TODO release all ExtntIngo records of trimmed range
    	return true;
		
	}
	
	@Override
    synchronized public void ckpoint(long off) throws org.apache.thrift.TException {
		log.info("mark latest checkpoint offset={}", off);
		if (off > ckmark) ckmark = off;
	}

}
