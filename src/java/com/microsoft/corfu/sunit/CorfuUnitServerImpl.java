package com.microsoft.corfu.sunit;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.slf4j.*;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import com.microsoft.corfu.*;
import com.microsoft.corfu.sunit.CorfuUnitServer;
import com.microsoft.corfu.sunit.CorfuUnitServer.Iface;
import com.microsoft.corfu.sunit.CorfuUnitServer.Processor;
import com.microsoft.corfu.unittests.CorfuClientTester;
import com.sun.xml.internal.bind.v2.runtime.reflect.ListIterator;

public class CorfuUnitServerImpl implements CorfuUnitServer.Iface {
	private Logger log = LoggerFactory.getLogger(CorfuUnitServerImpl.class);

	private static long UNITCAPACITY; // capacity in ENTRYSIZE units, i.e. UNITCAPACITY*ENTRYSIZE bytes
	private static int ENTRYSIZE;
	private static int PORT;
	private static String DRIVENAME = null;
	private static boolean RAMMODE = false;
	private static int RAMSIZE = -1; // RAM buffer capacity in ENTRYSIZE units, i.e. RAMSIZE*ENTRYSIZE bytes
	private static final int MAXRAMSIZE = 2 * 1024 * 1024 * 1024; // this size is in bytes

	private ArrayList<ByteBuffer> inmemoryStore;
		
	private ExtntInfo[] inmemoryMeta; // store for the meta data which would go at the end of each disk-block
	private BitSet storeMap;
	private int contiguoustail = 0;
	private long trimmark = 0; // log has been trimmed up to this position, non-inclusive

	private FileChannel DriveChannel = null;
	private ArrayList<MappedByteBuffer> DriveMap;

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		CorfuConfigManager CM = new CorfuConfigManager(new File("./0.aux"));
		int sid = -1;
		ENTRYSIZE = CM.getGrain();
		UNITCAPACITY = CM.getUnitsize(); 
		RAMSIZE = (int) Math.min(UNITCAPACITY, MAXRAMSIZE/ENTRYSIZE);

		for (int i = 0; i < args.length; ) {
			if (args[i].startsWith("-rammode")) {
				RAMMODE = true;
				RAMSIZE = 1; // in RAM, we store one buffer per entry
				System.out.println("working in RAM mode");
				i += 1;
			} else if (args[i].startsWith("-ramsize") && i < args.length-1) {
					RAMSIZE = Math.min(Integer.valueOf(args[i+1]) * 1024 * 1024 / ENTRYSIZE, 
																	MAXRAMSIZE/ENTRYSIZE);
					System.out.println("ramsize: " + RAMSIZE + " entries");
					i += 2;
			} else if (args[i].startsWith("-unit") && i < args.length-1) {
				sid = Integer.valueOf(args[i+1]);
				System.out.println("unit number: " + sid);
				i += 2;
			} else if (args[i].startsWith("-drivename") && i < args.length-1) {
				DRIVENAME = args[i+1];
				System.out.println("drivename: " + DRIVENAME);
				i += 2;
			} else {
				System.out.println("unknown param: " + args[i]);
				throw new Exception("Usage: " + CorfuClientTester.class.getName() + 
						" [-rammode] [-mapsize <# MBytes>] -drivename <name> -unit <unitnumber>");
			}
		}
		
		if ((!RAMMODE && DRIVENAME == null) || sid < 0) {
			System.out.println("missing arguments!");
			throw new Exception("Usage: " + CorfuClientTester.class.getName() + 
					" [-rammode] [-mapsize <# MBytes>] [-drivename <filename>] -unit <unitnumber>");
		}
		
		CorfuNode[] cn = CM.getGroupByNumber(0);
		System.out.println("group array size " + cn.length);
		if (cn.length < sid) {
			System.out.println("unit id exceeds group size, " + sid + " " + cn.length);
		}		
		PORT = cn[sid].getPort();
		
		System.out.println("UnitServer #" + sid + 
				" port=" + PORT + 
				" entrysize=" + ENTRYSIZE + 
				" capacity=" + UNITCAPACITY + "ents");
		
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
				RandomAccessFile f = new RandomAccessFile(DRIVENAME, "rw");
				f.setLength(UNITCAPACITY * ENTRYSIZE);
				DriveChannel = f.getChannel();
				DriveMap = new ArrayList<MappedByteBuffer>((int) (UNITCAPACITY*ENTRYSIZE/RAMSIZE)); 
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else {	
			inmemoryStore = new ArrayList<ByteBuffer>((int) UNITCAPACITY); 
			inmemoryMeta = new ExtntInfo[(int) UNITCAPACITY];
		}
		
		storeMap = new BitSet((int) UNITCAPACITY); // TODO if UNITCAPACITY is more than MAXINT, 
													// make storeMap a list of bitmaps, each one of MAXINT size
	}
	

	private MappedByteBuffer getMappedBuf(long relOff) {
		// System.out.println("getMappedBuf(" + relOff + ")");
		
		int mapind = (int) (relOff/RAMSIZE);
		MappedByteBuffer mb  = null;
		
		// System.out.println("  mapind=" + mapind);
		
		if (DriveMap.size() <= mapind) {
			// System.out.println("  allocate new memory mapped buffer");
			try {
				mb = DriveChannel.map(MapMode.READ_WRITE, relOff*ENTRYSIZE, RAMSIZE*ENTRYSIZE);
				DriveMap.add(mapind, mb);
				mb.load();
				mb.rewind(); 
	
			} catch (IOException e) {
				log.error("failure to sync drive to memory");
				e.printStackTrace();
				System.exit(-1);
			}
		}
		else
			mb = DriveMap.get(mapind);

		mb.position(((int) (relOff % RAMSIZE)) * ENTRYSIZE);

		// System.out.println("getMappedBuf relOff=" + relOff);
		// System.out.println("  mapind=" + mapind);
		// System.out.println("inmemStore.size()=" + inmemoryStore.size());
		// System.out.println("mb.position()=" + mb.position());
	
		return mb;
	}

	private void ExtntInfoCopy(ExtntInfo from, ExtntInfo to) {
		to.setFlag(from.getFlag());
		to.setMetaFirstOff(from.getMetaFirstOff());
		to.setMetaLength(from.getMetaLength());
	}
	/**
	 * utility function to handle incoming log-entry. depending on mode, if RAMMODE, it holds a pointer to the entry buffer in memory, 
	 * otherwise, it copies it into store.
	 * 
	 * @param relOff the physical offset to write to
	 * @param buf the buffer to store
	 * @param inf meta information on the extent that this entry belongs to
	 */
	private void RamToStore(long relOff, ByteBuffer buf, ExtntInfo inf) {
		
		if (RAMMODE) {
			if (inmemoryStore.size() > relOff) {
				// System.out.println("  replace existing Buff at pos");
				inmemoryStore.set((int) relOff, buf);
			}
			else {
				// System.out.println("  add new Buff at pos");
				// System.out.println("  buf.capacity " + buf.capacity());
				inmemoryStore.add((int) relOff, buf);
			}
			inmemoryMeta[(int)relOff] = inf; 
		}
		else {
			MappedByteBuffer mb = getMappedBuf(relOff);
			if (buf != null) {
				assert (mb.capacity() >= buf.capacity()); 
				buf.rewind();
				mb.put(buf.array());
			}
			assert(mb.capacity() >= 16); // TODO this is the size to write ExtntInfo's fields?? ugh!!!
			mb.putLong(inf.getMetaFirstOff());
			mb.putInt(inf.getMetaLength());
			mb.putInt(inf.getFlag());
		}
	}
	
	/**
	 * utility function to bring into memory a log entry. depending on mode, if RAMMODE, simply return a point to the in-memory buffer, 
	 * otherwise, it read the buffer from store.
	 * we also fill-up 'inf' here with the meta-info of the extent that this entry belongs to.
	 * 
	 * @param relOff the log-offset we would like to obtain
	 * @param inf reference to meta-info record to fill with the extent's meta-information
	 * @return a pointer to a ByteBuffer containing the content
	 */
	private ByteBuffer StoreToRam(long relOff, ExtntInfo inf)  {

		if (RAMMODE) {
			assert(inmemoryStore.size() > relOff);
			ExtntInfoCopy(inmemoryMeta[(int)relOff], inf);
			return inmemoryStore.get((int)relOff);
		}
		else {
			MappedByteBuffer mb = getMappedBuf(relOff);
			ByteBuffer rb = ByteBuffer.wrap(mb.array(), mb.position(), ENTRYSIZE);
			inf.setMetaFirstOff(mb.getLong());
			inf.setMetaLength(mb.getInt());
			return rb;
		}
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
		ByteBuffer bb;
		long fromOff = inf.getMetaFirstOff(), toOff = fromOff + inf.getMetaLength();
		
		log.debug("write({}) starting", inf);
		
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

		int i = storeMap.nextSetBit((int) relFromOff);
		if (relToOff <= relFromOff) { // first, range wraps around the array
			if (i >= 0) { // we expect no bit higher than FromOff to be set, hence for i to be -1
				log.info("attempt to overwrite! offset={} fill[{}..{}] set-bit before end={}", (fromOff+i), fromOff, toOff, i);
				return CorfuErrorCode.ERR_OVERWRITE;
			}
			i = storeMap.nextSetBit(0); 
		}
		if (i >= 0 && i < relToOff) { // we expect the next set bit to be higher than ToOff, or none at all
				log.info("attempt to overwrite! offset={} fill[{}..{}] set-bit after 0={}", (fromOff+i), fromOff, toOff, i);
				return CorfuErrorCode.ERR_OVERWRITE; 
			}

		// ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
		
		java.util.ListIterator<ByteBuffer> li = ctnt.listIterator();

		if (relToOff < relFromOff) {
			storeMap.set((int) relFromOff , (int) UNITCAPACITY);
			for (long off = relFromOff; off < UNITCAPACITY; off++) {
				assert li.hasNext();
				bb = li.next();
				assert bb.capacity() == ENTRYSIZE;
				RamToStore(off, bb, inf);
			}
			relFromOff = 0;
		}
		
		storeMap.set((int) relFromOff , (int) relToOff);
		for (long off = relFromOff; off < relToOff; off++) {
			log.debug("write({}) to store", off);
			assert li.hasNext();
			bb = li.next();
			assert bb.capacity() == ENTRYSIZE;
			RamToStore(off, bb, inf);
		}
		return CorfuErrorCode.OK; 
    }
	
	/**
	 * fix an individual offset belonging to the specified extent
	 * @param pos is the offset to fix
	 * @param inf the extent whose range this offset belongs to
	 * @return OK if succeeds in marking the extent's page for 'skip'
	 * 		ERROR_TRIMMED if the offset has already been trimmed
	 * 		ERROR_OVERWRITE if the offset is occupied (could be a good thing)
	 * 		ERROR_FULL if the offset spills over the capacity of the log
	 */
	synchronized public CorfuErrorCode fix(long pos, ExtntInfo inf) {
		CorfuErrorCode er; 
		
		if (pos - trimmark > UNITCAPACITY) {
			log.warn("fix({}): unit full! trimmark={} ", pos, trimmark);
			return CorfuErrorCode.ERR_FULL; 
		}
		
		if (pos < trimmark) {
			log.warn("fix({}): trimmed already! trimmark={} ", pos, trimmark);
			return CorfuErrorCode.ERR_TRIMMED; 
		}

		int relOff = (int) (pos % UNITCAPACITY);
		inf.setFlag(commonConstants.SKIPFLAG);
		
		if (storeMap.get(relOff)) {
			// offset already written
			log.debug("fix({}): written already! ", pos);
			return CorfuErrorCode.ERR_OVERWRITE; 
		}

		storeMap.set(relOff);
		RamToStore(relOff, ByteBuffer.allocate(0), inf);
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
			log.info("error reading offset {} in [{}..{}] errval={}", 
					args[2] /* badOff */, 
					args[0] /* from Off */, 
					args[1] /* to Off */, 
					er);
			return new ExtntWrap(er, null, null);
		}

		@Override
		public ExtntWrap badmetaHelper(ExtntInfo reqinfo, ExtntInfo ckinf) {
			if (ckinf.getFlag() == commonConstants.SKIPFLAG ) {
				log.info("extent {} filled; returning SKIP code ", ckinf);
				return new ExtntWrap(CorfuErrorCode.OK_SKIP, null, null);
			} else {
				log.info("ExtntInfo mismatch expecting {} received {}", reqinfo, ckinf);
				return new ExtntWrap(CorfuErrorCode.ERR_BADPARAM, null, null);
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
			return retval.badoffHelper(CorfuErrorCode.ERR_UNWRITTEN, fromOff, toOff, toOff);
		
		if (fromOff < trimmark)
			return retval.badoffHelper(CorfuErrorCode.ERR_TRIMMED, fromOff, toOff, fromOff);
		
		long relFromOff = fromOff % UNITCAPACITY, relToOff = toOff % UNITCAPACITY;
		long relBaseOff = fromOff - relFromOff;
		if (relToOff > relFromOff) {
			int i = storeMap.nextClearBit((int) relFromOff);
			if (i < relToOff)  // we expect the next clear bit to be higher than ToOff, or none at all
				return retval.badoffHelper(CorfuErrorCode.ERR_UNWRITTEN, fromOff, toOff, (relBaseOff+i) );
			
		} else {   // range wraps around the array
			int i = storeMap.nextClearBit((int) relFromOff); 
			if (i < (int) UNITCAPACITY)  // we expect no bit higher than FromOff to be clear, hence for i to be UNITCAPACITY
				return retval.badoffHelper(CorfuErrorCode.ERR_UNWRITTEN, fromOff, toOff, (relBaseOff+i) );

			i = storeMap.nextClearBit(0); 
			if (i < relToOff)  // we expect the next clear bit from wraparound origin to be higher than ToOff, or none
				return retval.badoffHelper(CorfuErrorCode.ERR_UNWRITTEN, fromOff, toOff, (relBaseOff+UNITCAPACITY+i) );

		}
		// ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
		// actual reading starts here (we already checked that entire range is written)
		
		ArrayList<ByteBuffer> wbufs = new ArrayList<ByteBuffer>();
		ExtntInfo ckinf = new ExtntInfo();
		
		if (relToOff < relFromOff) {
			for (long off = relFromOff; off < UNITCAPACITY; off++) {
				wbufs.add(StoreToRam(off, ckinf));
				if (!ckinf.equals(hdr.getExtntInf())) 
					return retval.badmetaHelper(hdr.getExtntInf(), ckinf);
			}
			relFromOff = 0; // wrap around and "spill" to the for loop below
		}
		
		for (long off = relFromOff; off < relToOff; off++) {
			wbufs.add(StoreToRam(off, ckinf));
			if (!ckinf.equals(hdr.getExtntInf())) 
				return retval.badmetaHelper(hdr.getExtntInf(), ckinf);
		}

		int relPrefetch = (int) (hdr.getPrefetchOff() % UNITCAPACITY);
		if (hdr.isPrefetch() && (hdr.getPrefetchOff() - trimmark) < UNITCAPACITY && storeMap.get(relPrefetch)) {
			StoreToRam(relPrefetch, ckinf); // TODO read only the meta-info here
			log.debug("ExtntInfo prefetch {} available -- {}", relPrefetch, ckinf);
			return new ExtntWrap(CorfuErrorCode.OK, ckinf, wbufs); 
		} else {
			log.debug("ExtntInfo prefetch {} unavailable isprefetch={} prefetchOff={} trimmark={} mapisset={}", 
					relPrefetch, hdr.isPrefetch(), hdr.getPrefetchOff(), trimmark, storeMap.get(relPrefetch));
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
	public ExtntWrap readmeta(long off) {
		log.debug("readmeta {}", off);
		if (off < trimmark)
			return retval.badoffHelper(CorfuErrorCode.ERR_TRIMMED, off, off, off);
		if ((off - trimmark) >= UNITCAPACITY )
			return retval.badoffHelper(CorfuErrorCode.ERR_UNWRITTEN, off, off, off );
		if (! storeMap.get((int) (off % UNITCAPACITY)) ) 
			return retval.badoffHelper(CorfuErrorCode.ERR_UNWRITTEN, off, off, off );

		ExtntInfo ret = new ExtntInfo();
		StoreToRam(off % UNITCAPACITY, ret); // TODO read only the meta-info here
		log.debug("readmeta off={} retinfo={}" , off, ret);
		return new ExtntWrap(CorfuErrorCode.OK, ret, null);
	}

	@Override
	synchronized public long check(CorfuLogMark typ) {
		
		int a = (int) (trimmark % UNITCAPACITY); // a is the relative trim mark
		if (typ.equals(CorfuLogMark.HEAD)) {
			return trimmark;
		} 
		
		else if (typ.equals(CorfuLogMark.TAIL)) {
	
			if (a > 0) {
				int candidateA = storeMap.previousSetBit(a-1);
				if (candidateA >= 0) return trimmark + UNITCAPACITY - (a-1-candidateA);
			}
			
			int candidateB = storeMap.previousSetBit((int)UNITCAPACITY-1);
			if (candidateB >= 0) return trimmark + (candidateB+1-a);
			
			return trimmark;
		}
		
		else if (typ.equals(CorfuLogMark.CONTIG)) {
			int candidateB = storeMap.nextClearBit(a);
			if (candidateB < UNITCAPACITY) return trimmark + (candidateB-a);
			// note: nextClearBit does not return -1 if none is found; 
			// the "next" index it finds is the one past the end of the bitMap. Odd, but that's the way it is..
	
			int candidateA = storeMap.nextClearBit(0);
			if (candidateA < a) return trimmark + UNITCAPACITY - (a - candidateA);
			
			return trimmark+UNITCAPACITY;		
		}
		
		log.warn("check({}) shouldn't reach here", typ);
		return -1;
	}
	
	@Override
	synchronized public boolean trim(long mark) throws org.apache.thrift.TException {
		log.debug("CorfuUnitServer trim curTrimMark={} newTrimMark={} contiguousmark={} check(CorfuLogMark.TAIL)={}", 
				trimmark, mark, check(CorfuLogMark.CONTIG), check(CorfuLogMark.TAIL));
		
		if (mark <= trimmark) return true;		
    	if (mark > check(CorfuLogMark.CONTIG)) {
    		log.warn("attempt to trim past the filled mark of storage unit {}", mark);
    		return false;
    	}
   
    	int curRelTrim = (int) (trimmark % UNITCAPACITY);
    	int newRelTrim = (int) (mark % UNITCAPACITY);
    	
    	if (newRelTrim > curRelTrim) {
    		storeMap.clear(curRelTrim, newRelTrim);
    	} else {
    		storeMap.clear(curRelTrim, (int)UNITCAPACITY);
    		storeMap.clear(0, newRelTrim);
    	}
    	trimmark = mark;
    	return true;
		
	}
}
