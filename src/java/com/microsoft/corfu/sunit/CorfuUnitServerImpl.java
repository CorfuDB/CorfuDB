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
import com.sun.xml.internal.bind.v2.runtime.reflect.ListIterator;

public class CorfuUnitServerImpl implements CorfuUnitServer.Iface {
	private static Logger slog = LoggerFactory.getLogger(CorfuUnitServerImpl.class);
	private Logger log = LoggerFactory.getLogger(CorfuUnitServerImpl.class);

	private static long UNITCAPACITY; // capacity in ENTRYSIZE units, i.e. UNITCAPACITY*ENTRYSIZE bytes
	private static int ENTRYSIZE;
	private static int METASIZE; // size in bytes of ExtntInfo after serialization
	private static int PORT;
	private static String DRIVENAME = null;
	private static boolean RAMMODE = false;
	private static long RAMSIZE = -1; // RAM buffer capacity in ENTRYSIZE units, i.e. RAMSIZE*ENTRYSIZE bytes
	private static final long MAXRAMSIZE = (long) 2 * 1024 * 1024 * 1024; // this size is in bytes

	private ByteBuffer[] inmemoryStore;
		
	private ExtntInfo[] inmemoryMeta; // store for the meta data which would go at the end of each disk-block
	private BitSet storeMap;
	private int contiguoustail = 0;
	private long trimmark = 0; // log has been trimmed up to this position, non-inclusive

	private FileChannel DriveChannel = null;

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		CorfuConfigManager CM = new CorfuConfigManager(new File("./0.aux"));
		int sid = -1;
		ENTRYSIZE = CM.getGrain();
		METASIZE = CorfuUtil.ExtntInfoSSize();
		UNITCAPACITY = CM.getUnitsize(); 
		RAMSIZE = Math.min(UNITCAPACITY, MAXRAMSIZE/(long)ENTRYSIZE);

		for (int i = 0; i < args.length; ) {
			if (args[i].startsWith("-rammode")) {
				RAMMODE = true;
				RAMSIZE = 1; // in RAM, we store one buffer per entry
				slog.info("working in RAM mode");
				i += 1;
			} else if (args[i].startsWith("-ramsize") && i < args.length-1) {
					RAMSIZE = Math.min(Long.valueOf(args[i+1]) * 1024 * 1024 / (long)ENTRYSIZE, 
																	MAXRAMSIZE/ (long)ENTRYSIZE);
					slog.info("ramsize: " + RAMSIZE + " entries");
					i += 2;
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
						" [-rammode] [-mapsize <# MBytes>] -drivename <name> -unit <unitnumber>");
			}
		}
		
		if ((!RAMMODE && DRIVENAME == null) || sid < 0) {
			slog.error("missing arguments!");
			throw new Exception("Usage: " + CorfuUnitServer.class.getName() + 
					" [-rammode] [-mapsize <# MBytes>] [-drivename <filename>] -unit <unitnumber>");
		}
		
		CorfuNode[] cn = CM.getGroupByNumber(0);
		if (cn.length < sid) {
			slog.error("unit id {} exceeds group size {}; quitting", sid, cn.length);
			throw new Exception("bad sunit #"); 
		}		
		PORT = cn[sid].getPort();
		
		slog.info("unit server #{} starting; port={}, entsize={} metasize={} capacity={} ramsize={}",
				sid, PORT, ENTRYSIZE , METASIZE, UNITCAPACITY, RAMSIZE);
		
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
		}
		else {	
			inmemoryStore = new ByteBuffer[(int) UNITCAPACITY]; 
			inmemoryMeta = new ExtntInfo[(int) UNITCAPACITY];
		}
		
		storeMap = new BitSet((int) UNITCAPACITY); // TODO if UNITCAPACITY is more than MAXINT, 
													// make storeMap a list of bitmaps, each one of MAXINT size
	}
	
	class entryplus {
		public entryplus(ByteBuffer buf, ExtntInfo inf) {
			super();
			this.buf = buf;
			this.inf = inf;
		}
		ByteBuffer buf;
		ExtntInfo inf;
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
			inmemoryStore[(int)relOff] = buf;
			inmemoryMeta[(int)relOff] = inf; 
		}
		else {
			try {
				ByteBuffer bb = ByteBuffer.wrap(CorfuUtil.ExtntInfoSerialize(inf));

				DriveChannel.position(relOff*(ENTRYSIZE+METASIZE));
				DriveChannel.write(buf); 
				DriveChannel.write(bb);
			} catch (IOException e) {
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
	private entryplus StoreToRam(long relOff)  {

		if (RAMMODE) {
			if (inmemoryMeta[(int)relOff].equals(null) ||
					inmemoryStore[(int)relOff].equals(null)) {
				log.warn("StoreToRam({}) found null entry, bit={}, trimmark={}",
						relOff, storeMap.get((int)relOff), trimmark);
				System.exit(1);
				
			}
			return new entryplus(inmemoryStore[(int)relOff], inmemoryMeta[(int)relOff]);
		}
		else try {
			DriveChannel.position(relOff*(ENTRYSIZE+METASIZE));
			ByteBuffer bb = ByteBuffer.allocate(ENTRYSIZE);
			DriveChannel.read(bb);
			ByteBuffer ib = ByteBuffer.allocate(METASIZE);
			DriveChannel.read(ib);
			ib.rewind();
			return new entryplus(bb, CorfuUtil.ExtntInfoDeserialize(ib.array()));
		} catch (IOException e) {
			log.warn("cannot retrieve entry {} from store, IO error; quitting", relOff);
			e.printStackTrace();
			System.exit(1);
		}
		return null; // never reached
	}
	
	private ExtntInfo StoreToRamMeta(long relOff) {
		ByteBuffer ib = null;
		if (RAMMODE) {
			if (inmemoryMeta[(int)relOff].equals(null)) {
				log.warn("StoreToRam({}) found null entry, bit={}, trimmark={}",
						relOff, storeMap.get((int)relOff), trimmark);
				System.exit(1);
			}
			return inmemoryMeta[(int)relOff];
		}
		else try {
			DriveChannel.position(relOff*(ENTRYSIZE+METASIZE) + ENTRYSIZE);
			ib = ByteBuffer.allocate(METASIZE);
			DriveChannel.read(ib); ib.rewind();
			return CorfuUtil.ExtntInfoDeserialize(ib.array());
		} catch (IOException e) {
			log.warn("cannot retrieve entry {} from store, IO error; quitting", relOff);
			try {			log.info("channel position {} read buf size {}", DriveChannel.size(), ib.capacity()); } catch (Exception e1) {}
			e.printStackTrace();
			System.exit(1);
		}
		return null; // never reached
		
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
			for (long off = relFromOff; off < UNITCAPACITY; off++) {
				bb = li.next();
				RamToStore(off, bb, inf);
			}
			storeMap.set((int) relFromOff , (int) UNITCAPACITY);
			relFromOff = 0;
		}
		
		for (long off = relFromOff; off < relToOff; off++) {
			bb = li.next();
			RamToStore(off, bb, inf);
		}
		storeMap.set((int) relFromOff , (int) relToOff);
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
		
		log.debug("fix({})", pos);
		
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
			log.debug("error reading offset {} in [{}..{}] errval={}", 
					args[2] /* badOff */, 
					args[0] /* from Off */, 
					args[1] /* to Off */, 
					er);
			return new ExtntWrap(er, new ExtntInfo(), new ArrayList<ByteBuffer>());
		}

		@Override
		public ExtntWrap badmetaHelper(ExtntInfo reqinfo, ExtntInfo ckinf) {
			if (ckinf.getFlag() == commonConstants.SKIPFLAG ) {
				log.debug("read({}): SKIP", ckinf);
				return new ExtntWrap(CorfuErrorCode.OK_SKIP, null, null);
			} else {
				log.info("ExtntInfo mismatch expecting {} received {}", reqinfo, ckinf);
				return new ExtntWrap(CorfuErrorCode.ERR_BADPARAM, new ExtntInfo(), new ArrayList<ByteBuffer>());
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
		entryplus ent;
		
		if (relToOff < relFromOff) {
			for (long off = relFromOff; off < UNITCAPACITY; off++) {
				ent = StoreToRam(off);
				if (!ent.inf.equals(hdr.getExtntInf())) 
					return retval.badmetaHelper(hdr.getExtntInf(), ent.inf);
				wbufs.add(ent.buf);
			}
			relFromOff = 0; // wrap around and "spill" to the for loop below
		}
		
		for (long off = relFromOff; off < relToOff; off++) {
			ent = StoreToRam(off);
			if (!ent.inf.equals(hdr.getExtntInf())) 
				return retval.badmetaHelper(hdr.getExtntInf(), ent.inf);
			wbufs.add(ent.buf);
		}

		int relPrefetch = (int) (hdr.getPrefetchOff() % UNITCAPACITY);
		if (hdr.isPrefetch() && (hdr.getPrefetchOff() - trimmark) < UNITCAPACITY && storeMap.get(relPrefetch)) {
			return new ExtntWrap(CorfuErrorCode.OK, StoreToRamMeta(relPrefetch), wbufs); 
		} else {
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
		if (! storeMap.get((int) (off % UNITCAPACITY)) ) 
			return retval.badoffHelper(CorfuErrorCode.ERR_UNWRITTEN, off, off, off );

		return new ExtntWrap(CorfuErrorCode.OK, StoreToRamMeta(off % UNITCAPACITY), new ArrayList<ByteBuffer>());
	}
	
	synchronized public ExtntWrap dbg(long off) {
		ExtntWrap ret = new ExtntWrap();
		
		if (off < trimmark) {
			ret.setErr(CorfuErrorCode.ERR_TRIMMED);
			return ret;
		}
		if ((off - trimmark) >= UNITCAPACITY) {
			ret.setErr(CorfuErrorCode.ERR_FULL);
			return ret;
		}
		long relOff = off % UNITCAPACITY;
		if (storeMap.get((int)relOff)) {
			ret.addToCtnt(ByteBuffer.allocate(1));
			if (RAMMODE) {
				if (inmemoryMeta[(int)relOff] == null)
					ret.setInf(new ExtntInfo(-1,  0,  0));
				else
					ret.setInf(inmemoryMeta[(int)relOff]);
			}
		} else {
			ret.addToCtnt(ByteBuffer.allocate(0));
		}
		ret.setErr(CorfuErrorCode.OK);
		
		return ret;
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
