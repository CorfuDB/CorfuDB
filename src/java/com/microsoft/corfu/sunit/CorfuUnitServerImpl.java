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
		
	private MetaInfo[] inmemoryMeta; // store for the meta data which would go at the end of each disk-block
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
			inmemoryMeta = new MetaInfo[(int) UNITCAPACITY];
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

	private void RamToStore(long relOff, ByteBuffer buf, MetaInfo inf) {
		
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
			assert (mb.capacity() >= buf.capacity() + 8); // 8 == sizeof long, ugh 
			buf.rewind();
			mb.put(buf.array());
			mb.putLong(inf.getMetaFirstOff());
			mb.putLong(inf.getMetaLastOff());
		}
	}
	
	private ByteBuffer StoreToRam(long relOff, MetaInfo inf)  {

		if (RAMMODE) {
			assert(inmemoryStore.size() > relOff);
			inf.setMetaFirstOff(inmemoryMeta[(int)relOff].getMetaFirstOff());
			inf.setMetaLastOff(inmemoryMeta[(int)relOff].getMetaLastOff());
			return inmemoryStore.get((int)relOff);
		}
		else {
			MappedByteBuffer mb = getMappedBuf(relOff);
			ByteBuffer rb = ByteBuffer.wrap(mb.array(), mb.position(), ENTRYSIZE);
			inf.setMetaFirstOff(mb.getLong());
			inf.setMetaLastOff(mb.getLong());
			return rb;
		}
	}

	
	/* (non-Javadoc)
	 * implements to CorfuUnitServer.Iface write() method.
	 * @see com.microsoft.corfu.sunit.CorfuUnitServer.Iface#write(com.microsoft.corfu.LogEntryWrap)
	 * 
	 * we make great effort for the write to either succeed in full, or not leave any partial garbage behind. 
	 * this means that we first check if all the pages to be written are free, and that the incoming entry contains content for each page.
	 * in the event of some error in the middle, we reset any values we already set.
	 */
	@Override
	synchronized public CorfuErrorCode write(MetaInfo inf, List<ByteBuffer> ctnt) throws org.apache.thrift.TException {
		ByteBuffer bb;
		long fromOff = inf.metaFirstOff, toOff = (fromOff + ctnt.size());
		// System.out.println("write  from,to:" + fromOff + " " + toOff);
		
		// ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
		// from here until the next '^^^^..' mark : 
		// code to verify that there is room to write the entire multi-page entry in one shot, and not overwrite any filled pages
		//
		if (toOff - trimmark >= UNITCAPACITY) {
			log.warn("unit full ! trimmark= {} fill[{}..{}]", trimmark, fromOff, toOff);
			return CorfuErrorCode.ERR_FULL; 
		}
		
		if (fromOff < trimmark) {
			log.info("attempt to overwrite trimmed! [{}..{}]", fromOff, toOff);
			return CorfuErrorCode.ERR_OVERWRITE; 
		}

		long relFromOff = fromOff % UNITCAPACITY, relToOff = toOff % UNITCAPACITY;
		// System.out.println("rel from,to:" + relFromOff + " " + relToOff);

		if (relToOff > relFromOff) {
			int i = storeMap.nextSetBit((int) relFromOff);
			if (i >= 0 && i < relToOff) { // we expect the next set bit to be higher than ToOff, or none at all
				log.info("attempt to overwrite! offset={} fill[{}..{}]", (fromOff+i), fromOff, toOff);
				return CorfuErrorCode.ERR_OVERWRITE; 
			}
		} else {   // range wraps around the array
			int i = storeMap.nextSetBit((int) relFromOff); 
			if (i >= 0) { // we expect no bit higher than FromOff to be set, hence for i to be -1
				log.info("attempt to overwrite! offset={} fill[{}..{}]", (fromOff+i), fromOff, toOff);
				return CorfuErrorCode.ERR_OVERWRITE;
			}
			i = storeMap.nextSetBit(0); 
			if (i >= 0 && i < relToOff) { // we expect the next set bit from wraparound origin (to be higher than ToOff, or none
				log.info("attempt to overwrite! offset={} fill[{}..{}]", (fromOff+i), fromOff, toOff);
				return CorfuErrorCode.ERR_OVERWRITE;
			}
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
			// System.out.println("  writing offset to store: " + off);
			assert li.hasNext();
			bb = li.next();
			assert bb.capacity() == ENTRYSIZE;
			RamToStore(off, bb, inf);
		}
		return CorfuErrorCode.OK; 
    }
	
	synchronized public CorfuErrorCode fill(long pos) {
		if (pos - trimmark >= UNITCAPACITY) {
			log.warn("attempt to fill beyond unit capacity! trimmark= {} fill({})", trimmark, pos);
			return CorfuErrorCode.ERR_FULL; 
		}
		
		if (pos < trimmark) {
			log.info("attempt to fill trimmed offset {}!", pos);
			return CorfuErrorCode.ERR_TRIMMED; 
		}

		int relOff = (int) (pos % UNITCAPACITY);
		if (storeMap.get(relOff)) {
			log.debug("attempt to fill a written entry (good?) {}", pos);
			return CorfuErrorCode.ERR_OVERWRITE;
		}
		
		storeMap.set(relOff);
		RamToStore(relOff, null, new MetaInfo(-pos, -pos));
		return CorfuErrorCode.OK;
	}
	
	/**
	 * @author dalia
	 * this is a util class interface, to save repetition of error-returning code
	 *
	 * @param <T>
	 */
	interface ErrorHelper<T> {
		T rh(CorfuErrorCode er, long... args);
	}
	ErrorHelper<LogEntryWrap> retval = new ErrorHelper<LogEntryWrap>() {
		@Override
		public LogEntryWrap rh(CorfuErrorCode er, long... args) {
			long fromOff = args[0];
			long toOff = args[1]; 
			long badOff = args[2];
			LogEntryWrap r;
			r = new LogEntryWrap(er, null, null);
			log.info("error reading offset {} in [{}..{}] errval={}", badOff, fromOff, toOff, er);
			return r;
		}
	};
	
	/* (non-Javadoc)
	 * @see com.microsoft.corfu.sunit.CorfuUnitServer.Iface#read(com.microsoft.corfu.LogHeader, com.microsoft.corfu.MetaInfo)
	 * 
	 * this method performs actual reading of a range of pages.
	 * it fails if any page within range has not been written.
	 * it returns OK_SKIP if it finds any page within range which has been junk-filled (i.e., the entire range becomes junked).
	 * 
	 * the method also reads-ahead the subsequent meta-info entry if hdr.readnext is set.
	 * if the next meta info record is not available, it returns the current meta-info structure
	 * 
	 *  @param a LogHeader describing the range to read
	 */
	@Override
	synchronized public LogEntryWrap read(LogHeader hdr) throws org.apache.thrift.TException {

		long fromOff = hdr.range.getMetaFirstOff(), toOff = hdr.range.getMetaLastOff();
		log.debug("read [{}..{}] time={} CAPACITY={}", fromOff, toOff, trimmark, UNITCAPACITY);
		
		// check that we can satisfy this request in full, up to '^^^^^^^^^^^^^' mark
		//
		
		if ((toOff - trimmark) >= UNITCAPACITY) 
			return retval.rh(CorfuErrorCode.ERR_UNWRITTEN, fromOff, toOff, toOff);
		
		if (fromOff < trimmark)
			return retval.rh(CorfuErrorCode.ERR_TRIMMED, fromOff, toOff, fromOff);
		
		long relFromOff = fromOff % UNITCAPACITY, relToOff = toOff % UNITCAPACITY;
		long relBaseOff = fromOff - relFromOff;

		if (relToOff > relFromOff) {
			int i = storeMap.nextClearBit((int) relFromOff);
			if (i < relToOff)  // we expect the next clear bit to be higher than ToOff, or none at all
				return retval.rh(CorfuErrorCode.ERR_UNWRITTEN, fromOff, toOff, (relBaseOff+i) );
			
		} else {   // range wraps around the array
			int i = storeMap.nextClearBit((int) relFromOff); 
			if (i < (int) UNITCAPACITY)  // we expect no bit higher than FromOff to be clear, hence for i to be UNITCAPACITY
				return retval.rh(CorfuErrorCode.ERR_UNWRITTEN, fromOff, toOff, (relBaseOff+i) );

			i = storeMap.nextClearBit(0); 
			if (i < relToOff)  // we expect the next clear bit from wraparound origin to be higher than ToOff, or none
				return retval.rh(CorfuErrorCode.ERR_UNWRITTEN, fromOff, toOff, (relBaseOff+UNITCAPACITY+i) );

		}
		// ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
		// actual reading starts here
		
		ArrayList<ByteBuffer> wbufs = new ArrayList<ByteBuffer>();
		MetaInfo ckinf = new MetaInfo();
		
		if (relToOff < relFromOff) {
			for (long off = relFromOff; off < UNITCAPACITY; off++) {
				wbufs.add(StoreToRam(off, ckinf));
				if (!ckinf.equals(hdr.range)) {
					log.debug("metainfo mismatch off={} expecting {} received {}", relBaseOff+off, hdr.range, ckinf);
					return retval.rh(CorfuErrorCode.OK_SKIP, fromOff, toOff, (relBaseOff+off) );
				}
			}
			relFromOff = 0; // wrap around and "spill" to the for loop below
		}
		
		for (long off = relFromOff; off < relToOff; off++) {
			wbufs.add(StoreToRam(off, ckinf));
			if (!ckinf.equals(hdr.range)) {
				log.debug("metainfo mismatch off={} expecting {} received {}", relBaseOff+off, hdr.range, ckinf);
				return retval.rh(CorfuErrorCode.OK_SKIP, fromOff, toOff, (relBaseOff+UNITCAPACITY+off) );
			}
		}

		int relPrefetch = (int) (hdr.prefetchOff % UNITCAPACITY);
		if (hdr.prefetch && (hdr.prefetchOff - trimmark) < UNITCAPACITY && storeMap.get(relPrefetch)) {
			StoreToRam(relPrefetch, ckinf); // TODO read only the meta-info here
			log.debug("metainfo prefetch {} available -- {}", relPrefetch, ckinf);
			return new LogEntryWrap(CorfuErrorCode.OK, ckinf, wbufs); 
		} else {
			log.debug("metainfo prefetch {} unavailable", relPrefetch);
			return new LogEntryWrap(CorfuErrorCode.OK, hdr.range, wbufs);
		}
	}
	
	/* read the meta-info record at specified offset
	 * 
	 * @param off- the offset to read from
	 * @return the meta-info record "wrapped" in LogEntryWrap. 
	 *         The wrapping contains error code: UNWRITTEN if reading beyond the tail of the log
	 * 
	 * (non-Javadoc)
	 * @see com.microsoft.corfu.sunit.CorfuUnitServer.Iface#readmeta(long)
	 */
	@Override
	public LogEntryWrap readmeta(long off) {
		MetaInfo ret = new MetaInfo();
		if ((off - trimmark) < UNITCAPACITY && storeMap.get((int) (off % UNITCAPACITY)) ) {
			StoreToRam(off % UNITCAPACITY, ret); // TODO read only the meta-info here
			log.debug("readmeta off={} retinfo={}" , off, ret);
			return new LogEntryWrap(CorfuErrorCode.OK, ret, null);
	} else 
		return retval.rh(CorfuErrorCode.ERR_UNWRITTEN, off, off, off );
	}

	@Override
	synchronized public long check() throws org.apache.thrift.TException {
		int a = (int) (trimmark % UNITCAPACITY); // a is the relative trim mark

		if (a > 0) {
			int candidateA = storeMap.previousSetBit(a-1);
			if (candidateA >= 0) return trimmark + UNITCAPACITY - (a-1-candidateA);
		}
		
		int candidateB = storeMap.previousSetBit((int)UNITCAPACITY-1);
		if (candidateB >= 0) return trimmark + (candidateB+1-a);
		
		return trimmark;
	}
	
	@Override
	synchronized public long checkcontiguous() throws org.apache.thrift.TException{
		int a = (int) (trimmark % UNITCAPACITY); // a is the relative trim mark

		int candidateB = storeMap.nextClearBit(a);
		if (candidateB < UNITCAPACITY) return trimmark + (candidateB-a);
		// note: nextClearBit does not return -1 if none is found; 
		// the "next" index it finds is the one past the end of the bitMap. Odd, but that's the way it is..

		int candidateA = storeMap.nextClearBit(0);
		if (candidateA < a) return trimmark + UNITCAPACITY - (a - candidateA);
		
		return trimmark+UNITCAPACITY;		
	}
	
	@Override
	synchronized public boolean trim(long mark) throws org.apache.thrift.TException {
		log.debug("CorfuUnitServer trim curTrimMark={} newTrimMark={} contiguousmark={} check()={}", 
				trimmark, mark, checkcontiguous(), check());
		
		if (mark <= trimmark) return true;		
    	if (mark > checkcontiguous()) {
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
