// @author Dahlia Malkhi
//
// implement a cyclic log store: logically infinite log sequence mapped onto a UNICAPACITY array of fixed-entrys	
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
import java.util.StringTokenizer;
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

	private long trimmark = 0; // log has been trimmed up to this position (excl)
	private int ckmark = 0; // start offset of latest checkpoint. TODO: persist!!

	private FileChannel DriveChannel = null;
	private Object DriveLck = new Object();
	
	private int lowwater = 0, highwater = 0, freewater = -1;
	private ByteBuffer[] inmemoryStore; // use in rammode
	private byte map[] = null;
	private ByteBuffer mapb = null;
	// private static final int longsz = Long.SIZE / Byte.SIZE;
	private static final int intsz = Integer.SIZE / Byte.SIZE;
	private static final int entsz = 2*intsz; 

	public void initLogStore(int sz) {
		inmemoryStore = new ByteBuffer[sz];
		UNITCAPACITY = freewater = sz;
		map = new byte[sz * (entsz)];
		mapb = ByteBuffer.wrap(map);
	}
	
	public void initLogStore(byte[] initmap, int sz) throws Exception { 
		inmemoryStore = new ByteBuffer[sz];
		UNITCAPACITY = freewater = sz;
		map = initmap;
		mapb = ByteBuffer.wrap(map);
	}

	class mapInfo {
		
		int physOffset;
		int length;
		ExtntMarkType et;

		public mapInfo(long logOffset) {
			
			int mi = mapind(logOffset);
			mapb.position(mi);
			physOffset = mapb.getInt();
			
			length = mapb.getInt();
			int etbit = length & 0x1; length >>= 1;
			if (length > 0) et = ExtntMarkType.EX_FILLED;
			else if (etbit != 0) et = ExtntMarkType.EX_SKIP;
			else et = ExtntMarkType.EX_EMPTY;
		}		
	}
	
	public int getMetaSZ() { return entsz; }
	
	public byte[] toArray() { return map; }
	public ByteBuffer toArray(int fr, int to) { 
		return ByteBuffer.wrap(map, fr*entsz, to*entsz); 
	}
	
	private int mapind(long logOffset) { // the cyclic-index converter

		int cind = (int)((logOffset+UNITCAPACITY) % UNITCAPACITY); 
		return cind * 2 * intsz;
	} 		
	
	private void put(int ind, ByteBuffer buf) {
		inmemoryStore[ind] = buf;
	}
	
	private boolean put(List<ByteBuffer> wbufs) {
		
		if (wbufs.size() > freewater) {
			return false;
		}
		
		for (int j = 0; j < wbufs.size(); j++) {
			put(highwater++, wbufs.get(j));
			if (highwater >= UNITCAPACITY) highwater = 0;
		}
		freewater -= wbufs.size();
		return true;
	}
	
	private ArrayList<ByteBuffer> get(int pos, int sz) {
		ArrayList<ByteBuffer> wbufs = new ArrayList<ByteBuffer>();
		
		for (int j = 0; j < sz; j++) {
			wbufs.add(inmemoryStore[pos++]);
			if (pos >= UNITCAPACITY) pos = 0;
		}
		
		return wbufs;	
	}
	
	public int getPhysOffset(long logOffset) { 
		int mi = mapind(logOffset);
		return mapb.getInt(mi);
	}
	
	public int getLength(long logOffset) {
		int mi = mapind(logOffset) + intsz;
		return mapb.getInt(mi);
	}
	
	public ExtntMarkType getET(long logOffset) {
		int mi = mapind(logOffset) + intsz;
		mapb.position(mi);
		int length = mapb.getInt();
		int et = length & 0x1; length >>= 1;
		if (length > 0) return ExtntMarkType.EX_FILLED;
		if (et != 0) return ExtntMarkType.EX_SKIP;
		return ExtntMarkType.EX_EMPTY;
	}
	
	public void setExtntInfo(long logOffset, int physOffset, int length, ExtntMarkType et) { 
		int mi = mapind(logOffset);
		mapb.position(mi);
		mapb.putInt(physOffset);
		length <<= 1; 
		if (et != ExtntMarkType.EX_EMPTY) length |= 1;
		mapb.putInt(length);
	}
	
	public void trimLogStore(long toOffset) {
		long lastskip = -1;
		long lasttrim = trimmark;
		
		log.info("=== trim({}) trimmark={} lastskip={} freewater={} lowwater/highwater={}/{} ===", 
				toOffset, trimmark, lastskip, freewater, lowwater, highwater);

		
		while (lasttrim < toOffset) {
			mapInfo minf = new mapInfo(lasttrim);
			log.debug("trim {}", minf);
			if (minf.et != ExtntMarkType.EX_FILLED) {
				// nothing to do, skip this non-existing extent
				lasttrim++;
			} else if (minf.physOffset == lowwater) {
				// free this extent's blocks, and move the lowwater mark past it
				
				log.info("free {} sz={}", lasttrim, minf.length);

				setExtntInfo(lasttrim, 0, 0, ExtntMarkType.EX_TRIMMED);
				// TODO free up blocks
				lowwater += minf.length; freewater += minf.length;
				if (lowwater >= UNITCAPACITY) lowwater %= UNITCAPACITY;
				if (lastskip >= 0) { //go back to last skip point
					log.info("go back to {}", lastskip);
					lasttrim = lastskip;
					lastskip = -1; 
				} else {
					lasttrim++;
				}
			} else {
				log.info("can't free {} yet", lasttrim);
				if (lastskip < 0) lastskip = lasttrim;
				lasttrim++;	
			}
		} // end while
		
		if (lastskip >= 0) 
			trimmark = lastskip;
		else
			trimmark = lasttrim;

		log.info("=== done trim({}) trimmark={} lastskip={} freewater={} lowwater/highwater={}/{} ===", 
				toOffset, trimmark, lastskip, freewater, lowwater, highwater);

	}

	public boolean appendExtntLogStore(long logOffset, List<ByteBuffer> wbufs, ExtntMarkType et) {
		if (getET(logOffset) != ExtntMarkType.EX_EMPTY) {
			log.info("append would overwrite {}", logOffset);
			return false;
		}
		if (logOffset < trimmark || (logOffset-trimmark) >= UNITCAPACITY) return false;
		int physOffset = highwater;
		if (!put(wbufs)) {
			log.info("no free space for append({})", logOffset);
			return false;
		}
		setExtntInfo(logOffset, physOffset, wbufs.size(), et);
		return true;
	}
	
	public ExtntWrap getExtntLogStore(long logOffset) {
		ExtntWrap wr = new ExtntWrap();

		if (logOffset < trimmark) {
			wr.setErr(CorfuErrorCode.ERR_TRIMMED);
			wr.setCtnt(new ArrayList<ByteBuffer>());
		} else if ((logOffset-trimmark) >= UNITCAPACITY) {
			wr.setErr(CorfuErrorCode.ERR_UNWRITTEN);
			wr.setCtnt(new ArrayList<ByteBuffer>());
		} else {
			mapInfo minf = new mapInfo(logOffset);
			wr.setInf(new ExtntInfo(logOffset, minf.length, minf.et));
			log.debug("read phys {}->{}, {}", minf.physOffset, minf.length, minf.et);
			if (minf.et == ExtntMarkType.EX_FILLED) {
				wr.setErr(CorfuErrorCode.OK);
				wr.setCtnt(get(minf.physOffset, minf.length));
				log.debug("ctnt {}", wr.getCtnt());
			} else if (minf.et == ExtntMarkType.EX_SKIP) {
				wr.setErr(CorfuErrorCode.OK_SKIP);
			} else if (minf.et == ExtntMarkType.EX_EMPTY) {
				wr.setErr(CorfuErrorCode.ERR_UNWRITTEN);
			} else if (minf.et == ExtntMarkType.EX_TRIMMED) {
				wr.setErr(CorfuErrorCode.ERR_TRIMMED);
			}
		}	
		return wr;
	}

	public ExtntInfo getExtntInfoLogStore(long logOffset) {	
		if (logOffset < trimmark) {
			return new ExtntInfo(0,  0,  ExtntMarkType.EX_TRIMMED);
		} else if ((logOffset-trimmark) >= UNITCAPACITY) {
			return new ExtntInfo(0,  0,  ExtntMarkType.EX_EMPTY);
		} else {
			mapInfo minf = new mapInfo(logOffset);
			return new ExtntInfo(logOffset, minf.length, minf.et);
		}
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		CorfuConfigManager CM = new CorfuConfigManager(new File("./corfu.xml"));
		int sid = -1, rid = -1;
		ENTRYSIZE = CM.getGrain();
		UNITCAPACITY = CM.getUnitsize(); 
		String Usage = "Usage: " + CorfuUnitServer.class.getName() + "-unit <stripe:replica> " +
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
				sid = Integer.parseInt(args[i+1].substring(0, args[i+1].indexOf(":")));
				rid = Integer.parseInt(args[i+1].substring(args[i+1].indexOf(":")+1));
				slog.info("stripe number: {} replica num: {}", sid, rid);
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
		
		if ((!RAMMODE && DRIVENAME == null) || sid < 0 || rid < 0) {
			slog.error("missing drive name or unit number!");
			throw new Exception(Usage);
		}
		if (RAMMODE && RECOVERY) {
			slog.error("cannot do recovery in rammode!");
			throw new Exception(Usage);
		}
		
		if (CM.getNumGroups() <= sid) {
			slog.error("stripe # {} exceeds num of stripes in configuration {}; quitting", sid, CM.getNumGroups());
			throw new Exception("bad sunit #"); 
		}		
		
		CorfuNode[] cn = CM.getGroupByNumber(sid);
		if (cn.length < rid) {
			slog.error("replica # {} exceeds replica group size {}; quitting", rid, cn.length);
			throw new Exception("bad sunit #"); 
		}		
		PORT = cn[rid].getPort();
		
		slog.info("unit server {}:{} starting; port={}, entsize={} capacity={}",
				sid, rid, PORT, ENTRYSIZE, UNITCAPACITY);
		
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
			initLogStore(UNITCAPACITY);
		}
		// make storeMap a list of bitmaps, each one of MAXINT size

	}
	
	private void writeExtntMap(int from, int to) throws IOException {
		if (to <= from) {
			DriveChannel.position(UNITCAPACITY*ENTRYSIZE+from*getMetaSZ());
			DriveChannel.write( toArray(from, UNITCAPACITY) );
	
			DriveChannel.position(UNITCAPACITY*ENTRYSIZE+0);
			DriveChannel.write( toArray(0, to) );
	
		} else {
			DriveChannel.position(UNITCAPACITY*ENTRYSIZE+from*getMetaSZ());
			DriveChannel.write( toArray(from, to) );
		}
	}
	
	private void writetrimmark() throws IOException {
		DriveChannel.position(UNITCAPACITY*ENTRYSIZE+ UNITCAPACITY*getMetaSZ());
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
		ByteBuffer bb = ByteBuffer.allocate(UNITCAPACITY*getMetaSZ());
		DriveChannel.read(bb);
/*		log.debug("recovery bitmap: {}", bb);
		for (int k = 0; k < bb.capacity(); k++) log.debug("bb[{}] {}", k, bb.get(k));
*/		
		ByteBuffer tb = ByteBuffer.allocate(sz);
		DriveChannel.position(UNITCAPACITY*ENTRYSIZE+ UNITCAPACITY*getMetaSZ());
		if (DriveChannel.read(tb) == sz) {
			log.debug("trimmark deseralize read {} bytes: {}", sz, tb.array());
			trimmark = ((Long)CorfuUtil.ObjectDeserialize(tb.array())).longValue();
		} else {
			log.info("no trimmark saved, setting initial trim=0");
			trimmark=0;
		}
		initLogStore(bb.array(), UNITCAPACITY);
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
		
	/* (non-Javadoc)
	 * implements to CorfuUnitServer.Iface write() method.
	 * @see com.microsoft.corfu.sunit.CorfuUnitServer.Iface#write(com.microsoft.corfu.ExtntWrap)
	 * 
	 * we make great effort for the write to either succeed in full, or not leave any partial garbage behind. 
	 * this means that we first check if all the pages to be written are free, and that the incoming entry contains content for each page.
	 * in the event of some error in the middle, we reset any values we already set.
	 */
	@Override
	synchronized public CorfuErrorCode write(long logOff, List<ByteBuffer> ctnt, ExtntMarkType et) throws org.apache.thrift.TException {
		
		log.debug("write({} size={} marktype={})", logOff, ctnt.size(), et);
		boolean success = appendExtntLogStore(logOff, ctnt, et);
		if (success) return CorfuErrorCode.OK; 
		else return CorfuErrorCode.ERR_OVERWRITE; // TODO differentiate write errors
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
	synchronized public CorfuErrorCode fix(long offset) throws TException {
		return write(offset, new ArrayList<ByteBuffer>(), ExtntMarkType.EX_SKIP);
	}
		
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
	synchronized public ExtntWrap read(long offset) throws org.apache.thrift.TException {
		log.debug("read({}) trim={} CAPACITY={}", offset, trimmark, UNITCAPACITY);
		return getExtntLogStore(offset);
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
		ExtntInfo inf = getExtntInfoLogStore(off);
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
		
		trimLogStore(mark);
    	if (!RAMMODE) {
	    	try {
	    	   	// TODO writebitmap(relTrimmark, relMark);
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
