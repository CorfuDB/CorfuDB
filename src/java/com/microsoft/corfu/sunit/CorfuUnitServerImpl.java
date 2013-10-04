package com.microsoft.corfu.sunit;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import com.microsoft.corfu.CorfuConfigManager;
import com.microsoft.corfu.CorfuErrorCode;
import com.microsoft.corfu.CorfuNode;
import com.microsoft.corfu.LogEntryWrap;
import com.microsoft.corfu.LogHeader;
import com.microsoft.corfu.sunit.CorfuUnitServer;
import com.microsoft.corfu.sunit.CorfuUnitServer.Iface;
import com.microsoft.corfu.sunit.CorfuUnitServer.Processor;
import com.sun.xml.internal.bind.v2.runtime.reflect.ListIterator;

public class CorfuUnitServerImpl implements CorfuUnitServer.Iface {

	private static int UNITCAPACITY; 
	private static int ENTRYSIZE;
	private static int PORT;

	private ByteBuffer[] inmemoryStore;
	private BitSet storeMap;
	private int contiguoustail = 0;
	private long trimmark = 0; // log has been trimmed up to this position, non-inclusive
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		CorfuConfigManager CM = new CorfuConfigManager(new File("./0.aux"));
		if (args.length < 1) 
			throw new Exception("Usage: " + CorfuUnitServerImpl.class.getSimpleName() + " serverid");
		int sid = Integer.valueOf(args[0]);
		
		System.out.println("UnitServer #" + sid);
		int sz = CM.getGroupsizeByNumber(0);
		System.out.println("groupsize" + sz);
		if (sz < 1) return;
		
		CorfuNode[] cn = CM.getGroupByNumber(0);
		System.out.println("group array size " + cn.length);
		PORT = cn[sid].getPort();
		ENTRYSIZE = CM.getGrain();
		UNITCAPACITY = CM.getUnitsize();
		
		System.out.println("UnitServer #" + sid + 
				" port=" + PORT + 
				" entrysize=" + ENTRYSIZE + 
				" capacity=" + UNITCAPACITY + "ents");
		
		new Thread(new Runnable() {
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
		inmemoryStore = new ByteBuffer[UNITCAPACITY];
		storeMap = new BitSet(UNITCAPACITY);
	}
	
	/* (non-Javadoc)
	 * implements to CorfuUnitServer.Iface write() method.
	 * @see com.microsoft.corfu.sunit.CorfuUnitServer.Iface#write(com.microsoft.corfu.LogEntryWrap)
	 * 
	 * we make great effort for the write to either succeed in full, or not leave any partial garbage behind. 
	 * this means that we first check if all the pages to be written are free, and that the incoming entry contains content for each page.
	 * in the event of some error in the middle, we reset any values we already set.
	 */
	synchronized public CorfuErrorCode write(LogEntryWrap ent) throws org.apache.thrift.TException {
		ByteBuffer bb;
		long fromOff = ent.hdr.off, toOff = (fromOff + ent.ctnt.size());
		
		java.util.ListIterator<ByteBuffer> li = ent.ctnt.listIterator();

		// ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
		// from here until the next '^^^^..' mark : 
		// code to verify that there is room to write the entire multi-page entry in one shot, and not overwrite any filled pages
		//
		if (toOff - trimmark >= UNITCAPACITY) {
			System.out.println("unit has no room ! trimmark=" + trimmark + " trying to fill [" + fromOff + ".." + toOff + "]");
			return CorfuErrorCode.ERR_FULL; 
		}
		
		if (fromOff < trimmark) {
			System.out.println("attempt to overwrite trimmed! [" + fromOff + ".." + toOff + "]");
			return CorfuErrorCode.ERR_OVERWRITE; 
		}

		int relFromOff = (int)(fromOff % UNITCAPACITY), relToOff = (int)(toOff % UNITCAPACITY);

		if (relToOff > relFromOff) {
			int i = storeMap.nextSetBit(relFromOff);
			if (i >= 0 && i < relToOff) { // we expect the next set bit to be higher than ToOff, or none at all
				System.out.println("attempt to overwrite! offset=" + 
						(fromOff+i) + "with range [" + fromOff + ".." + toOff + "]");
				return CorfuErrorCode.ERR_OVERWRITE; 
			}
		} else {   // range wraps around the array
			int i = storeMap.nextSetBit(relFromOff); 
			if (i >= 0) { // we expect no bit higher than FromOff to be set, hence for i to be -1
				System.out.println("attempt to overwrite! offset=" + 
						(fromOff+i) + "with range [" + fromOff + ".." + toOff + "]");
				return CorfuErrorCode.ERR_OVERWRITE;
			}
			i = storeMap.nextSetBit(0); 
			if (i >= 0 && i < relToOff) { // we expect the next set bit from wraparound origin (to be higher than ToOff, or none
				System.out.println("attempt to overwrite! offset=" + 
						(fromOff+i) + "with range [" + fromOff + ".." + toOff + "]");
				return CorfuErrorCode.ERR_OVERWRITE;
			}
		}
		// ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
		
		if (relToOff < relFromOff) {
			storeMap.set(relFromOff , UNITCAPACITY);
			for (int off = relFromOff; off < UNITCAPACITY; off++) {
				assert li.hasNext();
				bb = li.next();
				assert bb.capacity() == ENTRYSIZE;
				inmemoryStore[off] = bb;
			}
			relFromOff = 0;
		}
		
		storeMap.set(relFromOff , relToOff);
		for (int off = relFromOff; off < relToOff; off++) {
			assert li.hasNext();
			bb = li.next();
			assert bb.capacity() == ENTRYSIZE;
			inmemoryStore[off] = bb;
			
			off++;
		}
		return CorfuErrorCode.OK; 
    }


	synchronized public LogEntryWrap read(LogHeader hdr) throws org.apache.thrift.TException {
		// System.out.println("CorfuUnitServer check invoked");
		if ((hdr.off - trimmark) >= UNITCAPACITY) {
			System.out.println("read past end of storage unit: " + hdr.off);
			return new LogEntryWrap(new LogHeader(0, (short)0, CorfuErrorCode.ERR_UNWRITTEN), null);
		}
		if (hdr.off < trimmark) {
			System.out.println("read below trimmed mark" + hdr.off);
			return new LogEntryWrap(new LogHeader(0, (short)0, CorfuErrorCode.ERR_TRIMMED), null);
		}
		
		int relOffset = (int) (hdr.off % UNITCAPACITY);
		if (!storeMap.get(relOffset)) {
			System.out.println("attempt to read unwritten entry" + hdr.off);
			return new LogEntryWrap(new LogHeader(0, (short)0, CorfuErrorCode.ERR_UNWRITTEN), null);
		}
	
		ArrayList<ByteBuffer> wbufs = new ArrayList<ByteBuffer>();
		wbufs.add(inmemoryStore[relOffset]);

		return new LogEntryWrap(new LogHeader(hdr.off, (short)0, CorfuErrorCode.OK), wbufs);
	}

	synchronized public long check() throws org.apache.thrift.TException {
		int a = (int) (trimmark % UNITCAPACITY); // a is the relative trim mark

		if (a > 0) {
			int candidateA = storeMap.previousSetBit(a-1);
			if (candidateA >= 0) return (long) (trimmark + UNITCAPACITY - (a-1-candidateA));
		}
		
		int candidateB = storeMap.previousSetBit(UNITCAPACITY-1);
		if (candidateB >= 0) return (long) (trimmark + (candidateB+1-a));
		
		return trimmark;
	}
	
	synchronized public long checkcontiguous() throws org.apache.thrift.TException{
		int a = (int) (trimmark % UNITCAPACITY); // a is the relative trim mark

		int candidateB = storeMap.nextClearBit(a);
		if (candidateB < UNITCAPACITY) return (long) (trimmark + (candidateB-a));
		// note: nextClearBit does not return -1 if none is found; 
		// the "next" index it finds is the one past the end of the bitMap. Odd, but that's the way it is..

		int candidateA = storeMap.nextClearBit(0);
		if (candidateA < a) return (long) (trimmark + UNITCAPACITY - (a - candidateA));
		
		return trimmark+UNITCAPACITY;		
	}
	
	synchronized public boolean trim(long mark) throws org.apache.thrift.TException {
		System.out.println("CorfuUnitServer trim curTrimMark=" + trimmark +
				" newTrimMark=" + mark +
				" contiguousmark=" + checkcontiguous() +
				" check=" + check());
		
		if (mark <= trimmark) return true;		
    	if (mark > checkcontiguous()) {
    		System.out.println("attempt to trim past the filled mark of storage unit " + mark);
    		return false;
    	}
   
    	int curRelTrim = (int) (trimmark % UNITCAPACITY);
    	int newRelTrim = (int) (mark % UNITCAPACITY);
    	
    	if (newRelTrim > curRelTrim) {
    		storeMap.clear(curRelTrim, newRelTrim);
    	} else {
    		storeMap.clear(curRelTrim, UNITCAPACITY);
    		storeMap.clear(0, newRelTrim);
    	}
    	trimmark = mark;
    	return true;
		
	}
}
