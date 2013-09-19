package com.microsoft.corfu.sunit;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import com.microsoft.corfu.CorfuErrorCode;
import com.microsoft.corfu.CorfuPayloadWrap;
import com.microsoft.corfu.sunit.CorfuUnitServer;
import com.microsoft.corfu.sunit.CorfuUnitServer.Iface;
import com.microsoft.corfu.sunit.CorfuUnitServer.Processor;

public class CorfuUnitServerImpl implements CorfuUnitServer.Iface {

	private static final int UNITCAPACITY = 100000; // time 4KB == 400MBytes, in memory

	private int ENTRYSIZE = 4096;	// this can be supplied as a command-line parameter; 
									// here we set a default page-size..
	private ByteBuffer[] inmemoryStore = new ByteBuffer[UNITCAPACITY];
	private BitSet storeMap = new BitSet(UNITCAPACITY);
	private int contiguoustail = 0;
	private long trimmark = 0; // log has been trimmed up to this position, non-inclusive
	
	static class CorfuUnitServerWrapper implements Runnable {
	
		int port;
		int entrysize;
	
		public CorfuUnitServerWrapper(int port, int esize) {
			this.port = port;
			this.entrysize = esize;
		}
	
		@Override
		public void run() {
			
			TServer server;
			TServerSocket serverTransport;
			CorfuUnitServer.Processor<CorfuUnitServerImpl> processor; 
			System.out.println("run..");
	
			try {
				serverTransport = new TServerSocket(port);
				processor = 
						new CorfuUnitServer.Processor<CorfuUnitServerImpl>(new CorfuUnitServerImpl(entrysize));
				server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
				System.out.println("Starting Corfu storage unit server on port " + port);
				
				server.serve();
			} catch (TTransportException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 1) 
			throw new Exception("Usage: " + CorfuUnitServerImpl.class.getSimpleName() + " serverport [pagesize]");
		int port = Integer.valueOf(args[0]);
		int entrysize = 0;
		if (args.length > 1) entrysize = Integer.valueOf(args[1]); 
		new Thread(new CorfuUnitServerWrapper(port, entrysize)).run();
	}
	
	////////////////////////////////////////////////////////////////////////////////////
	
	public CorfuUnitServerImpl(int esize) {
		if (esize > 0) ENTRYSIZE = esize;
	}
	
	synchronized public CorfuErrorCode write(long absOffset, ByteBuffer ctnt) throws org.apache.thrift.TException {
		
		// System.out.println("CorfuUnitServer write invoked");
		// if (ctnt.hasArray() && ctnt.array().length != ENTRYSIZE) {
		if (ctnt.capacity() != ENTRYSIZE) {
			System.out.println("write: bad param size " + ctnt.capacity());
			return CorfuErrorCode.ERR_BADPARAM; 
		}
		
		if (absOffset - trimmark >= UNITCAPACITY) {
			System.out.println("unit is full! trimmark=" + trimmark + " absOffset=" + absOffset);
			return CorfuErrorCode.ERR_FULL; 
		}
		// check for various overwrite conditions
		// TODO // TODO return meaningful error value!!
		if (absOffset < trimmark) {
			System.out.println("attempt to overwrite a trimmed entry!");
			return CorfuErrorCode.ERR_OVERWRITE; 
		}
		
		int relOffset = (int)(absOffset % UNITCAPACITY);
		
		if (storeMap.get(relOffset)) {
			System.out.println("attempt to overwrite!");
			return CorfuErrorCode.ERR_OVERWRITE; 
		}
		
		storeMap.set(relOffset);
		inmemoryStore[relOffset] = ctnt;

		return CorfuErrorCode.OK; 
    }


	synchronized public CorfuPayloadWrap read(long absOffset) throws org.apache.thrift.TException {
		// System.out.println("CorfuUnitServer check invoked");
		if ((absOffset - trimmark) >= UNITCAPACITY) {
			System.out.println("read past end of storage unit: " + absOffset);
			return new CorfuPayloadWrap(CorfuErrorCode.ERR_UNWRITTEN, null);
		}
		if (absOffset < trimmark) {
			System.out.println("read below trimmed mark" + absOffset);
			return new CorfuPayloadWrap(CorfuErrorCode.ERR_TRIMMED, null);
		}
		
		int relOffset = (int) (absOffset % UNITCAPACITY);
		if (!storeMap.get(relOffset)) {
			System.out.println("attempt to read unwritten entry" + absOffset);
			return new CorfuPayloadWrap(CorfuErrorCode.ERR_UNWRITTEN, null);
		}
	
		return new CorfuPayloadWrap(CorfuErrorCode.OK, inmemoryStore[relOffset]);
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
		
		if (mark == trimmark) return true;		
    	if (mark < trimmark) return false;
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
