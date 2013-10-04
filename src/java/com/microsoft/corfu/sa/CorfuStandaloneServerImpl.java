package com.microsoft.corfu.sa;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.thrift.TException;

import com.microsoft.corfu.CorfuException;
import com.microsoft.corfu.CorfuStandaloneClientImpl;
import com.microsoft.corfu.CorfuErrorCode;
import com.microsoft.corfu.LogEntryWrap;
import com.microsoft.corfu.LogHeader;

public class CorfuStandaloneServerImpl implements CorfuStandaloneServer.Iface {

	private final static Logger LOGGER = LoggerFactory.getLogger(CorfuStandaloneServerImpl.class);
	private static final int ENTRYSIZE = 4096; // should be in a shared place somewhere ..
	private static final int LOGSIZE = 100000; // times 4KB == 400MBytes, in memory
	private ArrayList<ByteBuffer> inmemorylog = new ArrayList<ByteBuffer>(LOGSIZE);
	private int logtail = 0;
	private long trimmark = 0; // log has been trimmed up to this position, non-inclusive

	/* (non-Javadoc)
	 * @see com.microsoft.corfu.sa.CorfuStandaloneServer.Iface#append(java.nio.ByteBuffer)
	 */
	@Override
	synchronized public LogHeader append(LogEntryWrap ent) throws TException {
		LOGGER.trace("append invoked; logtail at " + logtail);
		if (ent == null || ent.ctnt == null) {
			LOGGER.warn("append invoked with null buffer");
			return new LogHeader(0, (short)0, CorfuErrorCode.ERR_BADPARAM);
		}
		if (ent.ctnt.hasArray() && ent.ctnt.array().length != ENTRYSIZE) { // do something, like throw exception
			LOGGER.warn("append invoked with buffer of size " + ent.ctnt.array().length + " : expected " + ENTRYSIZE);
			return new LogHeader(0, (short)0, CorfuErrorCode.ERR_BADPARAM);
		}
		
		if (inmemorylog.size() >= LOGSIZE) {
			LOGGER.warn("append attempt when log is full");
			return new LogHeader(0, (short)0, CorfuErrorCode.ERR_FULL);
		}
		
		inmemorylog.add(ent.ctnt);
		LOGGER.trace("append completed; log has " + inmemorylog.size() + "entries; logtail=" + logtail);
		return new LogHeader(logtail++, (short)1, CorfuErrorCode.OK);
	}
	
	synchronized public LogEntryWrap read(LogHeader hdr) {
		// read beyond the log tail
		LOGGER.trace("read invoked with offset " + hdr.off);
		if (hdr.off >= logtail) {
			LOGGER.warn("read attempt at offset " + hdr.off + " is past the log tail " + logtail);
			return new LogEntryWrap(new LogHeader(0, (short)0, CorfuErrorCode.ERR_UNWRITTEN), null);
		}
		
		// read below the trim mark
		if (hdr.off < trimmark) {
			System.out.println("read below trimmed mark" + hdr.off);
			LOGGER.warn("read attempt at offset " + hdr.off + " is below the trimmed mark " + trimmark);
			return new LogEntryWrap(new LogHeader(0, (short)0, CorfuErrorCode.ERR_TRIMMED), null);
		}
		
		// read successful
		return new LogEntryWrap(new LogHeader(0, (short)0, CorfuErrorCode.OK), inmemorylog.get((int)(hdr.off-trimmark)));
	}
	
	synchronized public long check() {
		LOGGER.trace("check invoked; logtail is " + logtail);
    	return logtail;
    }
    
	synchronized public boolean trim(long mark) {
		LOGGER.info("trim invoked with trim-mark " + mark);
		
		// trimming already trimmed area
    	if (mark <= trimmark) {
    		LOGGER.trace("trim at " + mark + " is below the trim-mark " + trimmark);
    		return false;
    	}
    	
    	// trimming past the tail of the log
    	if (mark > logtail) {
    		LOGGER.trace("trim at " + mark + " is beyond the log tail " + logtail);
    		return false;
    	}

    	// trim up to mark
    	inmemorylog.removeAll(inmemorylog.subList(0,  (int)(mark-trimmark)));
    	trimmark = mark;
		LOGGER.trace("trimed completed up to  " + mark);
    	return true;
    }

}
