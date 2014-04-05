package com.microsoft.corfu;

import java.util.HashMap;
import java.util.List;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.microsoft.corfu.sunit.CorfuConfigServer;
import com.microsoft.corfu.sunit.UnitWrap;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.*;

import com.microsoft.corfu.CorfuException;
import com.microsoft.corfu.sequencer.CorfuSequencer;
import com.microsoft.corfu.sunit.CorfuUnitServer;
import com.microsoft.corfu.sunit.CorfuUnitServer.Client;

public class CorfuClientImpl implements com.microsoft.corfu.CorfuAPI, com.microsoft.corfu.CorfuDbgInterface {
	Logger log = LoggerFactory.getLogger(CorfuClientImpl.class);
	
	CorfuConfigManager CM;
	CorfuSequencer.Client sequencer;
	TTransport[] transports;
	
	public CorfuClientImpl() throws CorfuException {
		
		log.warn("CurfuClientImpl logging level = dbg?{} info?{} warn?{} err?{}", 
				log.isDebugEnabled(), log.isInfoEnabled(), log.isWarnEnabled(), log.isErrorEnabled());
		CM = new CorfuConfigManager(new File("./corfu.xml"));
		buildClientConnections();
	}
	
	class clientSunitEndpoint {
		TTransport t = null;
		CorfuUnitServer.Client cl = null;
		TBinaryProtocol protocol = null;
        CorfuConfigServer.Client configcl = null;

        clientSunitEndpoint(CorfuNode cn) throws CorfuException {
            TMultiplexedProtocol mprotocol = null, mprotocol2 = null;

			try {
				t = new TSocket(cn.hostname, cn.port);
                t.open();
				protocol = new TBinaryProtocol(t);

                mprotocol = new TMultiplexedProtocol(protocol, "SUNIT");
				cl = new CorfuUnitServer.Client(mprotocol);
				log.info("client connection open with multiplexed server  {}:{}" , cn.hostname , cn.port);

                mprotocol2 = new TMultiplexedProtocol(protocol, "CONFIG");
                configcl = new CorfuConfigServer.Client(mprotocol2);
                log.info("config  connection open with multiplexed server  {}:{}" , cn.hostname , cn.port);

			} catch (TTransportException e) {
				e.printStackTrace();
				throw new CorfuException("could not set up connection(s)");
			}		
		}
		
	}
	CorfuUnitServer.Client getSUnit(Object ep) { return ((clientSunitEndpoint)ep).cl; }
    CorfuConfigServer.Client getConfigServer(Object ep) { return ((clientSunitEndpoint)ep).configcl; }

	class clientSequencerEndpoint {
		TTransport t = null;
		CorfuSequencer.Client cl = null;
		TBinaryProtocol protocol = null;
		
		clientSequencerEndpoint(CorfuNode cn) throws CorfuException {
			try {
				t = new TSocket(cn.hostname, cn.port);
				protocol = new TBinaryProtocol(t);
				cl = new CorfuSequencer.Client(protocol);
				t.open();
				log.info("client connection open with sequencer {}:{}", cn.hostname, cn.port);
			} catch (TTransportException e) {
				e.printStackTrace();
				throw new CorfuException("could not set up connection(s)");
			}		
		}
	}
	
	void buildClientConnections() throws CorfuException { 
		// invoked at startup and every time configuration changes

		// TODO for now, contains only startup code
		//

		for (int g = 0; g < CM.getNumGroups(); g++) {
			
			int nreplicas = CM.getGroupsizeByNumber(g);
			CorfuNode[] rset = CM.getGroupByNumber(g);
			
			for (int r = 0; r < nreplicas; r++) {
				CorfuNode cn = rset[r];
				Object o = new clientSunitEndpoint(cn);
				cn.setInfo(o);

			}
		}
		
		CorfuNode sn = CM.getSequencer();
		Object o = new clientSequencerEndpoint(sn);
		sn.setInfo(o);
		sequencer = ((clientSequencerEndpoint)o).cl;
	}

	/**
	 * Returns the size of a single Corfu entry
	 *
	 * @return		entry size
	 */
	@Override
	public int grainsize() throws CorfuException{
		return CM.getGrain();
	}

	/**
	 * @return an object describing the configuration, @see CorfuConfigManager
	 */
	public CorfuConfigManager getConfig() { return CM; }

	/**
	 * Breaks the bytebuffer is gets as parameter into grain-size buffers, and invokes appendExtnt(List<ByteBuffer>);
	 * 	see appendExtnt(List<ByteBuffer>, boolean) for more details
	 *
	 * @param	buf	the buffer to append to the log
	 * @param	bufsize	size of buffer to append
	 * @param autoTrim		flag, indicating whether to automatically trim the log to latest checkpoint if full
	 * @return		see appendExtnt(List<ByteBuffer>, boolean) 
	 * @throws 		see appendExtnt(List<ByteBuffer>, boolean)
	 */
	@Override
	public long appendExtnt(byte[] buf, int reqsize, boolean autoTrim) throws CorfuException {
		
		if (reqsize % grainsize() != 0) {
			throw new BadParamCorfuException("appendExtnt must be in multiples of log-entry size (" + grainsize() + ")");
		}

		int numents = reqsize/grainsize();
		ArrayList<ByteBuffer> wbufs = new ArrayList<ByteBuffer>(numents);
		for (int i = 0; i < numents; i++)
			wbufs.add(ByteBuffer.wrap(buf, i*grainsize(), grainsize()));
		return appendExtnt(wbufs, autoTrim);
	}
	@Override
	public long appendExtnt(byte[] buf, int reqsize) throws CorfuException {
		return appendExtnt(buf, reqsize, false);
	}
	@Override
	public long appendExtnt(List<ByteBuffer> ctnt) throws CorfuException {
		return appendExtnt(ctnt, false);
	}
	
	/**
	 * Appends an extent to the log. Extent will be written to consecutive log offsets.
	 * 
	 * if autoTrim is set, and the log is full, this call trims to the latest checkpoint-mark (and possibly fills 
	 * holes to make the log contiguous up to that point). if autoTrim is set, this method will not leave a hole in the log. 
	 * Conversely, if autoTrim is false and appendExtent() fails, any log-offsets assigned by the sequencers will remain holes. 
	 *
	 * @param ctnt          list of ByteBuffers to be written
	 * @param autoTrim		flag, indicating whether to automatically trim the log to latest checkpoint if full
	 * @return              the first log-offset of the written range 
	 * @throws CorfuException
	 * 		OutOfSpaceCorfuException indicates an attempt to append failed because storage unit(s) are full; user may try trim()
	 * 		OverwriteException indicates that an attempt to append failed because of an internal race; user may retry
	 * 		BadParamCorfuException or a general CorfuException indicate an internal problem, such as a server crash. Might not be recoverable
	 */
	@Override
	public long appendExtnt(List<ByteBuffer> ctnt, boolean autoTrim) throws CorfuException {
		long offset = -1;
		ExtntInfo inf;
		
		
		try {
			offset = sequencer.nextpos(1); 
			writeExtnt(offset, ctnt, autoTrim);
		} catch (TException e) {
			e.printStackTrace();
			throw new CorfuException("append() failed");
		}
		return offset;
	}
	
	public void writeExtnt(long offset, List<ByteBuffer> ctnt, boolean autoTrim) throws CorfuException {
		CorfuErrorCode er = null;
		EntryLocation el = CM.getCurrentConfiguration().getLocationForOffset(offset);
		CorfuUnitServer.Client sunit = getSUnit(el.group.replicas[0].getInfo());

		try {
			log.debug("write({} size={} marktype={}", offset, ctnt.size(), ExtntMarkType.EX_FILLED);
			er = sunit.write(offset, ctnt, ExtntMarkType.EX_FILLED);
		} catch (TException e) {
			e.printStackTrace();
			throw new CorfuException("append() failed");
		}

		if (er.equals(CorfuErrorCode.ERR_FULL) && autoTrim) {
			try {
				long trimoff = queryck(); 
                log.info("log full! forceappend trimming to " + trimoff);
                trim(trimoff);
				er = sunit.write(offset, ctnt, ExtntMarkType.EX_FILLED);
			} catch (Exception e) {
				e.printStackTrace();
				throw new CorfuException("forceappend() failed");
			}
		} 
		
		if (er.equals(CorfuErrorCode.ERR_FULL)) {
			throw new OutOfSpaceCorfuException("append() failed: full");
		} else
		if (er.equals(CorfuErrorCode.ERR_OVERWRITE)) {
			throw new OverwriteCorfuException("append() failed: overwritten");
		} else
		if (er.equals(CorfuErrorCode.ERR_BADPARAM)) {
			throw new BadParamCorfuException("append() failed: bad parameter passed");
		} 
	}
	
	long lastReadOffset = -1;
		
	/**
	 * get ExtntInfo for an extent starting at a specified log position.
	 * 
	 * @param pos the starting position of the extent 
	 * @param ref to an ExtntInfo record to fill 
	 * @throws CorfuException if read from server fails
	 */
	private void fetchMetaAt(long pos, ExtntInfo inf) throws CorfuException {
		ExtntWrap ret;
	
		EntryLocation el = CM.getCurrentConfiguration().getLocationForOffset(pos);
		CorfuUnitServer.Client sunit = getSUnit(el.group.replicas[0].getInfo());
		try {
			ret = sunit.readmeta(pos);
		} catch (TException e) {
			e.printStackTrace();
			throw new CorfuException("readmeta() failed");
		}
		
		CorfuErrorCode er = ret.getErr();

		if (er.equals(CorfuErrorCode.OK)) {
			CorfuUtil.ExtntInfoCopy(ret.getInf(), inf);
		} else {
			log.debug("readmeta({}) fails err={}", pos, er);
			if (er.equals(CorfuErrorCode.ERR_UNWRITTEN)) {
				throw new UnwrittenCorfuException("readExtnt fails, not written yet");
			} else 
			if (er.equals(CorfuErrorCode.ERR_TRIMMED)) {
				throw new TrimmedCorfuException("readExtnt fails because log was trimmed");
			} else
			if (er.equals(CorfuErrorCode.ERR_BADPARAM)) {
				throw new BadParamCorfuException("readExtnt fails with bad parameter");
			} 
		}
	}	

	/**
	 * a variant of readExtnt that takes the first log-offset position to read the extent from.
	 * 
	 * @param pos           starting position to read
	 * @return an extent wrapper, containing ExtntInfo and a list of ByteBuffers, one for each individual log-entry page
	 * @throws CorfuException
	 */
	@Override
	public ExtntWrap readExtnt(long offset) throws CorfuException {
		
		ExtntWrap ret;
		EntryLocation el = CM.getCurrentConfiguration().getLocationForOffset(offset);
		CorfuUnitServer.Client cl = ((clientSunitEndpoint) el.group.replicas[0].getInfo()).cl;
		
		try {
			log.debug("read offset {} now", offset);
			ret = cl.read(offset);
		} catch (TException e) {
			e.printStackTrace();
			throw new CorfuException("read() failed");
		}

		if (ret.getErr().equals(CorfuErrorCode.ERR_UNWRITTEN)) {
			log.info("readExtnt({}) fails, unwritten", offset);
			throw new UnwrittenCorfuException("read(" + offset +") failed: unwritten");
		} else 
		if (ret.getErr().equals(CorfuErrorCode.ERR_TRIMMED)) {
			lastReadOffset = offset;
			log.info("readExtnt({}) fails, trimmed", offset);
			throw new TrimmedCorfuException("read(" + offset +") failed: trimmed");
		} else
		if (ret.getErr().equals(CorfuErrorCode.ERR_BADPARAM)) {
			log.info("readExtnt({}) fails, bad param", offset);
			throw new OutOfSpaceCorfuException("read(" + offset +") failed: bad parameter");
		} 

		log.debug("read succeeds {}", offset);
		lastReadOffset = offset;
		return ret;
	}
		
	/**
	 * Reads the next extent; it remembers the last read extent (starting with zero).
	 * 
	 * @return an extent wrapper, containing ExtntInfo and a list of ByteBuffers, one for each individual log-entry page
	 * @throws CorfuException
	 */
	@Override
	public ExtntWrap readExtnt() throws CorfuException {
		
		ExtntWrap r;
		do {
			r = readExtnt(lastReadOffset+1);
		} while (r.getErr().equals(CorfuErrorCode.OK_SKIP));
		return r;
	}

	
	public void repairPos(long pos) {
		
	}
	
	@Override
	public long repairNext() throws CorfuException {
		long head, tail;
		long pos;
		CorfuErrorCode readErr = CorfuErrorCode.OK;
		boolean skip = false;
		
		// check current log bounds
		head = queryhead(); 
		tail = querytail(); 
		
		pos = lastReadOffset+1;
		
		if (pos < head) {
			log.info("repairNext repositioning to head={}", head);
			pos = head;
		}
		if (pos >= tail) {
			log.info("repairNext reached log tail, finishing");
			return pos; // TODO do something??
		}
				
		// next, try to read 'pos' to see what is the error value
		EntryLocation el = CM.getCurrentConfiguration().getLocationForOffset(pos);
		CorfuUnitServer.Client sunit = getSUnit(el.group.replicas[0].getInfo());
		try {
			log.debug("repairNext read({})", pos);
			ExtntWrap ret = sunit.read(pos);
			readErr = ret.getErr();
			if (!readErr.equals(CorfuErrorCode.OK)) skip = true;
		} catch (TException e) {
			e.printStackTrace();
			throw new CorfuException("repairNext read() failed, communication problem; quitting");
		}
		
		// finally, if either the meta-info was broken, or the content was broken, skip will be ==true.
		// in that case, we mark this entry for skipping, and we also try to mark it for skip on the storage units
		
		if (!skip) {
			lastReadOffset = pos;
			return pos;
		}
		
		lastReadOffset = pos;
			
		// now we try to fix 'inf'
		CorfuErrorCode er;
		log.debug("repairNext fix {}", pos);
		try {
			er = sunit.fix(pos);
		} catch (TException e1) {
			e1.printStackTrace();
			throw new CorfuException("repairNext() fix failed, communication problem; quitting");
		}
		if (er.equals(CorfuErrorCode.ERR_FULL)) {
			// TODO should we try to trim the log to the latest checkpoint and/or check if the extent range exceeds the log capacity??
			throw new OutOfSpaceCorfuException("repairNext failed, log full");
		}
		return pos;
	}

    /**
     * recover the token server by moving it to a known lower-bound on filled position
     * should only be used by administrative utilities
     *
     * @param lowbound
     */
    @Override
    public void tokenserverrecover(long lowbound) throws CorfuException {
        try {
            sequencer.recover(lowbound);
        } catch (TException e) {
            e.printStackTrace();
            throw new CorfuException("tokenserver recovery failed");
        }
    }

    /**
	 * force a delay until we are notified that previously invoked writes to the log have been safely forced to persistent store.
	 * 
	 * @throws CorfuException if the call to storage-units failed; in this case, there is no gaurantee regarding data persistence.
	 */
	@Override
	public void sync() throws CorfuException {
		int ngroups = CM.getNumGroups(); int nreplicas = CM.getGroupsizeByNumber(0);
		
		for (int j = 0; j < ngroups; j++) {
			for (int k = 0; k < nreplicas; k++) {
				CorfuUnitServer.Client sunit = getSUnit(CM.getGroupByNumber(j)[k].getInfo());					
				try { sunit.sync(); } catch (TException e) {
					throw new InternalCorfuException("sync() failed ");
				}
			}
		}
	}
	
	/**
	 * trim a prefix of log up to the specified position
	 * 
	 * @param offset the position to trim to (excl)
	 * 
	 * @throws CorfuException
	 */
	public void trim(long offset) throws CorfuException {
		int ngroups = CM.getNumGroups(); int nreplicas = CM.getGroupsizeByNumber(0);
		
		for (int j = 0; j < ngroups; j++) {
			for (int k = 0; k < nreplicas; k++) {
				CorfuUnitServer.Client sunit = getSUnit(CM.getGroupByNumber(j)[k].getInfo());					
				try { sunit.trim(offset); } catch (TException e) {
					throw new InternalCorfuException("sync() failed ");
				}
			}
		}		
	}

	
	
	/**
	 * Query the log head. 
	 *  
	 * @return the current head's index 
	 * @throws CorfuException if the check() call fails or returns illegal (negative) value 
	 */
	@Override
	public long queryhead() throws CorfuException {
		CorfuUnitServer.Client sunit = getSUnit(CM.getGroupByNumber(0)[0].getInfo());					
		long r;
		try {
			r = sunit.querytrim();
		} catch (TException t) {
			throw new InternalCorfuException("queryhead() failed ");
		}
		if (r < 0) throw new InternalCorfuException("queryhead() call returned negative value, shouldn't happen");
		return r;
	}
	
	/**
	 * Query the log tail. 
	 *  
	 * @return the current tail's index 
	 * @throws CorfuException if the check() call fails or returns illegal (negative) value 
	 */
	@Override
	public long querytail() throws CorfuException {
		long r;
		try {
			r = sequencer.nextpos(0);
		} catch (TException t) {
			throw new InternalCorfuException("querytail() failed ");
		}
		if (r < 0) throw new InternalCorfuException("querytail() call returned negative value, shouldn't happen");
		return r;
	}

	/**
	 * Query the last known checkpoint position. 
	 *  
	 * @return the last known checkpoint position.
	 * @throws CorfuException if the call fails or returns illegal (negative) value 
	 */
	@Override
	public long queryck() throws CorfuException {
		throw new BadParamCorfuException("queryck() not implemented yet");
	}
	
	/**
	 * inform about a new checkpoint mark. 
	 *  
	 * @param off the offset of the new checkpoint
	 * @throws CorfuException if the call fails 
	 */
	@Override
	public void ckpoint(long off) throws CorfuException {
		throw new BadParamCorfuException("ckpoint() not implemented yet");
	}
	
	/**
	 * set the read mark to the requested position. 
	 * after this, invoking readExtnt will perform at the specified position.
	 * 
	 * @param pos move the read mark to this log position
	 */
	@Override
	public void setMark(long pos) {
		lastReadOffset = pos-1;
	}
		
	// from here down, implement CorfuDbgInterface for debugging:
	// ==========================================================
	
	/**
	 * return the meta-info record associated with the specified offset. used for debugging.
	 * 
	 * @param offset the inquired position 
	 * @return an ExtntInfo object
	 * @throws CorfuException
	 *     TrimmedCorfuException, BadParam, Unwritten, with the obvious meanings
	 */
	@Override
	public ExtntWrap dbg(long pos) throws CorfuException {
		EntryLocation el = CM.getCurrentConfiguration().getLocationForOffset(pos);
		CorfuUnitServer.Client sunit = getSUnit(el.group.replicas[0].getInfo());
		try {
			return sunit.readmeta(pos);
		} catch (TException t) {
			throw new InternalCorfuException("dbg() failed ");
		}
	}
	
	/**
	 * utility function to grab tcnt tokens from the sequencer. used for debugging.
	 * 
	 * @param tcnt the number of tokens to grab from sequencer
	 * @throws CorfuException is thrown in case of unexpected communication problem with the sequencer
	 */
	@Override
	public void grabtokens(int tcnt) throws CorfuException {
		try {
			sequencer.nextpos(tcnt);
		} catch (TException t) {
			throw new InternalCorfuException("grabtoken failed");
		}
	}

	/* (non-Javadoc)
	 * @see com.microsoft.corfu.CorfuDbgInterface#write(long, byte[])
	 */
	public void write(long offset, byte[] buf) throws CorfuException {
		if (buf.length % grainsize() != 0) {
			throw new BadParamCorfuException("appendExtnt must be in multiples of log-entry size (" + grainsize() + ")");
		}

		int numents = buf.length/grainsize();
		ArrayList<ByteBuffer> wbufs = new ArrayList<ByteBuffer>(numents);
		for (int i = 0; i < numents; i++)
			wbufs.add(ByteBuffer.wrap(buf, i*grainsize(), grainsize()));
		writeExtnt(offset, wbufs, false);
	}

    public UnitWrap rebuild(long offset) throws CorfuException {
        EntryLocation el = CM.getCurrentConfiguration().getLocationForOffset(offset);
        CorfuConfigServer.Client cnfg = getConfigServer(el.group.replicas[0].getInfo());
        UnitWrap ret = null;
        try {
            ret = cnfg.rebuild();
        } catch (TException t) {
            throw new InternalCorfuException("rebuild failed");
        }
        return ret;
    }
}
