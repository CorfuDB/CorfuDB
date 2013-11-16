package com.microsoft.corfu;

import java.util.List;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.thrift.TException;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.*;

import com.microsoft.corfu.CorfuException;
import com.microsoft.corfu.sequencer.CorfuSequencer;
import com.microsoft.corfu.sunit.CorfuUnitServer;

public class CorfuClientImpl implements com.microsoft.corfu.CorfuExtendedInterface {
	Logger log = LoggerFactory.getLogger(CorfuClientImpl.class);
	
	CorfuConfigManager CM;
	CorfuUnitServer.Client[] sunits;
	CorfuSequencer.Client sequencer;
	TTransport[] transports;
	
	public CorfuClientImpl(CorfuConfigManager CM) throws CorfuException {
		
		log.warn("CurfuClientImpl logging level = dbg?{} info?{} warn?{} err?{}", 
				log.isDebugEnabled(), log.isInfoEnabled(), log.isWarnEnabled(), log.isErrorEnabled());
		this.CM = CM;
		buildClientConnections();
	}
	
	class clientSunitEndpoint {
		TTransport t = null;
		CorfuUnitServer.Client cl = null;
		TBinaryProtocol protocol = null;
		
		clientSunitEndpoint(CorfuNode cn) throws CorfuException {
			try {
				t = new TSocket(cn.hostname, cn.port);
				protocol = new TBinaryProtocol(t);
				cl = new CorfuUnitServer.Client(protocol);
				t.open();
				log.info("client connection open with server  {}:{}" , cn.hostname , cn.port);
		} catch (TTransportException e) {
				e.printStackTrace();
				throw new CorfuException("could not set up connection(s)");
			}		
		}
	}

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

		// TODO for now, code below here handles only a single sunit
		sunits = new CorfuUnitServer.Client[1];

		for (int g = 0; g < CM.getNumGroups(); g++) {
			
			int nreplicas = CM.getGroupsizeByNumber(0);
			CorfuNode[] rset = CM.getGroupByNumber(0);
			
			for (int r = 0; r < nreplicas; r++) {
				CorfuNode cn = rset[r];
				Object o = new clientSunitEndpoint(cn);
				cn.setInfo(o);
				sunits[g*nreplicas+r] = ((clientSunitEndpoint)o).cl;

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
	 * see appendExtnt(List<ByteBuffer>): 
	 *   Breaks the bytebuffer is gets as parameter into grain-size buffers, and invokes appendExtnt(List<ByteBuffer>);
	 *
	 * @param	buf	the buffer to append to the log
	 * @param	bufsize	size of buffer to append
	 * @param autoTrim		flag, indicating whether to automatically trim the log to latest checkpoint if full
	 * @return		the first log-offset of the written range 
	 * @throws CorfuException
	 */
	public long appendExtnt(byte[] buf, int reqsize, boolean autoTrim) throws CorfuException {

		int numents = (int)(reqsize/grainsize());
		ArrayList<ByteBuffer> wbufs = new ArrayList<ByteBuffer>(numents);
		for (int i = 0; i < numents; i++)
			wbufs.add(ByteBuffer.wrap(buf, i*grainsize(), grainsize()));
		return appendExtnt(wbufs, autoTrim);
	}
	public long appendExtnt(byte[] buf, int reqsize) throws CorfuException {
		return appendExtnt(buf, reqsize, false);
	}
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
	 */
	public long appendExtnt(List<ByteBuffer> ctnt, boolean autoTrim) throws CorfuException {
		long offset = -1;
		CorfuErrorCode er = null;
		ExtntInfo inf;
		
		try {
			offset = sequencer.nextpos(ctnt.size()); 
			inf  = new ExtntInfo(offset, offset+ctnt.size(), 0);
			er = sunits[0].write(inf, ctnt);
		} catch (TException e) {
			e.printStackTrace();
			throw new CorfuException("append() failed");
		}
		
		if (er.equals(CorfuErrorCode.ERR_FULL) && autoTrim) {
			try {
				long ckoff = checkpointLoc();
				long contigoff = check(true, true);
				if (ckoff > contigoff) repairLog(true, ckoff);
				
                log.info("log full! forceappend trimming to " + ckoff);
				trim(ckoff);
				er = sunits[0].write(inf, ctnt);
			} catch (Exception e) {
				e.printStackTrace();
				throw new CorfuException("forceappend() failed");
			}
		} 
		
		if (er.equals(CorfuErrorCode.ERR_FULL)) {
			throw new OverwriteCorfuException("append() failed: full");
		} else
		if (er.equals(CorfuErrorCode.ERR_OVERWRITE)) {
			throw new OverwriteCorfuException("append() failed: overwritten");
		} else
		if (er.equals(CorfuErrorCode.ERR_BADPARAM)) {
			throw new BadParamCorfuException("append() failed: bad parameter passed");
		} 
		
		return offset;
	}
	
	ExtntInfo curExtntInfo = new ExtntInfo(-1, -1, 0); // meta-info of last successfully read extent  
	ExtntInfo nexExtntInfo = new ExtntInfo(-1, -1, 0); // pre-fetched meta-info of next extent to read
	
	private void ExtntInfoCopy(ExtntInfo from, ExtntInfo to) {
		to.setFlag(from.getFlag());
		to.setMetaFirstOff(from.getMetaFirstOff());
		to.setMetaLastOff(from.getMetaLastOff());
	}
	
	/**
	 * obtain the ExtntInfo meta-info for the next extent to read.
	 * 
	 * this method makes use of the state we store in curExtntInfo and nextExtntInfo, which we update by pre-fetching on readExtnt() calls.
	 * 
	 * @param nextinf an ExtntInfo object to fill with the next log extent meta-info.
	 * @return OK if succeeds; 
	 * 		ERR_UNWRITTEN if the next meta-record hasn't been written yet;
	 * 		ERR_TRIMMED if the next position following the last extent has been meanwhile trimmed
	 * @throws CorfuException if read from server fails 
	 */
	private CorfuErrorCode getNextMeta(ExtntInfo inf) throws CorfuException {
		CorfuErrorCode er;
		
		synchronized(curExtntInfo) {
			for (;;) {
				if (curExtntInfo.equals(nexExtntInfo)) {
						// this means nexExtntInfo wasn't available for prefetching last time
					log.info("getNextMeta for cur={}", curExtntInfo);
					er = fetchMetaAt(curExtntInfo.getMetaLastOff()+1, nexExtntInfo);
					if (er != CorfuErrorCode.OK && er != CorfuErrorCode.OK_SKIP) return er;
					continue;
				}
				else if ((nexExtntInfo.getFlag() & commonConstants.SKIPFLAG)!=0 ) {
						// in this case, the nexExtntInfo we fetched previously must be skipped 
						// we try to progress both curExtntInfo and nexExtntInfo to the subsequent extent
					log.info("getNextMeta skip cur={} proceed to={}", curExtntInfo, nexExtntInfo);
					ExtntInfoCopy(nexExtntInfo, curExtntInfo);
					er = fetchMetaAt(nexExtntInfo.getMetaLastOff()+1, nexExtntInfo);
					if (er != CorfuErrorCode.OK && er != CorfuErrorCode.OK_SKIP) return er;
					continue;
				}
				
				ExtntInfoCopy(nexExtntInfo, inf);
				return CorfuErrorCode.OK;

			}
		}
	}
	
	/**
	 * get ExtntInfo for an extent starting at a specified log position.
	 * 
	 * @param pos the starting position of the extent 
	 * @return a ExtntInfo record of the extent starting at specified position.
	 *         throws a CorfuException if read from server fails; throws a TrimmedCorfuException if the requested position has been trimmed.
	 *         returns null if the next meta-record hasn't been written yet.
	 *         
	 */
	private CorfuErrorCode fetchMetaAt(long pos, ExtntInfo inf) throws CorfuException {
		synchronized(curExtntInfo) {
			ExtntWrap ret;
		
			try {
				ret = sunits[0].readmeta(pos);
			} catch (TException e) {
				e.printStackTrace();
				throw new CorfuException("readmeta() failed");
			}

			if (ret.getErr().equals(CorfuErrorCode.OK) || ret.getErr().equals(CorfuErrorCode.OK_SKIP)) {
				ExtntInfoCopy(ret.getPrefetchInf(), inf);
			}
					
			return ret.getErr();
		}
	}	

	/**
	 * Update the metadata of last-read extent and the prefetched meta of next extent
	 * @param cur the meta-info of the extent we just read
	 * @param prefetch the pre-fetched meta-info of the next extent
	 */
	private synchronized void updateMeta(ExtntInfo cur, ExtntInfo prefetch) {
		ExtntInfoCopy(cur, curExtntInfo);
		ExtntInfoCopy(prefetch, nexExtntInfo);
	}
	
	/**
	 * Reads a range of log-pages belonging to one entry.
	 *   This method is intended to be used with a ExtntWrap.nextinf returned from a previous call or by getNextMeta().
	 *   
	 *   Otherwise, it should only be invoked if you actually know an entry's boundaries 
	 *   and also sure you are correctly constructing a ExtntInfo record. 
	 *    
	 * @param inf           range to read
	 * @return a log-entry wrapper, containing ExtntInfo and a list of ByteBuffers, one for each individual log-entry page
	 * @throws CorfuException
	 */
	private ExtntWrap readExtnt(ExtntInfo inf) throws CorfuException {
		
		ExtntWrap ret;
		
		try {
			ret = sunits[0].read(new CorfuHeader(inf, true, inf.getMetaLastOff()+1, CorfuErrorCode.OK));
		} catch (TException e) {
			e.printStackTrace();
			throw new CorfuException("read() failed");
		}

		if (ret.getErr().equals(CorfuErrorCode.ERR_UNWRITTEN)) {
			throw new UnwrittenCorfuException("read(" + inf +") failed: unwritten");
		} else 
		if (ret.getErr().equals(CorfuErrorCode.ERR_TRIMMED)) {
			updateMeta(inf, inf);
			throw new TrimmedCorfuException("read(" + inf +") failed: trimmed");
		} else
		if (ret.getErr().equals(CorfuErrorCode.ERR_BADPARAM)) {
			throw new OutOfSpaceCorfuException("read(" + inf +") failed: bad parameter");
		} 

		updateMeta(inf, ret.getPrefetchInf());
		ret.prefetchInf = inf;
		return ret;
	}
		
	/**
	 * Reads the next extent; it remembers the last read extent (starting with zero).
	 * 
	 * @return an extent wrapper, containing ExtntInfo and a list of ByteBuffers, one for each individual log-entry page
	 * @throws CorfuException
	 */
	public ExtntWrap readExtnt() throws CorfuException {
		
		ExtntWrap r;
		ExtntInfo nextinf = new ExtntInfo();
		CorfuErrorCode er;
		do {
			er = getNextMeta(nextinf);
			
			if (er.equals(CorfuErrorCode.ERR_UNWRITTEN)) {
				throw new UnwrittenCorfuException("readExtnt fails, not written yet");
			} else 
			if (er.equals(CorfuErrorCode.ERR_TRIMMED)) {
				throw new TrimmedCorfuException("readExtnt fails because log was trimmed");
			} else
			if (er.equals(CorfuErrorCode.ERR_BADPARAM)) {
				throw new BadParamCorfuException("readExtnt fails with bad parameter");
			} 

			r = readExtnt(nextinf);
		} while (r.getErr().equals(CorfuErrorCode.OK_SKIP));
		return r;
	}

	/**
	 * a variant of readExtnt that takes the first log-offset position to read the extent from.
	 * 
	 * @param pos           starting position to read
	 * @return an extent wrapper, containing ExtntInfo and a list of ByteBuffers, one for each individual log-entry page
	 * @throws CorfuException
	 */
	public ExtntWrap readExtnt(long pos) throws CorfuException {
		ExtntInfo inf = new ExtntInfo();
		
		CorfuErrorCode er = fetchMetaAt(pos, inf);
		if (er.equals(CorfuErrorCode.ERR_UNWRITTEN)) {
			throw new UnwrittenCorfuException("readExtnt({}) fails, not written yet");
		} else 
		if (er.equals(CorfuErrorCode.ERR_TRIMMED)) {
			throw new TrimmedCorfuException("readExtnt({}) fails because log was trimmed");
		} else
		if (er.equals(CorfuErrorCode.ERR_BADPARAM)) {
			throw new BadParamCorfuException("readExtnt({}) fails with bad parameter");
		} 

		return readExtnt(inf);
	}
	
	/**
	 * this utility routine does two maintenance chores:
	 * 1. attempt to 'readExtnt()' up to the current consecutive tail.
	 * 		- if any broken extent encountered, invoke 'fix()' on the entire extent, to allow skipping them.
	 * 2. attempt to see if any log-offsets were allocated by the sequencer and never written. 
	 * 		- if any offsets are preventing progress, invoke 'fix()' on individual offsets to allow skipping them,
	 * 
	 * @throws CorfuException
	 */
	@Override
	public void repairLog() throws CorfuException {
		long tail;
		
		tail = check();
		repairLog(true, tail);
	}
	
	/**
	 * same as repailLog(), but if bounded is true, repairs only up to that offset.
	 * 
	 * @throws CorfuException
	 */
	@Override
	public void repairLog(boolean bounded, long tail)  throws CorfuException {
		boolean dummytail = false; 
		if (!bounded) {
			try {
				tail = sequencer.nextpos(1);
				log.info("repairLog grabbed next token {}", tail);
				dummytail = true;
			} catch (TException e) {
				e.printStackTrace();
				tail = check();
				log.warn("repairLog cannot obtain token from sequencer; repairing only up to check()={} mark", tail);
				dummytail = false;
			}
		}
		log.info("repairLog up to {}", tail);
		
		ExtntWrap r;
		ExtntInfo nextinf = new ExtntInfo();
		CorfuErrorCode er;
		
		while (curExtntInfo.getMetaLastOff() < tail) {
			// the extent following the last read/fixed might be in one of the following states:
			//	1. completely healthy ---- then getNextMeta() will succeed, and fix() will have no effect
			//  2. partially written ---- then getNextMeta() will succeed, and fix() will mark it for skipping
			//  3. not written at all ---- then getNextMeta() will return null, and we will invoke fix() one page at a time
			//  4. trimmed --- then getNextMeta() incurs a TrimmedCorfuException(), we catch it and continue from the trimmed mark
			
			er = getNextMeta(nextinf);			
			if (er.equals(CorfuErrorCode.ERR_UNWRITTEN)) {
				nextinf.setMetaFirstOff(curExtntInfo.getMetaLastOff()+1);
				nextinf.setMetaLastOff(curExtntInfo.getMetaLastOff()+1);
			} else 
			if (er.equals(CorfuErrorCode.ERR_TRIMMED)) {	// TODO: check the current head of log and try to repair from there??
				throw new TrimmedCorfuException("repairLog fails because log was trimmed");
			} else
			if (er.equals(CorfuErrorCode.ERR_BADPARAM)) {
				throw new BadParamCorfuException("repairLog param with bad parameter");
			} 

			log.info("next extent to repair : {}", nextinf);
			if (nextinf.getMetaLastOff() >= tail) break;

			try {
				er = sunits[0].fix(nextinf);
			} catch (TException e) {
				throw new CorfuException("repairLog encountered unxpected problem");
			}
			if (!er.equals(CorfuErrorCode.OK)) {
				throw new CorfuException("repairing log failed; shouldn't happen");
			}
			updateMeta(nextinf, nextinf);
		} ;
		
		if (dummytail) {
			try {
				sunits[0].fix(new ExtntInfo(tail, tail, 0));
			} catch (TException e) {
				throw new CorfuException("repairing log failed; canno fix current token position");
			}
		}
	}
	
	
	/**
	 * @return starting offset at the log of last (successful) checkpoint
	 */
	public long checkpointLoc() throws CorfuException { return check(false); // TODO!!!
	}

	/**
	 * this method tries to mark an extent for skipping. 
	 * Note that, if even a single page in the extent is successfully marked, the entire extent can never be read, and can be skipped.
	 * 
	 * @param extntInf the meta-info of the extent we need to fix
	 */
	private void fixExtnt(ExtntInfo extntInf) throws CorfuException {
		CorfuErrorCode er;
		try {
			er = sunits[0].fix(extntInf);
		} catch (TException e) {
			e.printStackTrace();
			throw new CorfuException("fixExtnt() failed");
		}
		
		if (er.equals(CorfuErrorCode.ERR_FULL)) {
			// this should never happen, the client invoking this fill is at fault here!
			throw new OutOfSpaceCorfuException("fixExtnt(" + extntInf +") failed: full");
		} else 
		if (er.equals(CorfuErrorCode.ERR_OVERWRITE)) {
			// this may be a good thing!
			throw new OverwriteCorfuException("fixExtnt(" + extntInf+") failed (may be a good sign!): overwritten");
		} else
		if (er.equals(CorfuErrorCode.ERR_TRIMMED)) {
			throw new TrimmedCorfuException("fixExtnt(" + extntInf +") failed: position has been trimmed");
		} 
	}
			
	/**
	 * Obtain the current mark in the log, where mark is one of the log mark types: Head, tail, or contiguous tail.
	 * 
	 * @param typ the type of log mark we query
	 * @return an offset in the log corresponding to the requested mark type. 
	 * @throws CorfuException if the check() call fails or returns illegal (negative) value 
	 */
	public long checkLogMark(CorfuLogMark typ) throws CorfuException {
		long r;
		try {
			r = sunits[0].check(typ);
		} catch (TException t) {
			throw new InternalCorfuException("check() call failed on storage unit");
		}
		if (r < 0) throw new InternalCorfuException("check() call returned negative value, shouldn't happen");
		return r;
	}
	
	/**
	 * set the read mark to the requested position. 
	 * after this, invoking readExtnt will perform at the specified position.
	 * 
	 * @param pos move the read mark to this log position
	 */
	public void setMark(long pos) {
		ExtntInfo inf = new ExtntInfo(pos-1, pos-1, 0);
		updateMeta(inf, inf);
	}
		


	
	// from here down, implement the CorfuInterface xface for backward compatibility:
	// ==========================================================
	
	/**
	 * Reads a single-page entry from the log.
	 * 
	 * This is a safe read; any returned
	 * entry is guaranteed to be persistent and visible to other clients.
	 *
	 * @param	pos	log position to read
	 * @return		log entry at requested position
	 */	
	@Override
	public byte[] read(long pos) throws CorfuException {
		ExtntInfo inf = new ExtntInfo(pos, pos, 0);
		ExtntWrap ret = readExtnt(inf);
		if (! ret.getCtnt().get(0).hasArray()) {
			throw new CorfuException("read() cannot extract byte array");
		}		
		return ret.getCtnt().get(0).array();
	}
	
	/**
	 * Reads an entry from the log. Depending on the parameter, the read
	 * can be safe or unsafe. A safe read means that any returned entry is 
	 * guaranteed to be persistent and visible to other clients.
	 *
	 * @param	pos	log position to read
	 * @param	safe	signifies whether read is required to be safe or not
	 * @return		log entry at requested position
	 */	
	@Override
	public byte[] read(long pos, boolean safe) throws CorfuException{
		return read(pos);
	}
	
	/**
	 * Reads a partial fragment of an entry from the log.
	 * The read can be safe or unsafe. A safe read means that any returned entry is 
	 * guaranteed to be persistent and visible to other clients.
	 *
	 * @param	pos	log position to read
	 * @param	safe	signifies whether read is required to be safe or not
	 * @param	start	fragment start offset within the entry
	 * @return		log entry at requested position
	 */
	@Override
	public byte[] read(long pos, boolean safe, int start) throws CorfuException {
		throw new CorfuException("partial read not supported");
	}

	/**
	 * Reads a partial fragment of an entry from the log. The start and length parameters
	 * delineate the fragment within the entry.
	 * The read can be safe or unsafe. A safe read means that any returned entry is 
	 * guaranteed to be persistent and visible to other clients.
	 *
	 * @param	pos	log position to read
	 * @param	safe	signifies whether read is required to be safe or not
	 * @param	start	fragment start offset within the entry
	 * @param	length	fragment length
	 * @return		log entry at requested position
	 */
	@Override
	public byte[] read(long pos, boolean safe, int start, int length) throws CorfuException {
		throw new CorfuException("partial read not supported");
	}

	/**
	 * Performs a prefixtrim on the log, trimming all entries before given position.
	 *
	 * @param	pos	log position to prefixtrim before
	 */
	@Override
	public void trim(long pos) throws CorfuException {		
		boolean ret = false;
		try {
			ret = sunits[0].trim(pos);
		} catch (TException e) {
			e.printStackTrace();
			throw new CorfuException("trim() failed");
		}
		
		if (!ret) { 
			throw new BadParamCorfuException("trim() failed. probably bad offset parameter");

		}
	}


	/**
	 * Performs a prefixtrim or an offsettrim on the log.
	 *
	 * @param	pos	log position to prefixtrim before or offsettrim at
	 * @param	offsettrim	signifies whether an offsettrim should be performed or a prefixtrim
	 */
	@Override
	public void trim(long pos, boolean offsettrim) throws CorfuException {
		if (offsettrim)
			throw new UnsupportedCorfuException("offset-trim not supported");
		else
			trim(pos);
	}
	
	/**
	 * Fills a hole in the log. 
	 *
	 * @param	pos	log position to fill
	 * @param	a junk buffer (ignored)
	 */
	@Override
	public void fill(long pos, byte[] junkbytes) throws CorfuException { repairLog(true, pos); }
	
	/**
	 * Returns the current non-contiguous tail of the Corfu log. This is the
	 * first unwritten position in the log after which all positions are unwritten.
	 *
	 * @return      current non-contiguous tail
	 */
	@Override
	public long check() throws CorfuException {
		return checkLogMark(CorfuLogMark.TAIL);
	}

	/**
	 * Returns the current tail of the Corfu log, either contiguous or non-contiguous,
	 * depending on the parameter. The contiguous tail is the
	 * first unwritten position in the log; the non-contiguous tail is the first
	 * unwritten position after which all positions are unwritten.
	 *
	 * @param	contiguous	signifies whether contiguous or non-contiguous tail is needed
	 * @return		current tail
	 */
	@Override
	public long check(boolean contiguous) throws CorfuException {
		if (contiguous)
			return checkLogMark(CorfuLogMark.CONTIG);
		else
			return checkLogMark(CorfuLogMark.TAIL);
	}

	/**
	 * Returns the current tail of the Corfu log, either contiguous or non-contiguous, and 
	 * either cached or non-cached, depending on the parameter. The contiguous tail is the
	 * first unwritten position in the log; the non-contiguous tail is the first
	 * unwritten position after which all positions are unwritten. If the cached
	 * parameter is set, a cached value is returned without incurring network traffic.
	 *
	 * @param	contiguous	signifies whether contiguous or non-contiguous tail is needed
	 * @param	cached	signifies whether a cached value is requested or not
	 * @return		current tail
	 */
	@Override
	public long check(boolean contiguous, boolean cached) throws CorfuException {
		return check(contiguous); // TODO?
	}

	/**
	 * Appends an entry to the log.
	 *
	 * @param	buf	the buffer to append to the log
	 * @return		position that buffer was appended at
	 */
	@Override
	public long append(byte[] buf) throws CorfuException {
		if (buf.length != grainsize()) {
			throw new BadParamCorfuException("append() expects fixed-size argument; use varappend() instead");
		}
		return appendExtnt(buf, grainsize(), false);
	}

	/**
	 * Appends an entry to the log.
	 *
	 * @param	buf	the buffer to append to the log
	 * @param	bufsize	size of buffer to append
	 * @return		position that buffer was appended at
	 */
	public long append(byte[] buf, int bufsize) throws CorfuException {
		throw new CorfuException("append with variable size is depracated; use varAppend instead");
	}
	
}
