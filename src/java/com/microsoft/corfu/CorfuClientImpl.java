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
			inf  = new ExtntInfo(offset, ctnt.size(), 0);
			er = sunits[0].write(inf, ctnt);
		} catch (TException e) {
			e.printStackTrace();
			throw new CorfuException("append() failed");
		}
		
		if (er.equals(CorfuErrorCode.ERR_FULL) && autoTrim) {
			try {
				long ckoff = checkpointLoc();
				long contigoff = checkLogMark(CorfuLogMark.CONTIG);
				if (ckoff > contigoff) ckoff = contigoff;
				
                log.info("log full! forceappend trimming to " + ckoff);
				trim(ckoff);
				er = sunits[0].write(inf, ctnt);
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
		
		return offset;
	}
	
	ExtntInfo lastReadExtntInfo = new ExtntInfo(-1, 1, 0); // meta-info of last successfully read extent  
	ExtntInfo PrefetchExtntInfo = new ExtntInfo(-1, 0, 0); // prefetched meta-info from last successful read, if any
	
	/** utility method to copy one Extnt meta-info record to another
	 * @param from source ExtntInfo
	 * @param to target ExtntInfo
	 */
	private void ExtntInfoCopy(ExtntInfo from, ExtntInfo to) {
		to.setFlag(from.getFlag());
		to.setMetaFirstOff(from.getMetaFirstOff());
		to.setMetaLength(from.getMetaLength());
	}
	
	/** utility method to compute the log-offset succeeding an extent
	 * @param inf the extent's meta-info
	 * @return the offset succeeding this extent
	 */
	private long ExtntSuccessor(ExtntInfo inf) { return inf.getMetaFirstOff() + inf.getMetaLength(); }
	
	/**
	 * obtain the ExtntInfo for the next extent to read.
	 * 
	 * this method makes use of the state we store in lastReadExtntInfo, lastTriedExtntInfo and PrefetchExtntInfo, 
	 * 
	 * @param nextinf an ExtntInfo object to fill with the next log extent meta-info.
	 * @throws CorfuException if read from server fails. Specifically,
	 * 		UnwrittenCorfuException if the next meta-record hasn't been written yet;
	 * 		TrimmedCorfuException if the next position following the last extent has been meanwhile trimmed
	 */
	private void getNextMeta(ExtntInfo inf) throws CorfuException {
		synchronized(lastReadExtntInfo) {
			for (;;) {
				if (PrefetchExtntInfo.getMetaFirstOff() > lastReadExtntInfo.getMetaFirstOff()) {
					if ((PrefetchExtntInfo.getFlag() & commonConstants.SKIPFLAG) != 0) {
						// in this case, the nexExtntInfo we fetched previously must be skipped 
						// we try to progress both curExtntInfo and nexExtntInfo to the subsequent extent
						log.debug("getNextMeta skip {}", PrefetchExtntInfo);
						ExtntInfoCopy(PrefetchExtntInfo, lastReadExtntInfo);
						fetchMetaAt(ExtntSuccessor(PrefetchExtntInfo), PrefetchExtntInfo);
						continue;
					}
				
				} else {
					// this means nexExtntInfo wasn't available for prefetching last time
					log.debug("getNextMeta for cur={}", lastReadExtntInfo);
					fetchMetaAt(ExtntSuccessor(lastReadExtntInfo), PrefetchExtntInfo);
					continue;
				}
				
				ExtntInfoCopy(PrefetchExtntInfo, inf);
				break;
			}
		}
	}
	
	/**
	 * get ExtntInfo for an extent starting at a specified log position.
	 * 
	 * @param pos the starting position of the extent 
	 * @param ref to an ExtntInfo record to fill 
	 * @throws CorfuException if read from server fails
	 */
	private void fetchMetaAt(long pos, ExtntInfo inf) throws CorfuException {
		synchronized(lastReadExtntInfo) {
			ExtntWrap ret;
		
			try {
				ret = sunits[0].readmeta(pos);
			} catch (TException e) {
				e.printStackTrace();
				throw new CorfuException("readmeta() failed");
			}
			
			CorfuErrorCode er = ret.getErr();

			if (er.equals(CorfuErrorCode.OK)) {
				ExtntInfoCopy(ret.getInf(), inf);
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
	}	

	/**
	 * Update the metadata of last-read extent and the prefetched meta of next extent
	 * @param cur the meta-info of the extent we just read
	 * @param prefetch the pre-fetched meta-info of the next extent
	 */
	private synchronized void updateMeta(ExtntInfo cur, ExtntInfo prefetch) {
		ExtntInfoCopy(cur, lastReadExtntInfo);
		ExtntInfoCopy(prefetch, PrefetchExtntInfo);
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
			CorfuHeader hdr = new CorfuHeader(inf, true, ExtntSuccessor(inf), CorfuErrorCode.OK);
			log.debug("readExtnt read(rang={}, prefertchoff={} isprefetch={})", hdr.getExtntInf(), hdr.getPrefetchOff(), hdr.isPrefetch());
			ret = sunits[0].read(hdr);
		} catch (TException e) {
			e.printStackTrace();
			throw new CorfuException("read() failed");
		}

		if (ret.getErr().equals(CorfuErrorCode.ERR_UNWRITTEN)) {
			log.info("readExtnt({}) fails, unwritten", inf);
			throw new UnwrittenCorfuException("read(" + inf +") failed: unwritten");
		} else 
		if (ret.getErr().equals(CorfuErrorCode.ERR_TRIMMED)) {
			updateMeta(inf, inf);
			log.info("readExtnt({}) fails, trimmed", inf);
			throw new TrimmedCorfuException("read(" + inf +") failed: trimmed");
		} else
		if (ret.getErr().equals(CorfuErrorCode.ERR_BADPARAM)) {
			log.info("readExtnt({}) fails, bad param", inf);
			throw new OutOfSpaceCorfuException("read(" + inf +") failed: bad parameter");
		} 

		updateMeta(inf, ret.getInf()); 	// first param is what we just read; second param is prefetch meta-info of next extent
		ret.setInf(inf);				// overwrite the prefetched meta-info, in order to return the extent info which we Wrap just read
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
		do {
			getNextMeta(nextinf);
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
		
		log.debug("readExtnt for position {}", pos);
		fetchMetaAt(pos, inf);
		log.debug("readExtnt({}) meta-info: {}", pos, inf);

		return readExtnt(inf);
	}
	
	public void repairPos(long pos) {
		
	}
	
	public void repairNext() throws CorfuException {
		ExtntInfo inf = new ExtntInfo();
		long head, tail;
		long pos;
		CorfuErrorCode fetchErr = CorfuErrorCode.OK;
		CorfuErrorCode readErr = CorfuErrorCode.OK;
		boolean skip = false;
		
		// check current log bounds
		head = checkLogMark(CorfuLogMark.HEAD); 
		tail = checkLogMark(CorfuLogMark.TAIL); 
		
		// first, get the meta-info of the next extent
		pos = ExtntSuccessor(lastReadExtntInfo);
		
		if (pos < head) {
			log.info("repairNext repositioning to head={}", head);
			pos = head;
		}
		if (pos >= tail) {
			log.info("repairNext reached log tail, finishing");
			return; // TODO do something??
		}
		
		try {
			fetchMetaAt(pos, inf);
			if (inf.getMetaFirstOff() < head) {
				// extent partially trimmed; skip
				log.info("repairNext partially-trimmed extent {}, skipping");
				skip = true;
				inf.setMetaFirstOff(head);
			}
		} catch (CorfuException e) {
			log.warn("repairNext fetchMeta({}) fails err={}", pos, e.er);
			inf.setMetaFirstOff(pos); inf.setMetaLength(1);
			skip = true;
			fetchErr = e.er;
		}
		
		
		// next, try to read it to see what is the error value
		try {
			CorfuHeader hdr = new CorfuHeader(inf, false, 0, CorfuErrorCode.OK);
			log.debug("repairNext read(range={}, prefertchoff={} isprefetch={})", hdr.getExtntInf(), hdr.getPrefetchOff(), hdr.isPrefetch());
			ExtntWrap ret = sunits[0].read(hdr);
			readErr = ret.getErr();
			if (!readErr.equals(CorfuErrorCode.OK)) skip = true;
		} catch (TException e) {
			e.printStackTrace();
			throw new CorfuException("repairNext read() failed, communication problem; quitting");
		}
		
		// finally, if either the meta-info was broken, or the content was broken, skip will be ==true.
		// in that case, we mark this entry for skipping, and we also try to mark it for skip on the storage units
		
		if (!skip) {
			updateMeta(lastReadExtntInfo, inf);
			return;
		}
		
		inf.setFlag(commonConstants.SKIPFLAG);
		updateMeta(inf, inf);
			
		// now we try to fix 'inf'
		CorfuErrorCode er;
		for (pos = inf.getMetaFirstOff(); pos < ExtntSuccessor(inf); pos++) {
			log.debug("repairNext fix pos={}", pos);
			try {
				er = sunits[0].fix(pos, inf);
			} catch (TException e1) {
				e1.printStackTrace();
				throw new CorfuException("repairNext() fix failed, communication problem; quitting");
			}
			if (er.equals(CorfuErrorCode.ERR_FULL)) {
				// TODO should we try to trim the log to the latest checkpoint and/or check if the extent range exceeds the log capacity??
				throw new OutOfSpaceCorfuException("repairNext failed, log full");
			}
		}
	}	
	
	
	/**
	 * @return starting offset at the log of last (successful) checkpoint
	 */
	public long checkpointLoc() throws CorfuException { return checkLogMark(CorfuLogMark.CONTIG); // TODO!!!
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
			if (typ.equals(CorfuLogMark.TAIL))
				r = sequencer.nextpos(0);
			else
				r = sunits[0].check(typ);
		} catch (TException t) {
			throw new InternalCorfuException("checkLogMark() failed ");
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
		ExtntInfo inf = new ExtntInfo(pos-1, 1, 0);
		updateMeta(inf, inf);
	}
		
	/**
	 * return the meta-info record associated with the specified offset. used for debugging.
	 * 
	 * @param offset the inquired position 
	 * @return an ExtntInfo object
	 * @throws CorfuException
	 *     TrimmedCorfuException, BadParam, Unwritten, with the obvious meanings
	 */
	public ExtntInfo dbg(long pos) throws CorfuException {
		ExtntInfo inf = new ExtntInfo();
		fetchMetaAt(pos, inf);
		log.debug("dbg({}) meta-info: {}", pos, inf);
		return inf;
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
		ExtntInfo inf = new ExtntInfo(pos, 1, 0);
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
	public void fill(long pos, byte[] junkbytes) throws CorfuException { repairPos(pos); }
	
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
