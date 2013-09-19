package com.microsoft.corfu;

import java.nio.ByteBuffer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.microsoft.corfu.CorfuException;
import com.microsoft.corfu.sequencer.CorfuSequencer;
import com.microsoft.corfu.sunit.CorfuUnitServer;
import com.microsoft.corfu.CorfuPayloadWrap;

public class CorfuClientImpl implements com.microsoft.corfu.CorfuExtendedInterface {
	CorfuConfigManager CM;
	CorfuUnitServer.Client[] sunits;
	CorfuSequencer.Client sequencer;
	TTransport[] transports;
	
	public CorfuClientImpl(CorfuConfigManager CM) throws CorfuException {
		
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
				System.out.println("cleint connection open with server  " + cn.hostname + ":" + cn.port);
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
				System.out.println("client connection open with sequencer  " + cn.hostname + ":" + cn.port);
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
	 * Returns the current non-contiguous tail of the Corfu log. This is the
	 * first unwritten position in the log after which all positions are unwritten.
	 *
	 * @return      current non-contiguous tail
	 */
	@Override
	public long check() throws CorfuException {
		return check(false);
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
		long r = -1;
		
		try {
			if (contiguous)
				r = sunits[0].checkcontiguous();
			else
				r = sunits[0].check();
		} catch (TException e) {
			e.printStackTrace();
			throw new CorfuException("check(contiguous=" + contiguous + ") failed");
		}
		return r;
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
	 * Returns the maximum size of a Corfu entry
	 *
	 * @return		entry size
	 */
	@Override
	public int entrysize() throws CorfuException{
		return 4096; // TODO
	}

	/**
	 * Appends an entry to the log.
	 *
	 * @param	buf	the buffer to append to the log
	 * @return		position that buffer was appended at
	 */
	@Override
	public long append(byte[] buf) throws CorfuException {
		long offset = -1;
		CorfuErrorCode er;
		
		try {
			offset = sequencer.nextpos();
			er = sunits[0].write(offset, ByteBuffer.wrap(buf));
		} catch (TException e) {
			e.printStackTrace();
			throw new CorfuException("append() failed");
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

	/**
	 * Appends an entry to the log.
	 *
	 * @param	buf	the buffer to append to the log
	 * @param	bufsize	size of buffer to append
	 * @return		position that buffer was appended at
	 */
	@Override
	public long append(byte[] buf, int bufsize) throws CorfuException {
		return append(buf);
	}
	
	
	/**
	 * Reads an entry from the log. This is a safe read; any returned
	 * entry is guaranteed to be persistent and visible to other clients.
	 *
	 * @param	pos	log position to read
	 * @return		log entry at requested position
	 */	
	@Override
	public byte[] read(long pos) throws CorfuException {
		CorfuPayloadWrap ret;
		try {
			ret = sunits[0].read(pos);
		} catch (TException e) {
			e.printStackTrace();
			throw new CorfuException("read() failed");
		}

		if (ret.err.equals(CorfuErrorCode.ERR_UNWRITTEN)) {
			throw new UnwrittenCorfuException("read(" + pos +") failed: unwritten");
		} else 
		if (ret.err.equals(CorfuErrorCode.ERR_TRIMMED)) {
			throw new TrimmedCorfuException("read(" + pos +") failed: trimmed");
		} else
		if (ret.err.equals(CorfuErrorCode.ERR_BADPARAM)) {
			throw new OutOfSpaceCorfuException("read(" + pos +") failed: bad parameter");
		} 

		if (! ret.ctnt.hasArray()) {
			throw new CorfuException("read() cannot extract byte array");
		}
		
		return ret.ctnt.array();
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
			throw new UnsupportedTrimCorfuException("offset-trim not supported");
		else
			trim(pos);
	}
	
	/**
	 * Fills a hole in the log, either completing a torn write or writing the supplied junk value.
	 *
	 * @param	pos	log position to fill
	 * @param	junkbytes	junk value to fill entry with
	 */
	@Override
	public void fill(long pos, byte[] junkbytes) throws CorfuException {
		CorfuErrorCode er;
		
		try {
			er = sunits[0].write(pos, ByteBuffer.wrap(junkbytes));
		} catch (TException e) {
			e.printStackTrace();
			throw new CorfuException("fill() failed");
		}
		
		if (er.equals(CorfuErrorCode.ERR_FULL)) {
			// this should never happen, the client invoking this fill is at fault here!
			throw new OutOfSpaceCorfuException("fill(" + pos +") failed: full");
		} else 
		if (er.equals(CorfuErrorCode.ERR_OVERWRITE)) {
			// this may be a good thing!
			throw new OverwriteCorfuException("fill(" + pos +") failed (may be a good sign!): overwritten");
		} else
		if (er.equals(CorfuErrorCode.ERR_BADPARAM)) {
			throw new BadParamCorfuException("fill(" + pos +") failed: bad parameter passed");
		} 
	}
	
	/* ************************************************************************************** */
	/* from here on, implement extended interface
	 */
	
	/* (non-Javadoc)
	 * @see com.microsoft.corfu.CorfuInterface#read(long)
	 * 
	 * fetch a log entry at specified position. 
	 * If the entry at the specified position is not (fully) written yet, 
	 * this call incurs a hole-filling at pos

	 * @param pos position to (force successful) read from
	 * @return an array of bytes with the content of the requested position
	 * @throws CorfuException
	 */
	@Override
	public byte[] forceread(long pos) throws CorfuException {
		byte[] ret = null;
		boolean tryrepeat = true;
		
		while (true) {
			try {
				ret = read(pos);
			} catch (UnwrittenCorfuException e) {
				try {
					byte[] buf = new byte[4096];
					fill(pos, buf);
				} catch (OverwriteCorfuException e1) {
					// This is good, means the entry has been written meanwhile!
					if (!tryrepeat) {
						throw new CorfuException("forceread: read failed even after fill");
					} else {
						tryrepeat = false;
					}
				}
			}	
			break;
		}
		return ret;
	}
	
	/**
	 * @throws CorfuException
	 * 
	 * this utility routing attempts to fill up log holes up to its current tail.
	 */
	@Override
	public void repairlog() throws CorfuException {
		long tail, contig_tail;
		
		tail = check();
		
		do {
			contig_tail = check(true);
			if (tail == contig_tail) break;
			
			try {
				byte[] ret = read(contig_tail);
			} catch (UnwrittenCorfuException e) {
				try {
					byte[] buf = new byte[4096];
					fill(contig_tail, buf);
				} catch (OverwriteCorfuException e01) {
					// This is good, means the entry has been written meanwhile!
				} catch (TrimmedCorfuException e02) {
					// This is also good, resume fixing the log
				}
			} catch (TrimmedCorfuException e1) {
				// This is good, resume fixing the log
			}
		} while (true);
	}
}
