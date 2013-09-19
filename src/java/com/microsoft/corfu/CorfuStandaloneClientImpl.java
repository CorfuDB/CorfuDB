package com.microsoft.corfu;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.microsoft.corfu.sa.CorfuStandaloneServer;

public class CorfuStandaloneClientImpl implements com.microsoft.corfu.CorfuInterface {
	private Logger LOGGER;
	CorfuStandaloneServer.Client crf;
	TTransport transport = null;
	
	public CorfuStandaloneClientImpl(String srvrEndpoint) throws CorfuException {
		String[] tokens = srvrEndpoint.split(":");
		
		try {
			transport = new TSocket(tokens[0], Integer.valueOf(tokens[1]));
			TBinaryProtocol protocol = new TBinaryProtocol(transport);
			crf = new CorfuStandaloneServer.Client(protocol);

			LOGGER = LoggerFactory.getLogger(CorfuStandaloneClientImpl.class);
			LOGGER.trace("connected " + srvrEndpoint);
			
			transport.open();
		} catch (TTransportException e) {
			e.printStackTrace();
			throw new CorfuException("standalone client fails to connect to server");
		}
	}

	public void explicitclose() {
        transport.close();
	}

	/**
	 * Returns the current non-contiguous tail of the Corfu log. This is the
	 * first unwritten position in the log after which all positions are unwritten.
	 *
	 * @return      current non-contiguous tail
	 */
	@Override
	public synchronized long check() throws CorfuException {
		long r = -1;
		
		try {
			r = crf.check();
		} catch (TException e) {
			e.printStackTrace();
			throw new CorfuException("check() failed");
		}
		return r;
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
		return check();
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
		return check();
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
	public /* synchronized */ long append(byte[] buf) throws CorfuException {
		CorfuOffsetWrap r = null;
		
		if (buf == null) {
			LOGGER.warn("append invoked with null buf");
			throw new CorfuException("append was passed a null buf array");
		}
		LOGGER.trace("append invoked (length=" + buf.length + ")");
		if (buf.length != entrysize()) {
			LOGGER.warn("append invoked with wrong length: " + buf.length + " : expected " + entrysize());
			throw new BadParamCorfuException("append was invoked with wrong buf size " + buf.length + " : expected " + entrysize());
		}
		
		try {
			r = crf.append(ByteBuffer.wrap(buf));
		} catch (TException e) {
			e.printStackTrace();
			LOGGER.warn("append failed");
			throw new CorfuException("append() failed");
		}
		
		if (r.offerr == CorfuErrorCode.ERR_FULL) {
			LOGGER.warn("append failed, log is full");
			throw new OutOfSpaceCorfuException("append() failed");
		}
		if (r.offerr == CorfuErrorCode.ERR_BADPARAM) {
			LOGGER.warn("append was invoked with a bad parameter");
			throw new BadParamCorfuException("append() failed due to bad parameter");
		}
		
		LOGGER.trace("CorfuStandaloneClient.append completed at offset " + r.off);
		return r.off;
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
	public /* synchronized */ byte[] read(long pos) throws CorfuException {
		LOGGER.trace("read invoked for offset " + pos);

		CorfuPayloadWrap ret = null;
		try {
			ret = crf.read(pos);
		} catch (TException e) {
			e.printStackTrace();
			LOGGER.warn("read failed");
			throw new CorfuException("read() failed");
		}
		
		if (ret.err == CorfuErrorCode.ERR_TRIMMED) { 
			LOGGER.warn("read position " + pos + " failed because it has been trimmed");
			throw new TrimmedCorfuException("read(" + pos + ") failed, trimmed");
		}
		if (ret.err == CorfuErrorCode.ERR_UNWRITTEN) { 
			LOGGER.warn("read position " + pos + " failed becauase it has not been written yet");
			throw new UnwrittenCorfuException("read(" + pos + ") failed, unwritten");
		}
		if (! ret.ctnt.hasArray()) {
			LOGGER.warn("read returned a direct ByteBuffer (no array)");
			throw new CorfuException("read() cannot extract byte array");
		}

		LOGGER.trace("read position " + pos + " completed");
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
	public synchronized void trim(long pos) throws CorfuException {		
		LOGGER.trace("trim invoked up to offset " + pos);

		boolean ret = false;
		try {
			ret = crf.trim(pos);
		} catch (TException e) {
			LOGGER.warn("trim failed");
			e.printStackTrace();
			throw new CorfuException("trim() failed");
		}
		
		if (!ret) { 
			LOGGER.warn("trim rpc return false ");
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
		throw new CorfuException("trim not supported");
	}
	
	/**
	 * Fills a hole in the log, either completing a torn write or writing the supplied junk value.
	 *
	 * @param	pos	log position to fill
	 * @param	junkbytes	junk value to fill entry with
	 */
	@Override
	public void fill(long pos, byte[] junkbytes) throws CorfuException {
		throw new CorfuException("fill not supported");
	}

}
