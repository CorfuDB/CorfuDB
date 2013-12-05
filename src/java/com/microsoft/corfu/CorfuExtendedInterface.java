package com.microsoft.corfu;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author dalia
 *
 */
/**
 * @author dalia
 *
 */
public interface CorfuExtendedInterface extends CorfuInterface {
	
	/**
	 * Returns a "grain" size, the equivalent of an individual Corfu log-page
	 *
	 * @return		entry size
	 */
	public int grainsize() throws CorfuException;

	/**
	 * Reads the next extent; it remembers the last read extent (starting with zero).
	 * 
	 * @return an extent wrapper, containing ExtntInfo and a list of ByteBuffers, one for each individual log-entry page
	 * @throws CorfuException
	 */
	public ExtntWrap readExtnt() throws CorfuException;

	/**
	 * a variant of readnext that takes the first log-offset position to read next extent from.
	 * 
	 * @param pos           starting position to read
	 * @return an extent wrapper, containing ExtntInfo and a list of ByteBuffers, one for each individual log-entry page
	 * @throws CorfuException
	 */
	public ExtntWrap readExtnt(long pos) throws CorfuException;

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
	public long appendExtnt(List<ByteBuffer> ctnt, boolean autoTrim) throws CorfuException;
	public long appendExtnt(List<ByteBuffer> ctnt) throws CorfuException;

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
	public long appendExtnt(byte[] buf, int reqsize, boolean autoTrim) throws CorfuException;
	public long appendExtnt(byte[] buf, int reqsize) throws CorfuException;
	
	/**
	 * Obtain the current mark in the log, where mark is one of the log mark types: Head, tail, or contiguous tail.
	 *    Head is the lowest non-trimmed log offset
	 *    Tail is the offset succeeding the last position filled in the log
	 *    Contiguous-tail is the offset succeeding the last position in the contiguous filled prefix of the log
	 * 
	 * @param typ the type of log mark we query
	 * @return an offset in the log corresponding to the requested mark type. 
	 * @throws CorfuException if the check() call fails or returns illegal (negative) value 
	 */
	public long checkLogMark(CorfuLogMark typ) throws CorfuException;
	
	/**
	 * set the read mark to the requested position. 
	 * after this, invoking readExtnt will perform at the specified position.
	 * 
	 * @param pos move the read mark to this log position
	 * @throws CorfuException
	 */
	public void setMark(long pos);
		
	public void repairNext() throws CorfuException;

	/**
	 * @return starting offset at the log of last (successful) checkpoint
	 */
	public long checkpointLoc() throws CorfuException;
	
	/**
	 * return the meta-info record associated with the specified offset. used for debugging.
	 * 
	 * @param offset the inquired position 
	 * @return an ExtntInfo object
	 * @throws CorfuException
	 *     TrimmedCorfuException, BadParam, Unwritten, with the obvious meanings
	 */
	public ExtntInfo dbg(long offset) throws CorfuException;
	
}
