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
	 * <UL>
	 * <LI>		OutOfSpaceCorfuException indicates an attempt to append failed because storage unit(s) are full; user may try trim()
	 * <LI>		OverwriteException indicates that an attempt to append failed because of an internal race; user may retry
	 * <LI>		BadParamCorfuException or a general CorfuException indicate an internal problem, such as a server crash. Might not be recoverable
	 * </UL>
	 */
	public long appendExtnt(List<ByteBuffer> ctnt, boolean autoTrim) throws CorfuException;
	public long appendExtnt(List<ByteBuffer> ctnt) throws CorfuException;

	/**
	 * Breaks the bytebuffer is gets as parameter into grain-size buffers, and invokes appendExtnt(List<ByteBuffer>);
	 * @see #appendExtnt(List, boolean) appendExtnt(List<ByteBuffer> ctnt, boolean autoTrim).
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
	 * force a delay until we are notified that previously invoked writes to the log have been safely forced to persistent store.
	 * 
	 * @throws CorfuException if the call to storage-units failed; in this case, there is no gaurantee regarding data persistence.
	 */
	public void sync() throws CorfuException;

	/**
	 * Query the log head. 
	 *  
	 * @return the current head's index 
	 * @throws CorfuException if the call fails or returns illegal (negative) value 
	 */
	public long queryhead() throws CorfuException;
	
	/**
	 * Query the log tail. 
	 *  
	 * @return the current tail's index 
	 * @throws CorfuException if the call fails or returns illegal (negative) value 
	 */
	public long querytail() throws CorfuException;
	
	/**
	 * Query the last known checkpoint position. 
	 *  
	 * @return the last known checkpoint position.
	 * @throws CorfuException if the call fails or returns illegal (negative) value 
	 */
	public long queryck() throws CorfuException;
	
	/**
	 * inform about a new checkpoint mark. 
	 *  
	 * @param off the offset of the new checkpoint
	 * @throws CorfuException if the call fails 
	 */
	public void ckpoint(long off) throws CorfuException;
	
	/**
	 * set the read mark to the requested position. 
	 * after this, invoking readExtnt will perform at the specified position.
	 * 
	 * @param pos move the read mark to this log position
	 * @throws CorfuException
	 */
	public void setMark(long pos);
		
	/**
	 * Try to fix the log so that the next invocation of readExtnt() succeeds.
	 * 
	 * @return the next position to read from
	 * @throws CorfuException
	 */
	public long repairNext() throws CorfuException;

	/**
	 * @return starting offset at the log of last (successful) checkpoint
	 */
	// public long checkpointLoc() throws CorfuException;
	
}
