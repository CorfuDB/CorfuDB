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
	 * Reads a range of log-entries (rather than one).
	 * 
	 * @param pos           starting position to read
	 * @param reqsize       requested size to read (must be an integer multiple of entrysize(), and less than 4GB)
	 * @return              list of ByteBuffers, one for each read entry
	 * @throws CorfuException
	 */
	public List<ByteBuffer> varRead(long pos, int reqsize) throws CorfuException;

	/**
	 * Appends a list of log-entries (rather than one). Entries will be written to consecutive log offsets.
	 *
	 * @param ctnt          list of ByteBuffers to be written
	 * @return              the first log-offset of the written range 
	 * @throws CorfuException
	 */
	public long varAppend(List<ByteBuffer> ctnt) throws CorfuException;

	/**
	 * Appends a variable-length entry to the log. 
	 * Breaks the entry into fixed-size buffers, and invokes varappend(List<ByteBuffer>);
	 *
	 * @param	buf	the buffer to append to the log
	 * @param	bufsize	size of buffer to append
	 * @return		position that buffer was appended at
	 */
	public long varAppend(byte[] buf, int reqsize) throws CorfuException;
	
	/**
	 * @see com.microsoft.corfu.CorfuInterface#read(long)
	 * 
	 * fetch a log entry at specified position. 
	 * If the entry at the specified position is not (fully) written yet, 
	 * this call incurs a hole-filling at pos

	 * @param pos position to (force successful) read from
	 * @return an array of bytes with the content of the requested position
	 * @throws CorfuException
	 */
	public List<ByteBuffer> forceRead(long pos) throws CorfuException;
	public List<ByteBuffer> forceRead(long pos, int reqsize) throws CorfuException;
	
	/**
	 * this utility routing attempts to fill up log holes up to its current tail.
	 * 
	 * @throws CorfuException
	 */
	public void repairLog() throws CorfuException;
	
	/**
	 * this utility routing attempts to fill up log holes up to specified offset;
	 * if bounded is false, it attempts to fill up to its current tail, same as repairlog() with no params.
	 * 
	 * @throws CorfuException
	 */
	public void repairLog(boolean bounded, long off) throws CorfuException;

	/**
	 * @return starting offset at the log of last (successful) checkpoint
	 */
	public long checkpointLoc() throws CorfuException;
	
	/**
	 * Like varAppend, but if the log is full, trims to the latest checkpoint-mark (and possibly fills 
	 * holes to make the log contiguous up to that point). It then retries
	 * 
	 * If successful, this method will not leave a hole in the log. 
	 * Conversely, calling append() with a failure, then trim() + append(), will leave a hole, 
	 * because the token assigned for append() the first time goes unused.
	 *  
	 * @param buf            buffer to be written
	 * @param bufsize        number of bytes to be written out of 'buf'
	 * @return               the first log offset of the consecutively appended range
	 */
	public long forceAppend(byte[] buf, int bufsize) throws CorfuException;

	/**
	 * Like varAppend, but if the log is full, trims to the latest checkpoint-mark (and possibly fills 
	 * holes to make the log contiguous up to that point). It then retries
	 * 
	 * If successful, this method will not leave a hole in the log. 
	 * Conversely, calling append() with a failure, then trim() + append(), will leave a hole, 
	 * because the token assigned for append() the first time goes unused.
	 *  
	 * @param ctnt    list of requested buffers to append
	 * @return        the first log offset of the consecutively appended range
	 */
	public long forceAppend(List<ByteBuffer> ctnt) throws CorfuException;
}
