package com.microsoft.corfu;

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
	 * @see com.microsoft.corfu.CorfuInterface#read(long)
	 * 
	 * fetch a log entry at specified position. 
	 * If the entry at the specified position is not (fully) written yet, 
	 * this call incurs a hole-filling at pos

	 * @param pos position to (force successful) read from
	 * @return an array of bytes with the content of the requested position
	 * @throws CorfuException
	 */
	public byte[] forceread(long pos) throws CorfuException;
	
	/**
	 * this utility routing attempts to fill up log holes up to its current tail.
	 * 
	 * @throws CorfuException
	 */
	public void repairlog() throws CorfuException;
	
	/**
	 * this utility routing attempts to fill up log holes up to specified offset;
	 * if bounded is false, it attempts to fill up to its current tail, same as repairlog() with no params.
	 * 
	 * @throws CorfuException
	 */
	public void repairlog(boolean bounded, long off) throws CorfuException;

	/**
	 * @return starting offset at the log of last (successful) checkpoint
	 */
	public long checkpointloc() throws CorfuException;
	
	/**
	 * Like append, but if the log is full, trims to the latest checkpoint-mark (and possibly fills 
	 * holes to make the log contiguous up to that point). It then retries
	 * 
	 * If successful, this method will not leave a hole in the log. 
	 * Conversely, calling append() with a failure, then trim() + append(), will leave a hole, 
	 * because the token assigned for append() the first time goes unused.
	 *  
	 * @param buf buffer to be written
	 * @param bufsize number of bytes to be written out of 'buf'
	 * @return
	 */
	public long forceappend(byte[] buf, int bufsize) throws CorfuException;
}
