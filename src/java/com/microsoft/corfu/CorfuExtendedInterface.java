package com.microsoft.corfu;

/**
 * @author dalia
 *
 */
public interface CorfuExtendedInterface extends CorfuInterface {
	
	
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
	public byte[] forceread(long pos) throws CorfuException;
	
	/**
	 * @throws CorfuException
	 * 
	 * this utility routing attempts to fill up log holes up to its current tail.
	 */
	public void repairlog() throws CorfuException;
}
