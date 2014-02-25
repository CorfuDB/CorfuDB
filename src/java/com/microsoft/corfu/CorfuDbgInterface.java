package com.microsoft.corfu;

public interface CorfuDbgInterface {
	/**
	 * fetch debugging info associated with the specified offset.
	 * 
	 * @param offset the inquired position 
	 * @return an ExtntWrap object
	 * @throws CorfuException
	 *     TrimmedCorfuException, BadParam, Unwritten, with the obvious meanings
	 */
	public ExtntWrap dbg(long offset) throws CorfuException;
	
	/**
	 * utility function to grab tcnt tokens from the sequencer. used for debugging.
	 * 
	 * @param tcnt the number of tokens to grab from sequencer
	 * @throws CorfuException is thrown in case of unexpected communication problem with the sequencer
	 */
	public void grabtokens(int tcnt) throws CorfuException;
	
	/**
	 * write a byte-buffer at the specified offset. used for debugging.
	 * bypasses the sequencer, and goes directly to a certain offset to attempt filling the position.
	 * 
	 * @param offset the position to fill
	 * @param buf  the buffer to write
	 */
	public void write(long offset, byte[] buf) throws CorfuException ;

}
