/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.corfudb.sharedlog;

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
public interface ClientAPI {
	
	/**
	 * @return		a "grain" size, the equivalent of an individual Corfu log-page
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
	 * a variant of readExtnt that takes the log-offset to read an extent from.
     * The last-read position is remembered for future readExtnt() calls
	 * 
	 * @param pos           starting position to read
	 * @return an extent wrapper, containing ExtntInfo and a list of ByteBuffers, one for each individual log-entry page
	 * @throws CorfuException
	 */
	public ExtntWrap readExtnt(long pos) throws CorfuException;

	/**
	 * Appends an extent to the log. Extent will be written to consecutive log offsets.
	 * 
	 * @param ctnt          list of ByteBuffers to be written
	 * @return              the first log-offset of the written range
	 * @throws CorfuException
	 * <UL>
	 * <LI>		OutOfSpaceCorfuException indicates an attempt to append failed because storage unit(s) are full; user may try trim()
	 * <LI>		OverwriteException indicates that an attempt to append failed because of an internal race; user may retry
	 * <LI>		BadParamCorfuException or a general CorfuException indicate an internal problem, such as a server crash. Might not be recoverable
	 * </UL>
	 */
	public long appendExtnt(List<ByteBuffer> ctnt) throws CorfuException;

	/**
	 * Breaks the bytebuffer is gets as parameter into grain-size buffers, and invokes appendExtnt(List<ByteBuffer>);
	 * @see #appendExtnt(List) appendExtnt(List<ByteBuffer> ctnt).
	 *
	 * @param	buf	the buffer to append to the log
	 * @param	reqsize	size of buffer to append
	 * @return		the first log-offset of the written range
	 * @throws CorfuException
	 */
	public long appendExtnt(byte[] buf, int reqsize) throws CorfuException;
	
	/**
	 * force a delay until we are notified that previously invoked writes to the log have been safely forced to persistent store.
	 * 
	 * @throws CorfuException if the call to storage-units failed; in this case, there is no gaurantee regarding data persistence.
	 */
	public void sync() throws CorfuException;
	
	/**
	 * Query the log head (the last trimmed position).
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
	 * @return starting offset at the log of last (successful) checkpoint
	 */
	// public long checkpointLoc() throws CorfuException;

}
