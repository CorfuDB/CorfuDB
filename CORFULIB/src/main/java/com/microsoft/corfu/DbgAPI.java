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
package com.microsoft.corfu;

public interface DbgAPI {
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
