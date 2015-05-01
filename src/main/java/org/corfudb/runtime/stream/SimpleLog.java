/**
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

package org.corfudb.runtime.stream;

import org.corfudb.runtime.TrimmedException;
import org.corfudb.runtime.UnwrittenException;
import org.corfudb.runtime.view.ISequencer;
import org.corfudb.runtime.view.IWriteOnceAddressSpace;
import org.corfudb.runtime.view.Sequencer;
import org.corfudb.runtime.view.WriteOnceAddressSpace;

import org.corfudb.runtime.OutOfSpaceException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

public class SimpleLog implements ILog {

    private final Logger log = LoggerFactory.getLogger(SimpleLog.class);

    private ISequencer sequencer;
    private IWriteOnceAddressSpace woas;

    public SimpleLog(ISequencer sequencer, IWriteOnceAddressSpace woas) {
        this.sequencer = sequencer;
        this.woas = woas;
    }

    /**
     * Appends data to the tail (end) of the log.
     *
     * @param data The object to append to tail of the log.
     * @return A timestamp representing the address the data was written to.
     * @throws OutOfSpaceException If there is no space remaining on the log.
     */
    @Override
    public ITimestamp append(Serializable data) throws OutOfSpaceException {
        while (true)
        {
            try {
                long token = sequencer.getNext();
                woas.write(token, data);
                return new SimpleTimestamp(token);
            } catch(Exception e) {
                log.warn("Issue appending to stream, getting new sequence number...", e);
            }
        }
    }

    /**
     * Reads data from an address in the log.
     *
     * @param address A timestamp representing the location to read from.
     * @return The object at that address.
     * @throws UnwrittenException If the address was unwritten.
     * @throws TrimmedException   If the address has been trimmed.
     */
    @Override
    public Object read(ITimestamp address) throws UnwrittenException, TrimmedException, ClassNotFoundException, IOException {
        SimpleTimestamp s = (SimpleTimestamp) address;
        return woas.readObject(s.address);
    }

    /**
     * Trims a prefix of the log, [0, address) exclusive.
     *
     * @param address A timestamp representing the location to trim.
     */
    @Override
    public void trim(ITimestamp address) {

    }
}
