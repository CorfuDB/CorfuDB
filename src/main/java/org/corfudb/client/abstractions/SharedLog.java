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

package org.corfudb.client.abstractions;

import org.corfudb.client.view.Sequencer;
import org.corfudb.client.view.WriteOnceAddressSpace;

import java.util.Map;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SharedLog {

    private final Logger log = LoggerFactory.getLogger(SharedLog.class);

    private Sequencer sequencer;
    private WriteOnceAddressSpace woas;

    public SharedLog(Sequencer sequencer, WriteOnceAddressSpace woas) {
        this.sequencer = sequencer;
        this.woas = woas;
    }

    public long append(byte[] data)
    {
        while (true)
        {
            try {
                long token = sequencer.getNext();
                woas.write(token, data);
                return token;
            } catch(Exception e) {
                log.warn("Issue appending to log, getting new sequence number...", e);
            }
        }
    }

    public byte[] read(long address)
    throws Exception
    {
        return woas.read(address);
    }

    public void trim(long address)
    {

    }
}
