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

package org.corfudb.runtime.entries;

import org.corfudb.runtime.stream.Timestamp;

import java.io.Serializable;

import java.io.IOException;

import org.corfudb.runtime.OverwriteException;

/**
 * This is a bundle entry interface, which is encountered inside the move entries of remotely bundled
 * logs. It enables remote logs to easily write results to remotes.
 */
public interface IBundleEntry {

    /**
     * Writes a payload in the remote slot, and collects the result of the bundle into the local
     * stream in 1 RTT.
     *
     * @param payload       The payload to insert into the slot.
     *
     * @return              The timestamp for the remote append operation, or null, if there was no remote slot.
     */
    public Timestamp writeSlot(byte[] payload)
    throws OverwriteException, IOException;

    /**
     * Writes a serializable payload in the remote slot, and collects the result of the bundle into the local
     * stream in 1 RTT.
     *
     * @param payloadObject       The payload to insert into the slot.
     *
     * @return                      The timestamp for the remote append operation, or null, if there was no remote slot.
     */
    public Timestamp writeSlot(Serializable payloadObject)
    throws OverwriteException, IOException;
}
