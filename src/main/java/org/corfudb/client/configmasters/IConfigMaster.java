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

package org.corfudb.client.configmasters;
import org.corfudb.client.IServerProtocol;
import org.corfudb.client.NetworkException;
import org.corfudb.client.UnwrittenException;
import org.corfudb.client.TrimmedException;
import org.corfudb.client.OverwriteException;

import org.corfudb.client.gossip.IGossip;
import org.corfudb.client.StreamData;
import java.util.Map;
import java.util.UUID;

/**
 * This interface represents a CorfuDB configuration master.
 */

public interface IConfigMaster extends IServerProtocol {

    /**
     * Adds a new stream to the system.
     *
     * @param logID         The ID of the log the stream starts on.
     * @param streamID      The streamID of the stream.
     * @param startPos      The start position of the stream.
     *
     * @return              True if the stream was successfully added to the system, false otherwise.
     */
    boolean addStream(UUID logID, UUID streamID, long startPos);

    /**
     * Gets information about a stream in the system.
     *
     * @param streamID      The ID of the stream to retrieve.
     *
     * @return              A StreamData object containing information about the stream, or null if the
     *                      stream could not be found.
     */
    StreamData getStream(UUID streamID);

    /**
     * Adds a log to the system.
     *
     * @param logID         The ID of the log to add.
     * @param path          True, if the log was added to the system, or false otherwise.
     */
    boolean addLog(UUID logID, String path);

    /**
     * Gets information about all logs known to the system.
     *
     * @return              A map containing all logs known to the system.
     */
    Map<UUID, String> getAllLogs();

    /**
     * Gets the configuration string for a log, given its id.
     *
     * @param logID         The ID of the log to retrieve.
     *
     * @return              The configuration string used to access that log.
     */
    String getLog(UUID logID);

    /**
     * Sends gossip to this configuration master. Unreliable.
     *
     * @param gossip        The gossip object to send to the remote configuration master.
     */
    void sendGossip(IGossip gossip);

    /**
     * Resets the entire log, and increments the epoch. Use only during testing to restore the system to a
     * known state.
     */
    void resetAll();
}

