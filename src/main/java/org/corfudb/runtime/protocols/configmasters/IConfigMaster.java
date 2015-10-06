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

package org.corfudb.runtime.protocols.configmasters;
import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.protocols.IServerProtocol;

import org.corfudb.runtime.view.CorfuDBView;
import org.corfudb.runtime.view.StreamData;
import java.util.Map;
import java.util.UUID;

/**
 * This interface represents a CorfuDB configuration master.
 */
public interface IConfigMaster extends IServerProtocol {

    /**
     * Adds a new stream to the system.
     *
     * @param logID         The ID of the stream the stream starts on.
     * @param streamID      The streamID of the stream.
     * @param startPos      The start position of the stream.
     *
     * @return              True if the stream was successfully added to the system, false otherwise.
     */
    boolean addStream(UUID logID, UUID streamID, long startPos);
    /**
     * Adds a new stream to the system.
     *
     * @param logID         The ID of the stream the stream starts on.
     * @param streamID      The streamID of the stream.
     * @param startPos      The start position of the stream.
     *
     * @return              True if the stream was successfully added to the system, false otherwise.
     */
    boolean addStreamCM(UUID logID, UUID streamID, long startPos, boolean nopass);

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
     * Adds a stream to the system.
     *
     * @param logID         The ID of the stream to add.
     * @param path          True, if the stream was added to the system, or false otherwise.
     */
    boolean addLog(UUID logID, String path);

    /**
     * Gets information about all logs known to the system.
     *
     * @return              A map containing all logs known to the system.
     */
    Map<UUID, String> getAllLogs();

    /**
     * Gets the configuration string for a stream, given its id.
     *
     * @param logID         The ID of the stream to retrieve.
     *
     * @return              The configuration string used to access that stream.
     */
    String getLog(UUID logID);

    /**
     * Resets the entire stream, and increments the epoch. Use only during testing to restore the system to a
     * known state.
     */
    void resetAll();

    /**
     * Gets the current view from the configuration master.
     * @return  The current view.
     */
    CorfuDBView getView();

    /**
     * Request reconfiguration due to a network exception.
     * @param e             The network exception that caused the reconfiguration request.
     */
    void requestReconfiguration(NetworkException e);

    /**
     * Force the configuration master to install a new view. This method should only be called during
     * testing.
     * @param v             The new view to install.
     */
    void forceNewView(CorfuDBView v);
}

