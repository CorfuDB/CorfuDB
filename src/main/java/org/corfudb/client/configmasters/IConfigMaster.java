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

import java.util.Map;
import java.util.UUID;

/**
 * This interface represents a CorfuDB configuration master.
 */

public interface IConfigMaster extends IServerProtocol {
    public class streamInfo {
        public UUID currentLog;
        public long startPos;
    }

    boolean addStream(UUID logID, UUID streamID, long startPos);
    streamInfo getStream(UUID streamID);
    boolean addLog(UUID logID, String path);
    Map<UUID, String> getAllLogs();
    String getLog(UUID logID);
    void resetAll();
}

