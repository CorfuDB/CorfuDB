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

package org.corfudb.runtime.protocols.sequencers;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.protocols.IServerProtocol;

import java.util.UUID;

/**
 * This interface represents a sequencer with support for streaming. Streaming
 * sequencers store additional information about the streams they are serving.
 */

public interface IStreamSequencer extends IServerProtocol {
    long sequenceGetNext(UUID stream, int count) throws NetworkException;
    long sequenceGetCurrent(UUID stream) throws NetworkException;
    void setAllocationSize(UUID stream, int count) throws NetworkException;
}

