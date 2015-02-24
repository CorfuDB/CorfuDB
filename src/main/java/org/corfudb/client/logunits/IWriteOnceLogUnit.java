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

package org.corfudb.client.logunits;

import org.corfudb.client.NetworkException;
import org.corfudb.client.UnwrittenException;
import org.corfudb.client.TrimmedException;
import org.corfudb.client.OverwriteException;

/**
 * This interface represents the simplest type of log unit.
 * Write once log units provide these simple features:
 *
 * Write, which takes an address and payload and returns success/failure
 *          It is guranteed that any address once written to is immutable (write-once)
 * Read, which takes an address and returns a payload, or an error if
 *          nothing exists at that address.
 * Trim, which takes some offset and marks all addresses before, inclusive, as trimmed.
 *          Trimmed addresses return a trimmed error when written to or read from.
 *
 * All methods are synchronous, that is, they block until successful completion
 * of the command.
 */

public interface IWriteOnceLogUnit {
    void write(long address, byte[] payload) throws OverwriteException, TrimmedException, NetworkException;
    byte[] read(long address) throws UnwrittenException, TrimmedException, NetworkException;
    void trim(long address) throws NetworkException;
}

