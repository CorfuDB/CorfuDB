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

package org.corfudb.runtime.protocols.logunits;

import org.corfudb.infrastructure.thrift.ExtntInfo;
import org.corfudb.infrastructure.thrift.Hints;
import org.corfudb.runtime.*;
import org.corfudb.runtime.protocols.IServerProtocol;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * This interface represents the simplest type of stream unit.
 * Write once stream units provide these simple features:
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

public interface IWriteOnceLogUnit extends IServerProtocol {
    void write(long address, Set<UUID> streams, byte[] payload) throws OverwriteException, TrimmedException, NetworkException, OutOfSpaceException;
    byte[] read(long address, UUID stream) throws UnwrittenException, TrimmedException, NetworkException;
    void trim(long address) throws NetworkException;

    /**
     * Gets the highest address written to this log unit. Some units may not support this operation and
     * will throw an UnsupportedOperationException
     * @return                      The highest address written to this logunit.
     * @throws NetworkException     If the log unit could not be contacted.
     */
    default long highestAddress() throws NetworkException
    {
        throw new UnsupportedOperationException("Log unit doesn't support querying latest address!");
    }

    default Hints readHints(long address) throws TrimmedException, NetworkException {
        throw new UnsupportedOperationException("Log unit doesn't support hints queries");
    }

    default void setHintsNext(long address, UUID stream, long nextOffset) throws TrimmedException, NetworkException {
        throw new UnsupportedOperationException("Log unit doesn't support hints updates");
    }

    default void setHintsTxDec(long address, boolean dec) throws TrimmedException, NetworkException {
        throw new UnsupportedOperationException("Log unit doesn't support hints updates");
    }

    default void setHintsFlatTxn(long address, Set<UUID> streams, byte[] flatTxn) throws TrimmedException, NetworkException {
        throw new UnsupportedOperationException("Log unit doesn't support hints updates");
    }
}

