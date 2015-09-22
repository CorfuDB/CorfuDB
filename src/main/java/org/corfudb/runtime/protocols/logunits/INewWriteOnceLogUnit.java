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

import lombok.Data;
import org.corfudb.infrastructure.NettyLogUnitServer;
import org.corfudb.infrastructure.thrift.Hint;
import org.corfudb.infrastructure.thrift.Hints;
import org.corfudb.infrastructure.thrift.ReadCode;
import org.corfudb.infrastructure.thrift.ReadResult;
import org.corfudb.infrastructure.wireprotocol.IMetadata;
import org.corfudb.infrastructure.wireprotocol.NettyLogUnitReadResponseMsg;
import org.corfudb.runtime.*;
import org.corfudb.runtime.protocols.IServerProtocol;

import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

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

public interface INewWriteOnceLogUnit extends IServerProtocol {

    enum WriteResult {
        OK,
        TRIMMED,
        OVERWRITE,
        RANK_SEALED,
        OOS
    }

    enum ReadResultType {
        EMPTY,
        DATA,
        FILLED_HOLE,
        TRIMMED
    }

    @Data
    class ReadResult implements IMetadata {
        final ReadResultType result;
        final Object payload;
        final EnumMap<NettyLogUnitServer.LogUnitMetadataType, Object> metadataMap;

        public ReadResult(NettyLogUnitReadResponseMsg m)
        {
            switch (m.getResult())
            {
                case DATA:
                    result = ReadResultType.DATA;
                    payload = m.getPayload();
                    metadataMap = m.getMetadataMap();
                    break;
                case EMPTY:
                    result = ReadResultType.EMPTY;
                    payload = null;
                    metadataMap = m.getMetadataMap();
                    break;
                case FILLED_HOLE:
                    result = ReadResultType.FILLED_HOLE;
                    payload = null;
                    metadataMap = m.getMetadataMap();
                    break;
                case TRIMMED:
                    result = ReadResultType.TRIMMED;
                    payload = null;
                    metadataMap = m.getMetadataMap();
                    break;
                default:
                    result = ReadResultType.EMPTY;
                    metadataMap = m.getMetadataMap();
                    payload = null;
            }
        }
    }

    /** Asynchronously write to the logging unit.
     *
     * @param address       The address to write to.
     * @param streams       The streams, if any, that this write belongs to.
     * @param rank          The rank of this write (used for quorum replication).
     * @param writeObject   The object, pre-serialization, to write.
     * @return              A CompletableFuture which will complete with the WriteResult once the
     *                      write completes.
     */
    CompletableFuture<WriteResult> write(long address, Set<UUID> streams, long rank, Object writeObject);

    /** Asynchronously write to the logging unit, giving a logical stream position.
     *
     * @param address                       The address to write to.
     * @param streamsAndLogicalAddresses    The streams, and logical addresses, if any, that this write belongs to.
     * @param rank                          The rank of this write (used for quorum replication).
     * @param writeObject                   The object, pre-serialization, to write.
     * @return                              A CompletableFuture which will complete with the WriteResult once the
     *                                      write completes.
     */
    CompletableFuture<WriteResult> write(long address, Map<UUID, Long> streamsAndLogicalAddresses, long rank, Object writeObject);

    /** Asynchronously read from the logging unit.
     *
     * @param address       The address to read from.
     * @return              A CompletableFuture which will complete with a ReadResult once the read
     *                      completes.
     */
    CompletableFuture<ReadResult> read(long address);

    /** Send a hint to the logging unit that a stream can be trimmed.
     *
     * @param stream        The stream to trim.
     * @param prefix        The prefix of the stream, as a global physical offset, to trim.
     */
    void trim(UUID stream, long prefix);

    /** Fill a hole at a given address.
     *
     * @param address       The address to fill a hole at.
     */
    void fillHole(long address);

    /** Force the garbage collector to begin garbage collection. */
    void forceGC();

    /** Change the default garbage collection interval. */
    void setGCInterval(long millis);
}

