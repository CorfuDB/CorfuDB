package org.corfudb.runtime.view.replication;

import org.corfudb.protocols.logprotocol.StreamData;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.exceptions.OverwriteException;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.UUID;

/** The public interface to a replication protocol.
 *
 * A replication protocol exposes three public functions, which
 * permit reading and writing to the log.
 *
 * Created by mwei on 4/6/17.
 */
public interface IReplicationProtocol {

    /** Write data to the log at the given address.
     *
     * This function blocks until -a- write at the global address
     * is committed to the log.
     *
     * If the write which is committed to the log was this write,
     * the function returns normally.
     *
     * If the write which was committed to the log was not the result
     * of this call, an OverwriteException is thrown.
     *
     * @param globalAddress         The global address to write the data at.
     * @param entryMap              A map of stream IDs to stream entries to
     *                              write to the log.
     * @throws OverwriteException   If a write was committed to the log and
     *                              it was not the result of this call.
     */
    void write(long globalAddress,
               @Nonnull Map<UUID, StreamData> entryMap) throws OverwriteException;

    /** Read data from a given address.
     *
     * This function only returns committed data. If the
     * address given has not committed, the implementation may
     * either block until it is committed, or commit a hole filling
     * entry to that address.
     *
     * @param globalAddress        The global address to read the data from.
     * @return                     The data that was committed at the
     *                             given global address, committing a hole
     *                             filling entry if necessary.
     */
    @Nonnull ILogData read(long globalAddress);

    /** Peek data from a given address.
     *
     * This function -may- return null if there was no entry
     * committed at the given global address, otherwise it
     * returns committed data at the given global address. It
     * does not attempt to hole fill if there was no entry.
     *
     * @param globalAddress        The global address to peek from.
     * @return                     The data that was committed at the
     *                             given global address, or NULL, if
     *                             there was no entry committed.
     */
    ILogData peek(long globalAddress);

}
