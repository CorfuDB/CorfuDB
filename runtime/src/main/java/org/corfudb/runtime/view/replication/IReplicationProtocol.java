package org.corfudb.runtime.view.replication;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.view.Layout;


/** The public interface to a replication protocol.
 *
 * <p>A replication protocol exposes three public functions, which
 * permit reading and writing to the log.
 *
 * <p>Created by mwei on 4/6/17.
 */
public interface IReplicationProtocol {

    /** Write data to the log at the given address.
     *
     * <p>This function blocks until -a- write at the global address
     * is committed to the log.
     *
     * <p>If the write which is committed to the log was this write,
     * the function returns normally.
     *
     * <p>If the write which was committed to the log was not the result
     * of this call, an OverwriteException is thrown.
     *
     * @param  layout               The layout to use for the write.
     * @param  data                 The ILogData to write to the log.
     * @throws OverwriteException   If a write was committed to the log and
     *                              it was not the result of this call.
     */
    void write(Layout layout, ILogData data) throws OverwriteException;

    /** Read data from a given address.
     *
     * <p>This function only returns committed data. If the
     * address given has not committed, the implementation may
     * either block until it is committed, or commit a hole filling
     * entry to that address.
     *
     * @param  layout              The layout to use for the read.
     * @param globalAddress        The global address to read the data from.
     * @return                     The data that was committed at the
     *                             given global address, committing a hole
     *                             filling entry if necessary.
     */
    @Nonnull ILogData read(Layout layout, long globalAddress);

    /** Read data from all the given addresses.
     *
     * <p>This method functions exactly like a read, except
     * that it returns the result for multiple addresses.
     *
     * <p>An implementation may optimize for this type of
     * bulk request, but the default implementation
     * just performs multiple reads (possible in parallel).
     *
     * @param layout                The layout to use for the readAll.
     * @param globalAddresses       A list of addresses to read from.
     * @return                      A map of addresses to committed
     *                              addresses, hole filling if necessary.
     */
    default @Nonnull
            Map<Long, ILogData> readAll(Layout layout, List<Long> globalAddresses) {
        return globalAddresses.parallelStream()
                .map(a -> new AbstractMap.SimpleImmutableEntry<>(a, read(layout, a)))
                .collect(Collectors.toMap(r -> r.getKey(), r -> r.getValue()));
    }

    /** Read data from a range.
     *
     * <p>This method functions exactly like a readAll, except
     * that it returns the result for a range of addresses.
     *
     * <p>An implementation may optimize for this type of
     * bulk request, but the default implementation
     * just performs multiple reads (possible in parallel).
     *
     * @param layout                The layout to use for the readRange.
     * @param globalAddresses       A list of addresses to read from.
     * @return                      A map of addresses to committed
     *                              addresses, hole filling if necessary.
     */
    default @Nonnull
    Map<Long, ILogData> readRange(Layout layout, Set<Long> globalAddresses) {
        return globalAddresses.parallelStream()
                .map(a -> new AbstractMap.SimpleImmutableEntry<>(a, read(layout, a)))
                .collect(Collectors.toMap(r -> r.getKey(), r -> r.getValue()));
    }

    /** Peek data from a given address.
     *
     * <p>This function -may- return null if there was no entry
     * committed at the given global address, otherwise it
     * returns committed data at the given global address. It
     * does not attempt to hole fill if there was no entry.
     *
     * @param  layout              The layout to use for the peek.
     * @param globalAddress        The global address to peek from.
     * @return                     The data that was committed at the
     *                             given global address, or NULL, if
     *                             there was no entry committed.
     */
    ILogData peek(Layout layout, long globalAddress);

    /** Peek data from all the given addresses.
     *
     * <p>This method functions exactly like a peek, except
     * that it returns the result for multiple addresses.
     *
     * <p>An implementation may optimize for this type of
     * bulk request, but the default implementation
     * just performs multiple peeks (possibly in parallel).
     *
     * @param  layout              The layout to use for the peekAll.
     * @param globalAddresses       A set of addresses to read from.
     * @return                      A map of addresses to uncommitted
     *                              addresses, without hole filling.
     */
    default @Nonnull Map<Long, ILogData> peekAll(Layout layout, Set<Long> globalAddresses) {
        return globalAddresses.parallelStream()
                .map(a -> new AbstractMap.SimpleImmutableEntry<>(a, peek(layout, a)))
                .collect(Collectors.toMap(r -> r.getKey(), r -> r.getValue()));
    }

}
