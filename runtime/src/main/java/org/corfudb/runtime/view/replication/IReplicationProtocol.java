package org.corfudb.runtime.view.replication;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.view.RuntimeLayout;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;


/**
 * The public interface to a replication protocol.
 *
 * <p>A replication protocol exposes three public functions, which
 * permit reading and writing to the log.
 *
 * <p>Created by mwei on 4/6/17.
 */
public interface IReplicationProtocol {

    /**
     * Write data to the log at the given address.
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
     * @param runtimeLayout the RuntimeLayout stamped with layout to use for the write.
     * @param data          the ILogData to write to the log.
     * @throws OverwriteException If a write was committed to the log and
     *                            it was not the result of this call.
     */
    void write(RuntimeLayout runtimeLayout, ILogData data) throws OverwriteException;

    /**
     * Read data from a given address.
     *
     * <p>This function only returns committed data. If the
     * address given has not committed, the implementation may
     * either block until it is committed, or commit a hole filling
     * entry to that address.
     *
     * @param runtimeLayout the RuntimeLayout stamped with layout to use for the read.
     * @param globalAddress the global address to read the data from.
     * @return the data committed at the given global address, hole filling if necessary.
     */
    @Nonnull
    ILogData read(RuntimeLayout runtimeLayout, long globalAddress);

    /**
     * Read data from all the given addresses.
     *
     * <p>This method functions exactly like a read, except
     * that it returns the result for multiple addresses.
     *
     * <p>An implementation may optimize for this type of
     * bulk request, but the default implementation
     * just performs multiple reads (possible in parallel).
     *
     * @param runtimeLayout the RuntimeLayout stamped with layout to use for the read.
     * @param addresses     a list of addresses to read from.
     * @param waitForWrite  flag whether wait for write is required or hole fill directly.
     * @param cacheOnServer whether the fetch results should be cached on log unit server.
     * @return a map of addresses to data commit at these address, hole filling if necessary.
     */
    @Nonnull
    Map<Long, ILogData> readAll(RuntimeLayout runtimeLayout,
                                Collection<Long> addresses,
                                boolean waitForWrite,
                                boolean cacheOnServer);

    /**
     * Commit the addresses by first reading and then hole filling if data not existed.
     *
     * @param runtimeLayout the RuntimeLayout stamped with layout to use for commit
     * @param addresses a list of addresses to commit
     */
    void commitAll(RuntimeLayout runtimeLayout, Collection<Long> addresses);

    /**
     * Peek data from a given address.
     *
     * <p>This function -may- return null if there was no entry
     * committed at the given global address, otherwise it
     * returns committed data at the given global address. It
     * does not attempt to hole fill if there was no entry.
     *
     * @param runtimeLayout the RuntimeLayout stamped with layout to use for the peek.
     * @param globalAddress the global address to peek from.
     * @return the data that was committed at the given global address, or NULL, if
     * there was no entry committed.
     */
    ILogData peek(RuntimeLayout runtimeLayout, long globalAddress);
}
