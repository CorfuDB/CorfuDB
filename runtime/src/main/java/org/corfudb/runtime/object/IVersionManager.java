package org.corfudb.runtime.object;

import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * An interface definition for a versioned SMR object. Classes implementing this
 * interface must provide the necessary synchronization primitives for allowing safe
 * access to the SMR object in a multi-threaded context.
 * @param <T> The type of the underlying SMR object.
 */
public interface IVersionManager<T extends ICorfuSMR<T>> {

    /**
     * Execute a method on the versioned object without synchronization. For
     * example, this can be useful for executing pure methods.
     * @param method The method to execute.
     * @param <R>    The return type of the provided method.
     * @return       The return value of the provided method.
     */
    <R> R passThrough(@Nonnull Function<T, R> method);

    /**
     * Perform a non-transactional access on state of the object.
     * @param rt           The runtime used to query the Sequencer and containing relevant parameters.
     * @param accessMethod The function to execute, which will be provided with the state of the object.
     * @param timestamp    The version of the object that the access will be performed on.
     * @param <R>          The return type of the access function.
     * @return             The return value of the access function.
     */
    <R> R access(@Nonnull CorfuRuntime rt,
                 @Nonnull ICorfuSMRAccess<R, T> accessMethod,
                 @Nonnull AtomicLong timestamp);

    /**
     * Perform a non-transactional update on this object, noting a request to save the upcall result if necessary.
     * @param updateEntry      The SMR entry to apply.
     * @param keepUpcallResult True if and only if the upcall result should be saved.
     * @return                 The address the update was logged at.
     */
    long logUpdate(@Nonnull SMREntry updateEntry, boolean keepUpcallResult);

    /**
     * Return the result of an upcall at the given timestamp.
     * @param rt        The runtime containing relevant parameters.
     * @param timestamp The timestamp for which the upcall is requested.
     * @param <R>       The type of the upcall returned.
     * @return          The result of the upcall.
     */
    <R> R getUpcallResult(@Nonnull CorfuRuntime rt, long timestamp);

    /**
     * Perform gc on this object as a function of the trim mark.
     * @param trimMark The trim mark used to perform gc.
     */
    void gc(long trimMark);

    /**
     * Obtain a snapshot proxy that can perform transactional operations on the
     * state of the underlying object at the provided timestamp.
     * @param transactionalContext The transactional context of the transaction requesting the snapshot proxy.
     * @param snapshotTimestamp    The version of the object that operations will be performed on.
     * @return                     The corresponding snapshot proxy.
     */
    ISnapshotProxy<T> getSnapshotProxy(@Nonnull AbstractTransactionalContext transactionalContext,
                                       @Nonnull Token snapshotTimestamp);

}
