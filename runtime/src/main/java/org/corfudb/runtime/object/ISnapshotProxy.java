package org.corfudb.runtime.object;

import org.corfudb.protocols.logprotocol.SMREntry;

import javax.annotation.Nonnull;


/**
 * An interface for core snapshot proxy operations.
 *
 * <p> A snapshot proxy is obtained from a snapshot serving agent, and
 * is responsible for performing transactional operations on the object
 * version requested by the underlying transaction.
 *
 * @param <T> The type of the underlying SMR object.
 */
public interface ISnapshotProxy<T> {

    /**
     * Access the state of the object.
     * @param accessFunction The function to execute, which will be provided with the state of the object.
     * @param <R>            The return type of the access function.
     * @return               The return value of the access function.
     */
    <R> R access(@Nonnull ICorfuSMRAccess<R, T> accessFunction);

    /**
     * Perform an optimistic update on the underlying object.
     * @param updateEntry The SMR entry used to update the state of the object.
     */
    void logUpdate(@Nonnull SMREntry updateEntry);

    /**
     * Get the result of an upcall.
     * @param timestamp The timestamp to return the upcall for.
     * @return          The result of the upcall.
     */
    Object getUpcallResult(long timestamp);

    /**
     * Return the last known stream position accessed during this transaction.
     * @return The last known stream position.
     */
    Long getLastKnownStreamPosition();

}
