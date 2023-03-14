package org.corfudb.runtime.object;

import java.util.Set;
import java.util.UUID;

/**
 * An interface for accessing a proxy, which
 * manages an SMR object.
 *
 * @param <T> The type of the SMR object.
 *            Created by mwei on 11/10/16.
 */
public interface ICorfuSMRProxy<T> {

    /**
     * Access the state of the object.
     *
     * @param accessMethod   The method to execute when accessing an object.
     * @param conflictObject Fine-grained conflict information, if available.
     * @param <R>            The type to return.
     * @return The result of the accessMethod
     */
    <R> R access(ICorfuSMRAccess<R, T> accessMethod, Object[] conflictObject);

    /**
     * Record an SMR function to the log before returning.
     *
     * @param smrUpdateFunction The name of the function to record.
     * @param keepUpcallResult  Whether or not we need to keep the
     *                          result to the upcall, for a subsequent
     *                          call to getUpcallResult.
     * @param conflictObject    Fine-grained conflict information, if
     *                          available.
     * @param args              The arguments to the function.
     * @return The address in the log the SMR function was recorded at.
     */
    long logUpdate(String smrUpdateFunction, boolean keepUpcallResult,
                   Object[] conflictObject, Object... args);

    /**
     * Get the ID of the stream this proxy is subscribed to.
     *
     * @return The UUID of the stream this proxy is subscribed to.
     */
    UUID getStreamID();

    /**
     * Get the latest version read by the proxy.
     *
     * @return The latest version read by the proxy.
     */
    long getVersion();

    /**
     * Get the stream tags on of the object the proxy is managing.
     *
     * @return stream tags on of the object the proxy is managing
     */
    Set<UUID> getStreamTags();
}
