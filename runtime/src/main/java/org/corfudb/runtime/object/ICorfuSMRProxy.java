package org.corfudb.runtime.object;

import org.corfudb.runtime.object.SnapshotGenerator.SnapshotGeneratorWithConsistency;
import org.corfudb.util.serializer.ISerializer;

import java.util.Set;
import java.util.UUID;

/**
 * An interface for accessing a proxy, which
 * manages an SMR object.
 *
 * @param <S> The type of the SMR object which must extend
 *            {@link SnapshotGenerator} and {@link ConsistencyView}
 */
public interface ICorfuSMRProxy<S extends SnapshotGeneratorWithConsistency<S>> {

    /**
     * Access the state of the object.
     *
     * @param accessMethod   The method to execute when accessing an object.
     * @param conflictObject Fine-grained conflict information, if available.
     * @param <R>            The type to return.
     * @return The result of the accessMethod
     */
    <R> R access(ICorfuSMRAccess<R, S> accessMethod, Object[] conflictObject);

    /**
     * Record an SMR function to the log before returning.
     *
     * @param smrUpdateFunction The name of the function to record.
     * @param conflictObject    Fine-grained conflict information, if available.
     * @param args              The arguments to the function.
     * @return The address in the log the SMR function was recorded at.
     */
    long logUpdate(String smrUpdateFunction,
                   Object[] conflictObject, Object... args);

    /**
     * Get the ID of the stream this proxy is subscribed to.
     *
     * @return The UUID of the stream this proxy is subscribed to.
     */
    UUID getStreamID();

    /**
     * Get the stream tags on of the object the proxy is managing.
     *
     * @return stream tags on of the object the proxy is managing
     */
    Set<UUID> getStreamTags();

    /**
     * Is object cached? The definition of cached depends on the
     * underlying implementation.
     *
     * @return true if object is cached
     */
    boolean isObjectCached();

    /**
     * Return the MultiVersionObject associated with this SMR object.
     *
     * @return the MultiVersionObject associated with this SMR object.
     */
    MultiVersionObject<S> getUnderlyingMVO();

    /**
     * Return the serializer associated with this SMR object.
     *
     * @return the serializer associated with this SMR object.
     */
    ISerializer getSerializer();
}
