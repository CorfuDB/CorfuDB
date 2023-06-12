package org.corfudb.runtime.object;

import org.corfudb.util.serializer.ISerializer;

import java.util.Set;
import java.util.UUID;

/**
 * An interface for accessing auxiliary data associated with
 * proxy, which manages an SMR object.
 */
public interface ICorfuSMRProxyMetadata {

    boolean isObjectCached();

    MultiVersionObject<?> getUnderlyingMVO();

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

    ISerializer getSerializer();
}
