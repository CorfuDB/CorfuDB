package org.corfudb.runtime.entries;

import org.corfudb.runtime.stream.ITimestamp;

import java.io.Serializable;
import java.util.UUID;

/**
 * This interface represents the minimal functions that a stream
 * entry must support.
 * Created by mwei on 4/30/15.
 */
public interface IStreamEntry {

    /**
     * Gets the ID of the stream this entry belongs to.
     * @return  The UUID of the stream this entry belongs to.
     */
    UUID getStreamId();

    /**
     * Gets the timestamp of the stream this entry belongs to.
     * @return The timestamp of the stream this entry belongs to.
     */
    ITimestamp getTimestamp();

    /**
     * Gets the payload of this stream.
     * @return The payload of the stream.
     */
    Object getPayload();
}
