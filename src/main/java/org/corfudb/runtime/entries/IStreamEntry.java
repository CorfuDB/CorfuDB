package org.corfudb.runtime.entries;

import org.corfudb.runtime.stream.ITimestamp;
import java.util.UUID;

/**
 * This interface represents the minimal functions that a stream
 * entry must support.
 * Created by mwei on 4/30/15.
 */
public interface IStreamEntry extends Comparable<IStreamEntry> {

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

    /**
     * Compares the entries, using the timestamp.
     * @param entry
     * @return
     */
    default int compareTo(IStreamEntry entry) {
        return getTimestamp().compareTo(entry.getTimestamp());
    }
}
