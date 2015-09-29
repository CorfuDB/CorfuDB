package org.corfudb.runtime.entries;

import org.corfudb.runtime.stream.ITimestamp;

import java.util.List;
import java.util.UUID;

/**
 * This interface represents the minimal functions that a stream
 * entry must support.
 * Created by mwei on 4/30/15.
 */
public interface IStreamEntry extends Comparable<IStreamEntry> {

    /**
     * Gets the list of of the streams this entry belongs to.
     * @return  The list of streams this entry belongs to.
     */
    List<UUID> getStreamIds();

    /**
     * Returns whether this entry belongs to a given stream ID.
     * @param stream    The stream ID to check
     * @return          True, if this entry belongs to that stream, false otherwise.
     */
    boolean containsStream(UUID stream);

    /**
     * Gets the timestamp of the stream this entry belongs to.
     * @return The timestamp of the stream this entry belongs to.
     */
    ITimestamp getTimestamp();

    /**
     * Set the timestamp.
     * @param   newTs   The timestamp of this entry.
     */
    void setTimestamp(ITimestamp ts);

    /**
     * Gets the logical timestamp of the stream this entry belongs to.
     * @return The logical timestamp of the stream this entry belongs to.
     */
    default ITimestamp getLogicalTimestamp() {
        throw new UnsupportedOperationException("Unsupported!");
    }

    /**
     * Gets the logical timestamp of the stream this entry belongs to.
     * @return The logical timestamp of the stream this entry belongs to.
     */
    default void setLogicalTimestamp(ITimestamp ts) {
        throw new UnsupportedOperationException("Unsupported!");
    }


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
