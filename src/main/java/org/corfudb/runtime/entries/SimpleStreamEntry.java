package org.corfudb.runtime.entries;

import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.stream.SimpleTimestamp;

import java.beans.Transient;
import java.io.Serializable;
import java.util.*;

/**
 * Created by mwei on 4/30/15.
 */
public class SimpleStreamEntry implements IStreamEntry, Serializable{

    public List<UUID> id;
    public Serializable payload;
    public transient ITimestamp timestamp;

    public SimpleStreamEntry(UUID id, Serializable payload, SimpleTimestamp timestamp)
    {
       this(Collections.singletonList(id), payload, timestamp);
    }

    public SimpleStreamEntry(List<UUID> id, Serializable payload, SimpleTimestamp timestamp)
    {
        this.id = id;
        this.payload = payload;
        this.timestamp = timestamp;
    }

    /**
     * Gets the list of of the streams this entry belongs to.
     *
     * @return The list of streams this entry belongs to.
     */
    @Override
    public List<UUID> getStreamIds() {
        return id;
    }

    /**
     * Returns whether this entry belongs to a given stream ID.
     *
     * @param stream The stream ID to check
     * @return True, if this entry belongs to that stream, false otherwise.
     */
    @Override
    public boolean containsStream(UUID stream) {
        return id.contains(stream);
    }

    /**
     * Gets the timestamp of the stream this entry belongs to.
     *
     * @return The timestamp of the stream this entry belongs to.
     */
    @Override
    public ITimestamp getTimestamp() {
        return timestamp;
    }

    /**
     * Set the timestamp.
     *
     * @param ts
     */
    @Override
    public void setTimestamp(ITimestamp ts) {
        this.timestamp = ts;
    }

    /**
     * Gets the payload of this stream.
     *
     * @return The payload of the stream.
     */
    @Override
    public Object getPayload() {
        return payload;
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof SimpleStreamEntry) && getTimestamp().equals(((SimpleStreamEntry) o).getTimestamp());
    }

    @Override
    public int hashCode()
    {
        return getTimestamp().hashCode();
    }
}
