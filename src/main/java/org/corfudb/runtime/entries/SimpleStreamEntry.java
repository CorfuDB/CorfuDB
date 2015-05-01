package org.corfudb.runtime.entries;

import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.stream.SimpleTimestamp;

import java.beans.Transient;
import java.io.Serializable;
import java.util.UUID;

/**
 * Created by mwei on 4/30/15.
 */
public class SimpleStreamEntry implements IStreamEntry, Serializable{

    public UUID id;
    public Serializable payload;
    public transient SimpleTimestamp timestamp;

    public SimpleStreamEntry(UUID id, Serializable payload, SimpleTimestamp timestamp)
    {
        this.id = id;
        this.payload = payload;
        this.timestamp = timestamp;
    }

    /**
     * Gets the ID of the stream this entry belongs to.
     *
     * @return The UUID of the stream this entry belongs to.
     */
    @Override
    public UUID getStreamId() {
        return id;
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
     * Gets the payload of this stream.
     *
     * @return The payload of the stream.
     */
    @Override
    public Object getPayload() {
        return payload;
    }
}
