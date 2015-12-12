package org.corfudb.runtime.smr;

import org.corfudb.runtime.entries.IStreamEntry;
import org.corfudb.runtime.stream.ITimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * Created by mwei on 6/1/15.
 */

public class MultiCommand<T, R> implements ISMREngineCommand<T, R>, IStreamEntry, Serializable {

    private static final Logger log = LoggerFactory.getLogger(MultiCommand.class);

    Map<UUID, ISMREngineCommand[]> commandMap;
    ITimestamp ts;

    @SuppressWarnings("unchecked")
    public MultiCommand(Map<UUID, ISMREngineCommand[]> commandMap)
    {
        this.commandMap = commandMap;
    }

    public Set<UUID> getStreams()
    {
        return commandMap.keySet();
    }

    /**
     * Gets the list of of the streams this entry belongs to.
     *
     * @return The list of streams this entry belongs to.
     */
    @Override
    public List<UUID> getStreamIds() {
        return new ArrayList<UUID>(getStreams());
    }

    /**
     * Returns whether this entry belongs to a given stream ID.
     *
     * @param stream The stream ID to check
     * @return True, if this entry belongs to that stream, false otherwise.
     */
    @Override
    public boolean containsStream(UUID stream) {
        return getStreams().contains(stream);
    }

    /**
     * Gets the timestamp of the stream this entry belongs to.
     *
     * @return The timestamp of the stream this entry belongs to.
     */
    @Override
    public ITimestamp getTimestamp() {
        return ts;
    }

    /**
     * Set the timestamp.
     *
     * @param ts
     */
    @Override
    public void setTimestamp(ITimestamp ts) {
        this.ts = ts;
    }

    /**
     * Gets the payload of this stream.
     *
     * @return The payload of the stream.
     */
    @Override
    public Object getPayload() {
        return this;
    }

    /**
     * Applies this function to the given arguments.
     *
     * @param t                  the first function argument
     * @param tismrEngineOptions the second function argument
     * @return the function result
     */
    @Override
    @SuppressWarnings("unchecked")
    public R apply(T t, ISMREngine.ISMREngineOptions<T> ismrEngineOptions) {
        if (commandMap.get(ismrEngineOptions.getEngineID()) != null) {
            log.info("playing back " + commandMap.get(ismrEngineOptions.getEngineID()).length + " commands for stream " + ismrEngineOptions.getEngineID());
        }
        for (ISMREngineCommand c : commandMap.get(ismrEngineOptions.getEngineID()))
        {
            c.apply(t, ismrEngineOptions);
        }
        return null;
    }
}
