package org.corfudb.runtime.smr;

import org.corfudb.runtime.entries.IStreamEntry;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.view.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * Created by mwei on 6/1/15.
 */

public class MultiCommand<T> implements ISMREngineCommand<T>, IStreamEntry, Serializable {

    private static final Logger log = LoggerFactory.getLogger(MultiCommand.class);

    Map<UUID, List<ISMREngineCommand>> commandMap;
    ITimestamp ts;

    @SuppressWarnings("unchecked")
    public MultiCommand(Map<UUID, List<ISMREngineCommand>> commandMap)
    {
        this.commandMap = (Map<UUID, List<ISMREngineCommand>>) Serializer.copy(commandMap);
    }

    /**
     * Performs this operation on the given arguments.
     *
     * @param t                 the first input argument
     * @param ismrEngineOptions the second input argument
     */
    @Override
    @SuppressWarnings("unchecked")
    public void accept(T t, ISMREngine.ISMREngineOptions ismrEngineOptions)
    {
        if (commandMap.get(ismrEngineOptions.getEngineID()) != null) {
            log.info("playing back " + commandMap.get(ismrEngineOptions.getEngineID()).size() + " commands for stream " + ismrEngineOptions.getEngineID());
        }
        for (ISMREngineCommand c : commandMap.get(ismrEngineOptions.getEngineID()))
             {
                     c.accept(t, ismrEngineOptions);
             }
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
        return new ArrayList(getStreams());
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
}
