package org.corfudb.protocols.wireprotocol;

import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.runtime.CorfuRuntime;

import java.util.EnumMap;
import java.util.UUID;

/**
 * Created by mwei on 8/17/16.
 */
public interface ILogData extends IMetadata {

    Object getPayload(CorfuRuntime t);
    DataType getType();

    /**
     * Return whether or not this entry is a log entry.
     */
    default boolean isLogEntry(CorfuRuntime runtime) {
        return getPayload(runtime) instanceof LogEntry;
    }

    /**
     * Return the payload as a log entry.
     */
    default LogEntry getLogEntry(CorfuRuntime runtime) {
        return (LogEntry) getPayload(runtime);
    }

    /**
     * Return if there is backpointer for a particular stream.
     */
    default boolean hasBackpointer(UUID streamID) {
        return getBackpointerMap() != null
                && getBackpointerMap().containsKey(streamID);
    }

    /**
     * Return the backpointer for a particular stream.
     */
    default Long getBackpointer(UUID streamID) {
        return getBackpointerMap().get(streamID);
    }

    /**
     * Return if this is the first entry in a particular stream.
     */
    default boolean isFirstEntry(UUID streamID) {
        return getBackpointer(streamID) == -1L;
    }

    /**
     * Get an estimate of how large this entry is in memory.
     *
     * @return An estimate on the size of this object, in bytes.
     */
    default int getSizeEstimate() {
        return 1; // The default is that we don't know, so we return 1.
    }

    default boolean isHole() {
        return getType() == DataType.HOLE;
    }
}
