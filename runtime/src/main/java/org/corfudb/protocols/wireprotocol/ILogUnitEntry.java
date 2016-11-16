package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.runtime.CorfuRuntime;

import java.util.EnumMap;
import java.util.UUID;

/**
 * Created by mwei on 2/1/16.
 */
public interface ILogUnitEntry extends IMetadata {

    /**
     * Gets the type of result this entry represents.
     *
     * @return The type of result this entry represents.
     */
    DataType getResultType();

    /**
     * Gets the metadata map.
     *
     * @return A map containing the metadata for this entry.
     */
    EnumMap<LogUnitMetadataType, Object> getMetadataMap();

    /**
     * Gets a ByteBuf representing the payload for this data.
     *
     * @return A ByteBuf representing the payload for this data.
     */
    ByteBuf getBuffer();

    /**
     * Gets the deserialized payload.
     *
     * @return An object representing the deserialized payload.
     */
    Object getPayload();

    /**
     * Gets the address for this entry
     *
     * @return The global log address for this entry, or null if it is not known.
     */
    Long getAddress();

    /**
     * Set the runtime used for deserialization.
     */
    ILogUnitEntry setRuntime(CorfuRuntime runtime);

    /**
     * Return whether or not this entry is a log entry.
     */
    default boolean isLogEntry() {
        return getPayload() instanceof LogEntry;
    }

    /**
     * Return the payload as a log entry.
     */
    default LogEntry getLogEntry() {
        return (LogEntry) getPayload();
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
}
