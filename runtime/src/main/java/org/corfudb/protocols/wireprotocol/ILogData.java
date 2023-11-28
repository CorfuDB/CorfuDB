package org.corfudb.protocols.wireprotocol;

import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.runtime.CorfuRuntime;

import java.util.Optional;
import java.util.UUID;

/**
 * An interface to log data entries.
 * Log data entries represent data stored in the actual log,
 * with convenience methods for software to retrieve the
 * stored information.
 * <p>
 * Created by mwei on 8/17/16.
 */
public interface ILogData extends IMetadata, Comparable<ILogData> {

    Object getPayload(CorfuRuntime t);

    DataType getType();

    @Override
    default int compareTo(ILogData o) {
        return getGlobalAddress().compareTo(o.getGlobalAddress());
    }

    /**
     * This class provides a serialization handle, which manages
     * the lifetime of the serialized copy of this entry.
     */
    class SerializationHandle implements AutoCloseable {

        /**
         * A reference to the log data.
         */
        final ILogData data;

        /**
         * Explicitly request the serialized form of this log data
         * which only exists for the lifetime of this handle.
         *
         * @return the serialized form of this handle
         */
        public ILogData getSerialized() {
            return data;
        }

        /**
         * Create a new serialized handle with a reference
         * to the log data.
         *
         * @param data     the log data to manage
         * @param limit if a value is passed, validate size of data against the limit
         * @param metadata whether metadata needs to be serialized
         */
        public SerializationHandle(ILogData data, boolean metadata, Optional<Integer> limit) {
            this.data = data;
            data.acquireBuffer(metadata, limit);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() {
            data.releaseBuffer();
        }
    }

    /**
     * Get the serialization handle of this entry that manages
     * the lifetime of the serialized copy.
     *
     * @param metadata whether metadata needs to be serialized
     * @return a serialization handle of this entry
     */
    default SerializationHandle getSerializedForm(boolean metadata) {
        return getSerializedForm(metadata, Optional.empty());
    }

    /**
     * Get the serialization handle of this entry that manages
     * the lifetime of the serialized copy.
     *
     * @param metadata whether metadata needs to be serialized
     * @param limit if a value is passed, validate size of data against the limit
     * @return a serialization handle of this entry
     */
    default SerializationHandle getSerializedForm(boolean metadata, Optional<Integer> limit) {
        return new SerializationHandle(this, metadata, limit);
    }

    /**
     * Release the serialization buffer.
     */
    void releaseBuffer();

    /**
     * Acquire the serialization buffer.
     *
     * @param metadata whether metadata needs to be serialized
     * @param limit if a value is passed, validate size of data against the limit
     */
    void acquireBuffer(boolean metadata, Optional<Integer> limit);

    /**
     * Return the payload as a log entry.
     */
    default LogEntry getLogEntry(CorfuRuntime runtime) {
        return (LogEntry) getPayload(runtime);
    }

    /**
     * Return if there is backpointer for a particular stream.
     */
    default boolean hasBackpointer(UUID streamId) {
        return getBackpointerMap() != null
                && getBackpointerMap().containsKey(streamId);
    }

    /**
     * Return the backpointer for a particular stream.
     */
    default Long getBackpointer(UUID streamId) {
        if (!hasBackpointer(streamId)) {
            return null;
        }
        return getBackpointerMap().get(streamId);
    }

    /**
     * Get an estimate of how large this entry is in memory.
     *
     * @return An estimate on the size of this object, in bytes.
     */
    int getSizeEstimate();

    /**
     * Assign a given token to this log data.
     *
     * @param token the token to use
     */
    void useToken(IToken token);

    /**
     * Return whether the entry represents a hole or not.
     */
    default boolean isHole() {
        return getType() == DataType.HOLE;
    }

    /**
     * Return whether the entry represents an empty entry or not.
     */
    default boolean isEmpty() {
        return getType() == DataType.EMPTY;
    }

    /**
     * Return true if and only if the entry represents a trimmed address.
     */
    default boolean isTrimmed() {
        return getType() == DataType.TRIMMED;
    }

    /**
     * Return true if this LogData contains data
     */
    default boolean isData() {
        return getType() == DataType.DATA;
    }

    default void setId(UUID clientId) {
        setClientId(clientId);
        setThreadId(Thread.currentThread().getId());
    }
}
