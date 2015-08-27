package org.corfudb.runtime.view;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.thrift.Hint;
import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.OutOfSpaceException;
import org.corfudb.runtime.OverwriteException;
import org.corfudb.runtime.TrimmedException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;

/**
 * A stream address space is a write-once address space which is fully stream-aware.
 * Entries in a stream address space contain:
 *  1) A set of streams which the entry belongs to
 *  2) A set of hints present when the entry was retrieved
 *  3) The payload, as a byte buffer.
 *
 * Created by mwei on 8/26/15.
 */
public interface IStreamAddressSpace {

    /**
     * This class represents an entry in a stream address space.
     */
    @Data
    @Slf4j
    class StreamAddressSpaceEntry<T>
    {
        /**
         * The set of streams that this entry belongs to.
         */
        final Set<UUID> streams;

        /**
         * The set of hints present when this entry was retrieved.
         */
        Set<Hint> hints;

        /**
         * The payload for this stream entry.
         */
        final ByteBuffer payload;

        /**
         * The global index (address) for this entry.
         */
        final Long globalIndex;

        /**
         * The deserialized version of the payload.
         */
        @Getter(lazy=true)
        private final T deserializedEntry = deserializePayload();


        /**
         * Deserialize the payload. Used by the internal getter, which deserializes once and caches the
         * result.
         * @return  The deserialized payload.
         */
        @SuppressWarnings("unchecked")
        private T deserializePayload()
        {
            try {
                ByteBuffer deserializeBuffer = payload.duplicate();
                payload.clear();
                return (T) Serializer.deserialize(deserializeBuffer);
            }
            catch (Exception e)
            {
                log.error("Error deserializing payload at index " + getGlobalIndex(), e);
            }
            return null;
        }
    }

    /**
     * Write to the stream address space.
     * @param offset    The offset (global index) to write to.
     * @param streams   The streams that this entry will belong to.
     * @param payload   The payload that belongs to this entry.
     * @throws OverwriteException       If the index has been already written to.
     * @throws TrimmedException         If the index has been previously written to and is now released for garbage collection.
     * @throws OutOfSpaceException      If there is no space remaining in the current view of the address space.
     */
    void write(long offset, Set<UUID> streams, ByteBuffer payload)
        throws OverwriteException, TrimmedException, OutOfSpaceException;

    /**
     * Read from the stream address space.
     * @param offset    The offset (global index) to read from.
     * @return          A StreamAddressSpaceEntry which represents this entry, or null, if there is no entry at this space.
     * @throws TrimmedException        If the index has been previously written to and is now released for garbage collection.
     */
    StreamAddressSpaceEntry read(long offset)
        throws TrimmedException;

}
