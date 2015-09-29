package org.corfudb.runtime.view;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.thrift.Hint;
import org.corfudb.runtime.NetworkException;
import org.corfudb.runtime.OutOfSpaceException;
import org.corfudb.runtime.OverwriteException;
import org.corfudb.runtime.TrimmedException;
import org.corfudb.runtime.entries.IStreamEntry;
import org.corfudb.runtime.protocols.logunits.INewWriteOnceLogUnit;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.stream.SimpleTimestamp;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;

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

    enum StreamAddressEntryCode {
        DATA,
        HOLE,
        EMPTY,
        TRIMMED,
        EMPTY_BATCH
    };

    enum StreamAddressWriteResult {
        OK,
        OVERWRITE,
        TRIMMED,
        OOS
    };

    /**
     * This class represents an entry in a stream address space.
     */
    @Data
    @AllArgsConstructor
    @RequiredArgsConstructor
    class StreamAddressSpaceEntry<T> implements IStreamEntry
    {

        /**
         * This constructor is for generating classes which contain a code only (i.e, trimmed)
         * @param codeOnly
         */
        public StreamAddressSpaceEntry(Long globalIndex, StreamAddressEntryCode codeOnly)
        {
            this.streams = Collections.emptySet();
            this.globalIndex = globalIndex;
            this.code = codeOnly;
            this.payload = null;
        }

        /**
         * The set of streams that this entry belongs to.
         */
        @NonNull
        final Set<UUID> streams;

        /**
         * The set of hints present when this entry was retrieved.
         */
        Set<Hint> hints;

        /**
         * The global index (address) for this entry.
         */
        final Long globalIndex;

        /**
         * The code for the entry.
         */
        final StreamAddressEntryCode code;

        /**
         * The logical timestamp, to be set by the streaming implementation.
         */
        ITimestamp logicalTimestamp = ITimestamp.getInvalidTimestamp();

        /**
         * The deserialized version of the payload.
         */
        private final T payload;

        /**
         * Gets the list of of the streams this entry belongs to.
         *
         * @return The list of streams this entry belongs to.
         */
        @Override
        public List<UUID> getStreamIds() {
            return new ArrayList<UUID>(streams);
        }

        /**
         * Returns whether this entry belongs to a given stream ID.
         *
         * @param stream The stream ID to check
         * @return True, if this entry belongs to that stream, false otherwise.
         */
        @Override
        public boolean containsStream(UUID stream) {
            return streams.size() == 0 || streams.contains(stream); //an entry with no streams belongs in all streams.
        }

        /**
         * Gets the timestamp of the stream this entry belongs to.
         *
         * @return The timestamp of the stream this entry belongs to.
         */
        @Override
        public ITimestamp getTimestamp() {
            return new SimpleTimestamp(globalIndex);
        }

        /**
         * Set the timestamp.
         *
         * @param ts
         */
        @Override
        public void setTimestamp(ITimestamp ts) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Asynchronously write to the stream address space.
     * @param offset    The offset (global index) to write to.
     * @param streams   The streams that this entry will belong to.
     * @param payload   The unserialized payload that belongs to this entry.
     */
    CompletableFuture<StreamAddressWriteResult> writeAsync(long offset, Set<UUID> streams, Object payload);

    /**
     * Asynchronously read from the stream address space.
     * @param offset    The offset (global index) to read from.
     * @return          A StreamAddressSpaceEntry containing the data that was read.
     */
    CompletableFuture<StreamAddressSpaceEntry> readAsync(long offset);


    /**
     * Write to the stream address space.
     * @param offset    The offset (global index) to write to.
     * @param streams   The streams that this entry will belong to.
     * @param payload   The preserialized payload that belongs to this entry.
     */
    @SneakyThrows
    default StreamAddressWriteResult write(long offset, Set<UUID> streams, Object payload)
    {
        return writeAsync(offset, streams, payload).get();
    }

    /**
     * Fill an address in the address space with a hole entry. This method is unreliable (not guaranteed to send a request
     * to any log unit) and asynchronous.
     * @param offset    The offset (global index) to fill.
     */
    void fillHole(long offset);

    /**
     * Read from the stream address space.
     * @param offset    The offset (global index) to read from.
     * @return          A StreamAddressSpaceEntry which represents this entry, or null, if there is no entry at this space.
     * @throws TrimmedException        If the index has been previously written to and is now released for garbage collection.
     */
    @SneakyThrows
    default StreamAddressSpaceEntry read(long offset)
    {
        return readAsync(offset).get();
    }

    /**
     * Trim a prefix of a stream.
     * @param stream    The ID of the stream to be trimmed.
     * @param prefix    The prefix to be trimmed, inclusive.
     */
    void trim(UUID stream, long prefix);
}
