package org.corfudb.runtime.view;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.StreamCOWEntry;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.ReplexOverwriteException;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Font-end mechanism that writes and reads objects as log entries. Each entry
 * is tagged with a stream-ID and contains back-pointers to recent
 * offsets sharing the same stream-ID.
 * Created by mwei on 12/11/15.
 */
@Slf4j
public class StreamView implements AutoCloseable {

    /**
     * The ID of the stream.
     */
    @Getter
    final UUID streamID;
    final NavigableSet<StreamContext> streamContexts;
    CorfuRuntime runtime;

    public StreamView(CorfuRuntime runtime, UUID streamID) {
        this.runtime = runtime;
        this.streamID = streamID;
        this.streamContexts = new ConcurrentSkipListSet<>();
        this.streamContexts.add(new StreamContext(streamID, Long.MAX_VALUE));
    }

    public long getLogPointer() {
        return getCurrentContext().logPointer.get();
    }

    StreamContext getCurrentContext() {
        return streamContexts.first();
    }

    /** Reset the state of this streamview, causing the next read to return
     * from the beginning of the stream.
     */
    public synchronized void reset() {
        this.streamContexts.clear();
        this.streamContexts.add(new StreamContext(streamID, Long.MAX_VALUE));
    }

    /**
     * Write an object to this stream, returning the physical address it
     * was written at.
     * <p>
     * Note: While the completion of this operation guarantees that the write
     * has been persisted, it DOES NOT guarantee that the object has been
     * written to the stream. For example, another client may have deleted
     * the stream.
     *
     * @param object The object to write to the stream.
     * @return The address this
     */
    public long write(Object object) {
        return acquireAndWrite(object, null, null);
    }

    /**
     * Write an object to this stream, returning the physical address it
     * was written at.
     * <p>
     * Note: While the completion of this operation guarantees that the write
     * has been persisted, it DOES NOT guarantee that the object has been
     * written to the stream. For example, another client may have deleted
     * the stream.
     *
     * @param object                The object to write to the stream.
     * @param acquisitionCallback   A function which will be called after the successful acquisition
     *                              of a token, but before the data is written.
     * @param deacquisitionCallback A function which will be called after an overwrite error is encountered
     *                              on a previously acquired token.
     * @return The address this object was written at.
     */
    public long acquireAndWrite(Object object, Function<TokenResponse, Boolean> acquisitionCallback,
                                Function<TokenResponse, Boolean> deacquisitionCallback) {
        TokenResponse tokenResponse = null;
        while (true) {
            tokenResponse =
                    runtime.getSequencerView().nextToken(Collections.singleton(streamID), 1);
            long token = tokenResponse.getToken();
            log.trace("Write[{}]: acquired token = {}, global addr: {}", streamID, tokenResponse, token);
            if (acquisitionCallback != null) {
                if (!acquisitionCallback.apply(tokenResponse)) {
                    log.trace("Acquisition rejected token, hole filling acquired address.");
                    try {
                        runtime.getAddressSpaceView().fillHole(token);
                    } catch (OverwriteException oe) {
                        log.trace("Hole fill completed by remote client.");
                    }
                    return -1L;
                }
            }
            try {
                runtime.getAddressSpaceView().write(token, Collections.singleton(streamID),
                        object, tokenResponse.getBackpointerMap(), tokenResponse.getStreamAddresses());
                return token;
            } catch (OverwriteException oe) {
                if (deacquisitionCallback != null && !deacquisitionCallback.apply(tokenResponse)) {
                    log.trace("Acquisition rejected overwrite at {}, not retrying.", token);
                    return -1L;
                }
                log.debug("Overwrite occurred at {}, retrying.", token);
            }
        }
    }

    /**
     * Returns the last issued token for this stream.
     *
     * @return The last issued token for this stream.
     */
    public long check() {
        return runtime.getSequencerView().nextToken(Collections.singleton(streamID), 0).getToken();
    }

    /**
     * Resolve a list of entries, using backpointers, to read.
     *
     * @param read The current address we are reading from.
     * @return A list of entries that we have resolved for reading.
     */
    public NavigableSet<Long> resolveBackpointersToRead(UUID streamID, long read) {
        long latestToken = runtime.getSequencerView().nextToken(Collections.singleton(streamID), 0).getToken();
        log.trace("Read[{}]: latest token at {}, read at: {}", streamID, latestToken, read);
        if (latestToken < read) {
            return new ConcurrentSkipListSet<>();
        }
        NavigableSet<Long> resolvedBackpointers = new ConcurrentSkipListSet<>();
        boolean hitStreamStart = false;
        boolean hitBeforeRead = false;
        if (!runtime.backpointersDisabled) {
            resolvedBackpointers.add(latestToken);
            ILogData r = runtime.getAddressSpaceView().read(latestToken);
            long backPointer = latestToken;
            while (r.getType() != DataType.EMPTY
                    && r.getBackpointerMap().containsKey(streamID)) {
                long prevRead = backPointer;
                backPointer = r.getBackpointerMap().get(streamID);
                log.trace("Read backPointer to {} at {}", backPointer, prevRead);
                if (backPointer == read) {
                    resolvedBackpointers.add(backPointer);
                    break;
                } else if (backPointer == -1L) {
                    log.trace("Hit stream start at {}, ending", prevRead);
                    hitStreamStart = true;
                    break;
                } else if (backPointer < read - 1) {
                    hitBeforeRead = true;
                    break;
                } else if (backPointer < getCurrentContext().logPointer.get()) {
                    hitBeforeRead = true;
                    break;
                } else {
                    resolvedBackpointers.add(backPointer);
                }
                if (! (backPointer < prevRead)) {
                    // throw new Exception("Backpointer error in stream {}: backPointer {} prevRead {}", streamID.toString(), backPointer, prevRead);
                    System.out.printf("************************** ERROR/TODO: Backpointer error in stream %s: backPointer %d prevRead %d\n", streamID.toString(), backPointer, prevRead);
                    return resolvedBackpointers;
                }

                // following backpointers...
                r = runtime.getAddressSpaceView().read(backPointer);
                log.trace("Following backpointer to %{}", backPointer);
            }
        } else {
            resolvedBackpointers.add(latestToken);
        }
        if (!hitStreamStart && !hitBeforeRead && resolvedBackpointers.first() != read) {
            long backpointerMin = resolvedBackpointers.first();
            log.trace("Backpointer min is at {} but read is at {}, filling.", backpointerMin, read);
            while (backpointerMin > read && backpointerMin > 0) {
                backpointerMin--;
                resolvedBackpointers.add(backpointerMin);
            }
        }
        log.trace("Backpointer resolved to {}.", resolvedBackpointers);
        return resolvedBackpointers;
    }

    private Layout.ReplicationMode getReplicationMode() {
        return runtime.getLayoutView().getLayout().getSegments().get(
                runtime.getLayoutView().getLayout().getSegments().size() - 1)
                .getReplicationMode();
    }
    /**
     * Read the next item from the stream.
     * This method is synchronized to protect against multiple readers.
     *
     * @return The next item from the stream.
     */
    @SuppressWarnings("unchecked")
    public synchronized ILogData read() {
        return getReplicationMode().getStreamViewDelegate().read(this, Long.MAX_VALUE);
    }

    public synchronized ILogData read(long maxGlobal) {
        return getReplicationMode().getStreamViewDelegate().read(this, maxGlobal);
    }

    public synchronized ILogData[] readTo(long pos) {
        return getReplicationMode().getStreamViewDelegate().readTo(this, pos);
    }

    /**
     * Closes this resource, relinquishing any underlying resources.
     * This method is invoked automatically on object managed by the
     * {@code try}-with-resources statement.
     *
     * @throws Exception if this resource cannot be closed
     */
    @Override
    public void close() throws Exception {

    }

    @ToString
    class StreamContext implements Comparable<StreamContext> {

        /**
         * The id (stream ID) of this context.
         */
        final UUID contextID;

        /**
         * The maximum address that we should follow to, or
         * Long.MAX_VALUE, if this is the final context.
         */
        final long maxAddress;
        /**
         * A pointer to the log. In Chain-Replication, this is a GLOBAL address. In Replex-Corfu, this is a STREAM addr.
         */
        final AtomicLong logPointer;
        /**
         * A skipList of resolved stream addresses.
         */
        NavigableSet<Long> currentBackpointerList
                = new ConcurrentSkipListSet<>();

        StreamContext(UUID contextID, long maxAddress) {
            this.contextID = contextID;
            this.maxAddress = maxAddress;
            this.logPointer = new AtomicLong(0);
        }

        @Override
        public int compareTo(StreamContext o) {
            return Long.compare(this.maxAddress, o.maxAddress);
        }
    }
}
