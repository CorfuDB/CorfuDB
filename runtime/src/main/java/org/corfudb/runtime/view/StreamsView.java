package org.corfudb.runtime.view;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.AppendException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.StaleTokenException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.Utils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Created by mwei on 12/11/15.
 */
@Slf4j
public class StreamsView extends AbstractView {

    /**
     * Checkpoint of streams have their own stream id derived from the
     * stream id. We add the checkpoint suffix to the original stream id.
     */
    public static final String CHECKPOINT_SUFFIX = "_cp";

    /**
     * This list should have references to all opened streams. The ViewsGarbageCollector
     * will use this list to clear trimmed addresses from streams. Since new streams
     * can be opened during a runtime garbage collection cycle, the collection has
     * to be thread-safe.
     */
    private List<IStreamView> openedStreams = new CopyOnWriteArrayList<>();

    public StreamsView(final CorfuRuntime runtime) {
        super(runtime);
    }

    /**
     * Creates and returns a new StreamView on a stream. 
     *
     * @param stream The UUID of the stream to get a view on.
     * @return A view
     */
    public IStreamView get(UUID stream) {
        return this.get(stream, StreamOptions.DEFAULT);
    }

    /**
     * Since streams can also be used by higher level abstractions, some consumers implement
     * synchronization at a higher level, so the stream implementation doesn't have to
     * be thread-safe. Also, gc will not be called on unsafe streams therefore gc needs to
     * be managed by the consumer.
     *
     * @param stream stream id
     * @return an unsafe (not thread safe) stream implementation
     */
    public IStreamView getUnsafe(UUID stream) {
        return this.getUnsafe(stream, StreamOptions.DEFAULT);
    }

    /**
     * Create and return a new stream. This stream will automatically
     * garbage collected by ViewsGarbageCollector.
     *
     * @param streamId The UUID of the stream to get a view on.
     * @return A view
     */
    public IStreamView get(UUID streamId, StreamOptions options) {
        IStreamView stream = runtime.getLayoutView().getLayout().getLastSegment()
                .getReplicationMode()
                .getStreamView(runtime, streamId, options);
        openedStreams.add(stream);
        return stream;
    }

    /**
     * Create and return a stream that won't be automatically garbage collected.
     * @param stream stream id
     * @param options open options
     */
    public IStreamView getUnsafe(UUID stream, StreamOptions options) {
        return runtime.getLayoutView().getLayout().getLastSegment()
                .getReplicationMode().getUnsafeStreamView(runtime, stream, options);
    }

    /**
     * Remove all opened streams.
     */
    public void clear() {
        openedStreams.clear();
    }

    /**
     * Remove a specific stream from openedStreams
     */
    public void removeStream(IStreamView stream) {
        openedStreams.remove(stream);
    }

    /**
     * Run garbage collection on all opened streams. Note that opened
     * unsafe streams will be excluded (because its unsafe for the garbage
     * collector thread to operate on them while being used by a different
     * thread).
     */
    public void gc(long trimMark) {
        for (IStreamView streamView : openedStreams) {
            streamView.gc(trimMark);
        }
    }

    /**
     * Append to multiple streams simultaneously, possibly providing
     * information on how to resolve conflicts.
     *
     * @param streamIDs    The streams to append to.
     * @param object       The object to append to each stream.
     * @param conflictInfo Conflict information for the sequencer to check.
     * @param cacheOption  The caching mode for write/append
     * @return The address the entry was written to.
     * @throws TransactionAbortedException If the transaction was aborted by
     *                                     the sequencer.
     */
    public long append(@Nonnull Object object, @Nullable TxResolutionInfo conflictInfo,
                       @Nonnull CacheOption cacheOption, @Nonnull UUID... streamIDs) {

        final boolean serializeMetadata = false;
        final LogData ld = new LogData(DataType.DATA, object, runtime.getParameters().getCodecType());
        TokenResponse tokenResponse = null;

        // Opening serialization handle before acquiring token, this way we prevent the
        // readers to wait for the possibly long serialization time in writer.
        // The serialization here only serializes the payload because the token is not
        // acquired yet, thus metadata is incomplete. Once a token is acquired, the
        // writer will append the serialized metadata to the buffer.
        try (ILogData.SerializationHandle sh = ld.getSerializedForm(serializeMetadata)) {
            int payloadSize = ld.checkMaxWriteSize(runtime.getParameters().getMaxWriteSize());

            for (int retry = 0; retry < runtime.getParameters().getWriteRetry(); retry++) {
                // Go to the sequencer, grab a token to write.
                tokenResponse = conflictInfo == null
                        ? runtime.getSequencerView().next(streamIDs) // Token w/o conflict info
                        : runtime.getSequencerView().next(conflictInfo, streamIDs); // Token w/ conflict info

                // Is our token a valid type?
                AbortCause abortCause = getAbortCauseFromToken(tokenResponse);

                if (abortCause != null) {
                    throw new TransactionAbortedException(
                            conflictInfo,
                            tokenResponse.getConflictKey(), tokenResponse.getConflictStream(),
                            tokenResponse.getToken().getSequence(), abortCause,
                            TransactionalContext.getCurrentContext());
                }

                try {
                    // Run pre-commit listeners if we are in transaction.
                    runPreCommitListeners(tokenResponse, ld, serializeMetadata);
                    // Attempt to write to the log.
                    runtime.getAddressSpaceView().write(tokenResponse, ld, cacheOption);
                    // If we're here, we succeeded, return the acquired token.
                    return tokenResponse.getSequence();
                } catch (OverwriteException oe) {
                    // We were overwritten, get a new token and try again.
                    log.warn("append[{}]: Overwritten after {} retries, streams {}",
                            tokenResponse.getSequence(), retry,
                            Arrays.stream(streamIDs).map(Utils::toReadableId).collect(Collectors.toSet()));

                    if (conflictInfo != null) {
                        // On retry, check for conflicts only from the previous attempt position,
                        // otherwise the transaction will always conflict with itself.
                        conflictInfo.setSnapshotTimestamp(tokenResponse.getToken());
                    }

                } catch (StaleTokenException se) {
                    // the epoch changed from when we grabbed the token from sequencer
                    log.warn("append[{}]: StaleToken, streams {}", tokenResponse.getSequence(),
                            Arrays.stream(streamIDs).map(Utils::toReadableId).collect(Collectors.toSet()));

                    throw new TransactionAbortedException(
                            conflictInfo,
                            tokenResponse.getConflictKey(), tokenResponse.getConflictStream(),
                            tokenResponse.getToken().getSequence(),
                            AbortCause.NEW_SEQUENCER, // in the future perhaps define a new AbortCause?
                            TransactionalContext.getCurrentContext());
                }
            }

            log.error("append[{}]: failed after {} retries, streams {}, write size {} bytes",
                    tokenResponse == null ? -1 : tokenResponse.getSequence(),
                    runtime.getParameters().getWriteRetry(),
                    Arrays.stream(streamIDs).map(Utils::toReadableId).collect(Collectors.toSet()),
                    payloadSize);
        }

        throw new AppendException();
    }

    /**
     * Append to multiple streams and caches the result.
     *
     * @see StreamsView#append(Object, TxResolutionInfo, CacheOption, UUID...)
     */
    public long append(@Nonnull Object object, @Nullable TxResolutionInfo conflictInfo,
                       @Nonnull UUID... streamIDs) {
        return append(object, conflictInfo, CacheOption.WRITE_THROUGH, streamIDs);
    }

    private AbortCause getAbortCauseFromToken(TokenResponse tokenResponse) {
        AbortCause abortCause = null;

        switch (tokenResponse.getRespType()) {
            case TX_ABORT_CONFLICT:
                abortCause = AbortCause.CONFLICT;
                break;
            case TX_ABORT_NEWSEQ:
                abortCause = AbortCause.NEW_SEQUENCER;
                break;
            case TX_ABORT_SEQ_OVERFLOW:
                abortCause = AbortCause.SEQUENCER_OVERFLOW;
                break;
            case TX_ABORT_SEQ_TRIM:
                abortCause = AbortCause.SEQUENCER_TRIM;
                break;
        }

        return abortCause;
    }

    private void runPreCommitListeners(TokenResponse tokenResponse,
                                       LogData ld, boolean serializeMetadata) {
        if (TransactionalContext.isInTransaction()) {
            // If this transaction has entries that wish to capture the committed address
            // invoke its preCommitCallbacks with the tokenResponse from the sequencer.
            // Note that we might invoke the same method multiple times on retries,
            // which means the preCommitCallback must be idempotent.
            log.debug("append: Invoking {} preCommitListeners",
                    TransactionalContext.getRootContext().getPreCommitListeners().size());
            List<TransactionalContext.PreCommitListener> listeners =
                    TransactionalContext.getRootContext().getPreCommitListeners();
            // If there are pre-commit listeners, the payload will be changed,
            // so we need to update the acquired serialized buffer.
            if (!listeners.isEmpty()) {
                listeners.forEach(e -> e.preCommitCallback(tokenResponse));
                ld.updateAcquiredBuffer(serializeMetadata);
            }
        }
    }

    @VisibleForTesting
    List<IStreamView> getOpenedStreams() {
        return openedStreams;
    }
}
