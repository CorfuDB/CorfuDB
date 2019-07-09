package org.corfudb.runtime.view;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TokenType;
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

    @Getter
    Multimap<UUID, IStreamView> streamCache = Multimaps.synchronizedMultimap(HashMultimap.create());

    public StreamsView(final CorfuRuntime runtime) {
        super(runtime);
    }

    /**
     * Get a view on a stream. The view has its own pointer to the stream.
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
     * Get a view on a stream. The view has its own pointer to the stream.
     *
     * @param stream The UUID of the stream to get a view on.
     * @return A view
     */
    public IStreamView get(UUID stream, StreamOptions options) {
        IStreamView streamView = runtime.getLayoutView().getLayout().getLatestSegment()
                .getReplicationMode().getStreamView(runtime, stream, options);
        streamCache.put(stream, streamView);
        return streamView;
    }

    public IStreamView getUnsafe(UUID stream, StreamOptions options) {
        return runtime.getLayoutView().getLayout().getLatestSegment()
                .getReplicationMode().getUnsafeStreamView(runtime, stream, options);
    }

    /**
     * Run garbage collection on all opened streams. Note that opened
     * unsafe streams will be excluded (because its unsafe for the garbage
     * collector thread to operate on them while being used by a different
     * thread).
     *
     */
    public void gc(long trimMark) {
        for (IStreamView streamView : getStreamCache().values()) {
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
                       @Nonnull CacheOption cacheOption, @Nonnull UUID ... streamIDs) {

        final LogData ld = new LogData(DataType.DATA, object);
        ld.checkMaxWriteSize(runtime.getParameters().getMaxWriteSize());

        // Go to the sequencer, grab an initial token.
        TokenResponse tokenResponse = conflictInfo == null
                ? runtime.getSequencerView().next(streamIDs) // Token w/o conflict info
                : runtime.getSequencerView().next(conflictInfo, streamIDs); // Token w/ conflict info

        for (int x = 0; x < runtime.getParameters().getWriteRetry(); x++) {

            // Is our token a valid type?
            if (tokenResponse.getRespType() == TokenType.TX_ABORT_CONFLICT) {
                throw new TransactionAbortedException(
                        conflictInfo,
                        tokenResponse.getConflictKey(), tokenResponse.getConflictStream(),
                        tokenResponse.getToken().getSequence(), AbortCause.CONFLICT,
                        TransactionalContext.getCurrentContext());
            } else if (tokenResponse.getRespType() == TokenType.TX_ABORT_NEWSEQ) {
                throw new TransactionAbortedException(
                        conflictInfo,
                        tokenResponse.getConflictKey(), tokenResponse.getConflictStream(),
                        tokenResponse.getToken().getSequence(), AbortCause.NEW_SEQUENCER,
                        TransactionalContext.getCurrentContext());
            } else if (tokenResponse.getRespType() == TokenType.TX_ABORT_SEQ_OVERFLOW) {
                throw new TransactionAbortedException(
                        conflictInfo,
                        tokenResponse.getConflictKey(), tokenResponse.getConflictStream(),
                        tokenResponse.getToken().getSequence(), AbortCause.SEQUENCER_OVERFLOW,
                        TransactionalContext.getCurrentContext());
            } else if (tokenResponse.getRespType() == TokenType.TX_ABORT_SEQ_TRIM) {
                throw new TransactionAbortedException(
                        conflictInfo,
                        tokenResponse.getConflictKey(), tokenResponse.getConflictStream(),
                        tokenResponse.getToken().getSequence(), AbortCause.SEQUENCER_TRIM,
                        TransactionalContext.getCurrentContext());
            }

            // Attempt to write to the log
            try {
                runtime.getAddressSpaceView().write(tokenResponse, ld, cacheOption);
                // If we're here, we succeeded, return the acquired token
                return tokenResponse.getSequence();
            } catch (OverwriteException oe) {

                // We were overwritten, get a new token and try again.
                log.warn("append[{}]: Overwritten after {} retries, streams {}",
                        tokenResponse.getSequence(),
                        x,
                        Arrays.stream(streamIDs).map(Utils::toReadableId).collect(Collectors.toSet()));

                TokenResponse temp;
                if (conflictInfo == null) {
                    // Token w/o conflict info
                    temp = runtime.getSequencerView().next(streamIDs);
                } else {

                    // On retry, check for conflicts only from the previous
                    // attempt position
                    conflictInfo.setSnapshotTimestamp(tokenResponse.getToken());

                    // Token w/ conflict info
                    temp = runtime.getSequencerView().next(conflictInfo, streamIDs);
                }

                // We need to fix the token (to use the stream addresses- may
                // eventually be deprecated since these are no longer used)
                tokenResponse = new TokenResponse(
                        temp.getRespType(), tokenResponse.getConflictKey(),
                        tokenResponse.getConflictStream(), temp.getToken(),
                        temp.getBackpointerMap(), Collections.emptyList());

            } catch (StaleTokenException se) {
                // the epoch changed from when we grabbed the token from sequencer
                log.warn("append[{}]: StaleToken , streams {}", tokenResponse.getSequence(),
                        Arrays.stream(streamIDs).map(Utils::toReadableId).collect(Collectors.toSet()));

                throw new TransactionAbortedException(
                        conflictInfo,
                        tokenResponse.getConflictKey(), tokenResponse.getConflictStream(),
                        tokenResponse.getToken().getSequence(), AbortCause.NEW_SEQUENCER, // in the future,
                        // perhaps
                        // define a new AbortCause?
                        TransactionalContext.getCurrentContext());
            }
        }

        log.error("append[{}]: failed after {} retries , streams {}, write size {} bytes",
                tokenResponse.getSequence(),
                runtime.getParameters().getWriteRetry(),
                Arrays.stream(streamIDs).map(Utils::toReadableId).collect(Collectors.toSet()),
                ILogData.getSerializedSize(object));
        throw new AppendException();
    }

    /**
     * Append to multiple streams and caches the result.
     *
     * @see StreamsView#append(Object, TxResolutionInfo, CacheOption, UUID...)
     */
    public long append(@Nonnull Object object, @Nullable TxResolutionInfo conflictInfo,
                       @Nonnull UUID ... streamIDs) {
       return append(object, conflictInfo, CacheOption.WRITE_THROUGH, streamIDs);
    }
}
