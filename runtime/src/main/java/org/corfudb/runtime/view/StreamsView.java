package org.corfudb.runtime.view;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.ILogData;
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
     * Get a view on a stream. The view has its own pointer to the stream.
     *
     * @param stream The UUID of the stream to get a view on.
     * @return A view
     */
    public IStreamView get(UUID stream, StreamOptions options) {
        return runtime.getLayoutView().getLayout().getSegments().get(
                runtime.getLayoutView().getLayout().getSegments().size() - 1)
                .getReplicationMode().getStreamView(runtime, stream, options);
    }

    /**
     * Append to multiple streams simultaneously, possibly providing
     * information on how to resolve conflicts.
     *
     * @param streamIDs    The streams to append to.
     * @param object       The object to append to each stream.
     * @param conflictInfo Conflict information for the sequencer to check.
     * @return The address the entry was written to.
     * @throws TransactionAbortedException If the transaction was aborted by
     *                                     the sequencer.
     */
    public long append(@Nonnull Set<UUID> streamIDs, @Nonnull Object object,
                       @Nullable TxResolutionInfo conflictInfo) throws TransactionAbortedException {

        // Go to the sequencer, grab an initial token.
        TokenResponse tokenResponse = conflictInfo == null
                ? runtime.getSequencerView().nextToken(streamIDs, 1) // Token w/o conflict info
                : runtime.getSequencerView().nextToken(streamIDs, 1,
                conflictInfo); // Token w/ conflict info

        for (int x = 0; x < runtime.getParameters().getWriteRetry(); x++) {

            // Is our token a valid type?
            if (tokenResponse.getRespType() == TokenType.TX_ABORT_CONFLICT) {
                throw new TransactionAbortedException(
                        conflictInfo,
                        tokenResponse.getConflictKey(),
                        AbortCause.CONFLICT,
                        TransactionalContext.getCurrentContext());
            } else if (tokenResponse.getRespType() == TokenType.TX_ABORT_NEWSEQ) {
                throw new TransactionAbortedException(
                        conflictInfo,
                        tokenResponse.getConflictKey(),
                        AbortCause.NEW_SEQUENCER,
                        TransactionalContext.getCurrentContext());
            } else if (tokenResponse.getRespType() == TokenType.TX_ABORT_SEQ_OVERFLOW) {
                throw new TransactionAbortedException(
                        conflictInfo,
                        tokenResponse.getConflictKey(),
                        AbortCause.SEQUENCER_OVERFLOW,
                        TransactionalContext.getCurrentContext());
            } else if (tokenResponse.getRespType() == TokenType.TX_ABORT_SEQ_TRIM) {
                throw new TransactionAbortedException(
                        conflictInfo,
                        tokenResponse.getConflictKey(),
                        AbortCause.SEQUENCER_TRIM,
                        TransactionalContext.getCurrentContext());
            }

            // Attempt to write to the log
            try {
                runtime.getAddressSpaceView().write(tokenResponse, object);
                // If we're here, we succeeded, return the acquired token
                return tokenResponse.getTokenValue();
            } catch (OverwriteException oe) {

                // We were overwritten, get a new token and try again.
                log.warn("append[{}]: Overwritten after {} retries, streams {}",
                        tokenResponse.getTokenValue(),
                        x,
                        streamIDs.stream().map(Utils::toReadableId).collect(Collectors.toSet()));

                TokenResponse temp;
                if (conflictInfo == null) {
                    // Token w/o conflict info
                    temp = runtime.getSequencerView().nextToken(streamIDs, 1);
                } else {

                    // On retry, check for conflicts only from the previous
                    // attempt position
                    conflictInfo.setSnapshotTimestamp(tokenResponse.getToken().getTokenValue());

                    // Token w/ conflict info
                    temp = runtime.getSequencerView().nextToken(streamIDs,
                            1, conflictInfo);
                }

                // We need to fix the token (to use the stream addresses- may
                // eventually be deprecated since these are no longer used)
                tokenResponse = new TokenResponse(
                        temp.getRespType(), tokenResponse.getConflictKey(),
                        temp.getToken(), temp.getBackpointerMap());

            } catch (StaleTokenException se) {
                // the epoch changed from when we grabbed the token from sequencer
                log.warn("append[{}]: StaleToken , streams {}", tokenResponse.getTokenValue(),
                        streamIDs.stream().map(Utils::toReadableId).collect(Collectors.toSet()));

                throw new TransactionAbortedException(
                        conflictInfo,
                        tokenResponse.getConflictKey(),
                        AbortCause.NEW_SEQUENCER, // in the future, perhaps define a new AbortCause?
                        TransactionalContext.getCurrentContext());
            }
        }

        log.error("append[{}]: failed after {} retries , streams {}, write size {} bytes",
                tokenResponse.getTokenValue(),
                runtime.getParameters().getWriteRetry(),
                streamIDs.stream().map(Utils::toReadableId).collect(Collectors.toSet()),
                ILogData.getSerializedSize(object));
        throw new AppendException();
    }
}
