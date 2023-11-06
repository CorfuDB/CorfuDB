package org.corfudb.runtime.view;

import com.google.common.collect.Lists;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.object.transactions.Transaction;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.CFUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Created by mwei on 12/10/15.
 */

public class SequencerView extends AbstractView {

    public SequencerView(CorfuRuntime runtime) {
        super(runtime);
    }

    /**
     * Return the current global tail token in the sequencer or the tails
     * of multiple streams.
     *
     * @param streamIds the streams to query
     * @return the global tail or a list of tails
     */
    public TokenResponse query(UUID... streamIds) {
        Supplier<TokenResponse> querySupplier = () -> {
            if (streamIds.length == 0) {
                return layoutHelper(e -> CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
                        .nextToken(Collections.emptyList(), 0)));
            } else {
                return layoutHelper(e -> CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
                        .nextToken(Arrays.asList(streamIds), 0)));
            }
        };

        long start = System.nanoTime();
        TokenResponse ret = MicroMeterUtils.time(querySupplier, "sequencer.query");
        long end = System.nanoTime();
        if (TransactionalContext.isInTransaction()) {
            AbstractTransactionalContext context = TransactionalContext.getRootContext();
            context.tailQuery++;
            for (UUID id : streamIds) {
                context.readStreams.add(id.toString());
            }
            context.timeSpent += (end - start);
        }
        return ret;

    }

    /**
     * Return the tail of a specific stream.
     *
     * @param streamId the stream to query
     * @return the stream tail
     */
    public long query(UUID streamId) {
        Supplier<Long> tailSupplier = () ->
                layoutHelper(e -> CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
                        .nextToken(Arrays.asList(streamId), 0))).getStreamTail(streamId);
        long start = System.nanoTime();
        long ret = MicroMeterUtils.time(tailSupplier, "sequencer.query");
        long end = System.nanoTime();
        if (TransactionalContext.isInTransaction()) {
            AbstractTransactionalContext context = TransactionalContext.getRootContext();
            context.tailQuery++;
            context.readStreams.add(streamId.toString());
            context.timeSpent += (end - start);
        }
        return ret;
    }

    /**
     * Return the next token in the sequencer for a particular stream.
     *
     * @param streamIds The stream IDs to retrieve from.
     * @return The first token retrieved.
     */
    public TokenResponse next(UUID... streamIds) {
        Supplier<TokenResponse> tokenSupplier = () ->
                layoutHelper(e -> CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
                        .nextToken(Arrays.asList(streamIds), 1)));
        return MicroMeterUtils.time(tokenSupplier, "sequencer.next");
    }

    /**
     * Retrieve a stream's address space from sequencer server.
     *
     * @param streamsAddressesRange range of streams address space to request.
     * @return address space composed of the trim mark and collection of all addresses belonging to this stream.
     */
    public StreamAddressSpace getStreamAddressSpace(StreamAddressRange streamsAddressesRange) {
        long start = System.nanoTime();
        StreamAddressSpace ret =  getStreamsAddressSpace(Arrays.asList(streamsAddressesRange)).get(streamsAddressesRange.getStreamID());
        long end = System.nanoTime();
        if (TransactionalContext.isInTransaction()) {
            AbstractTransactionalContext context = TransactionalContext.getRootContext();
            context.streamQuery++;
            context.readStreams.add(streamsAddressesRange.getStreamID().toString());
            context.timeSpent += (end - start);
        }
        return  ret;
    }

    /**
     * Retrieve multiple streams address space.
     *
     * @param streamsAddressesRange list of streams and ranges to be requested.
     * @return address space for each stream in the request.
     */
    public Map<UUID, StreamAddressSpace> getStreamsAddressSpace(List<StreamAddressRange> streamsAddressesRange) {
        Supplier<Map<UUID, StreamAddressSpace>> streamsAddressResponseSupplier = () ->
                layoutHelper(e ->
                        CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
                                .getStreamsAddressSpace(streamsAddressesRange)).getAddressMap());

        return MicroMeterUtils.time(streamsAddressResponseSupplier, "sequencer.stream_address_range");
    }

    /**
     * Acquire a token for a number of streams if there are no conflicts.
     *
     * @param conflictInfo transaction conflict info
     * @param streamIds    streams to acquire the token for
     * @return First token to be written for the streams if there are no conflicts
     */
    public TokenResponse next(TxResolutionInfo conflictInfo, UUID... streamIds) {
        Supplier<TokenResponse> tokenSupplier = () ->
                layoutHelper(e -> CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
                        .nextToken(Arrays.asList(streamIds), 1, conflictInfo)));
        return MicroMeterUtils.time(tokenSupplier, "sequencer.tx_resolution");
    }

    /**
     * Return the next token in the sequencer for a particular stream.
     *
     * <p>If numTokens == 0, then the streamAddressesMap returned is the last handed out token for
     * each stream (if streamIDs is not empty). The token returned is the global address as
     * previously defined, namely, max global address across all the streams.</p>
     *
     * @param streamIDs The stream IDs to retrieve from.
     * @param numTokens The number of tokens to reserve.
     * @return The first token retrieved.
     */
    @Deprecated
    public TokenResponse nextToken(Set<UUID> streamIDs, int numTokens) {
        return layoutHelper(e -> CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
                .nextToken(Lists.newArrayList(streamIDs), numTokens)));
    }

    @Deprecated
    public TokenResponse nextToken(Set<UUID> streamIDs, int numTokens,
                                   TxResolutionInfo conflictInfo) {

        return layoutHelper(e -> CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
                .nextToken(Lists.newArrayList(streamIDs), numTokens, conflictInfo)));
    }

    public void trimCache(long address) {
        runtime.getLayoutView().getRuntimeLayout().getPrimarySequencerClient().trimCache(address);
    }
}