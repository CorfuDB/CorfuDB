package org.corfudb.runtime.view;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.Lists;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.CFUtils;
import org.corfudb.util.CorfuComponent;
import org.corfudb.util.MetricsUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.corfudb.util.Utils.getLogAddressSpace;

/**
 * Created by mwei on 12/10/15.
 */

public class SequencerView extends AbstractView {

    // Timers used for measuring sequencer operations
    private Timer sequencerNextOneStream;
    private Timer sequencerQuery;
    private Timer sequencerNextMultipleStream;
    private Timer sequencerDeprecatedNextOneStream;
    private Timer sequencerDeprecatedNextMultipleStream;
    private Timer sequencerTrimCache;
    private Timer sequencerMutilpleStreamAddressSpace;
    private static final MetricRegistry metricRegistry = CorfuRuntime.getDefaultMetrics();

    public SequencerView(CorfuRuntime runtime) {
        super(runtime);

        // Setup timers
        setupTimers();
    }

    /**
     * Set up timers for different sequencer request from the client perspective
     */
    private void setupTimers() {
        sequencerQuery = metricRegistry.timer(CorfuComponent.CLIENT_SEQUENCER +
                "query");
        sequencerTrimCache = metricRegistry.timer(CorfuComponent.CLIENT_SEQUENCER +
                "trim-cache");
        sequencerNextOneStream = metricRegistry.timer(CorfuComponent.CLIENT_SEQUENCER +
                "particular-next");
        sequencerNextMultipleStream = metricRegistry.timer(CorfuComponent.CLIENT_SEQUENCER +
                "multiple-next");
        sequencerDeprecatedNextOneStream = metricRegistry.timer(CorfuComponent.CLIENT_SEQUENCER +
                "deprecated-particular-next");
        sequencerDeprecatedNextMultipleStream = metricRegistry.timer(CorfuComponent.CLIENT_SEQUENCER +
                "deprecated-multiple-next");
        sequencerMutilpleStreamAddressSpace = metricRegistry.timer(CorfuComponent.CLIENT_SEQUENCER +
                "multiple-stream-address-space");
    }

    /**
     * Return the next token in the sequencer for the global tail or the tails
     * of multiple streams.
     *
     * @param streamIds the streams to query
     * @return the global tail or a list of tails
     */
    public TokenResponse query(UUID... streamIds) {
        try (Timer.Context context = MetricsUtils.getConditionalContext(sequencerQuery)){
            if (streamIds.length == 0) {
                return layoutHelper(e -> CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
                        .nextToken(Collections.emptyList(), 0)));
            } else {
                return layoutHelper(e -> CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
                        .nextToken(Arrays.asList(streamIds), 0)));
            }
        }
    }

    /**
     * Return the tail of a specific stream.
     *
     * @param streamId the stream to query
     * @return the stream tail
     */
    public long query(UUID streamId) {
        try (Timer.Context context = MetricsUtils.getConditionalContext(sequencerQuery)) {
                return layoutHelper(e -> CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
                        .nextToken(Arrays.asList(streamId), 0))).getStreamTail(streamId);
        }
    }

    /**
     * Return the next token in the sequencer for a particular stream.
     *
     * @param streamIds The stream IDs to retrieve from.
     * @return The first token retrieved.
     */
    public TokenResponse next(UUID ... streamIds) {
        try (Timer.Context context = MetricsUtils.getConditionalContext(sequencerNextOneStream)) {
            return layoutHelper(e -> CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
                    .nextToken(Arrays.asList(streamIds), 1)));
        }
    }

    /**
     * Retrieve a stream's address space from sequencer server.
     *
     * @param streamsAddressesRange range of streams address space to request.
     * @return address space composed of the trim mark and collection of all addresses belonging to this stream.
     */
    public StreamAddressSpace getStreamAddressSpace(StreamAddressRange streamsAddressesRange) {
        return getStreamsAddressSpace(Arrays.asList(streamsAddressesRange)).get(streamsAddressesRange.getStreamID());
    }

    /**
     * Retrieve multiple streams address space.
     *
     * @param streamsAddressesRange list of streams and ranges to be requested.
     * @return address space for each stream in the request.
     */
    public Map<UUID, StreamAddressSpace> getStreamsAddressSpace(List<StreamAddressRange> streamsAddressesRange) {
        try (Timer.Context context = MetricsUtils.getConditionalContext(sequencerMutilpleStreamAddressSpace)) {
            StreamsAddressResponse streamsAddressResponse = layoutHelper(e ->
                    CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
                            .getStreamsAddressSpace(streamsAddressesRange)));
            return streamsAddressResponse.getAddressMap();
        }
    }

    /**
     * Retrieve IDs of all steams.
     * @return a list of stream IDs.
     */
    public List<UUID> getStreamsId() {
        return layoutHelper(e ->
                CFUtils.getUninterruptibly(e.getPrimarySequencerClient().getStreamsId()).getStreamIds()
        );
    }

    /**
     * Trim compacted addresses at address space view of specified streams.
     * This function consists of two steps:
     *
     * 1. Fetch address space up to compaction mark of streams in interest from LogUnit servers.
     * 2. Trim compacted addresses at the sequencer by replacing address space in range [0, compactionMark] with the
     *    address space just fetched in step 1.
     *
     * @param batch IDs of streams in interest.
     */
    public void addressSpaceTrimInBatch(List<UUID> batch) {
        // Relies on layoutHelper to handle exceptions.
        layoutHelper(e -> {
            StreamsAddressResponse streamsAddress = getLogAddressSpace(e, batch);
            replaceStreamsAddressSpace(streamsAddress);
            return null;
        });
    }

    private void replaceStreamsAddressSpace(StreamsAddressResponse streamAddressSpaceMap) {
        layoutHelper(e ->
                CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
                        .streamsAddressSpaceReplace(streamAddressSpaceMap)));
    }

    /**
     *
     * Acquire a token for a number of streams if there are no conflicts.
     *
     * @param conflictInfo transaction conflict info
     * @param streamIds streams to acquire the token for
     * @return First token to be written for the streams if there are no conflicts
     */
    public TokenResponse next(TxResolutionInfo conflictInfo, UUID ... streamIds) {
        try (Timer.Context context = MetricsUtils.getConditionalContext(sequencerNextMultipleStream)) {
            return layoutHelper(e -> CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
                    .nextToken(Arrays.asList(streamIds), 1, conflictInfo)));
        }
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
        try (Timer.Context context = MetricsUtils.getConditionalContext(sequencerDeprecatedNextOneStream)){
            return layoutHelper(e -> CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
                    .nextToken(Lists.newArrayList(streamIDs), numTokens)));
        }
    }

    @Deprecated
    public TokenResponse nextToken(Set<UUID> streamIDs, int numTokens,
                                   TxResolutionInfo conflictInfo) {
        try (Timer.Context context = MetricsUtils.getConditionalContext(sequencerDeprecatedNextMultipleStream)){
            return layoutHelper(e -> CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
                    .nextToken(Lists.newArrayList(streamIDs), numTokens, conflictInfo)));
        }
    }
}