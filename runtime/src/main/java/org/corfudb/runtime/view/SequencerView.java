package org.corfudb.runtime.view;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.Lists;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.CFUtils;
import org.corfudb.util.CorfuComponent;
import org.corfudb.util.MetricsUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;


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
    private static MetricRegistry metricRegistry = CorfuRuntime.getDefaultMetrics();

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
     * Return the next token in the sequencer for a particular stream.
     *
     * @param streamIds The stream IDs to retrieve from.
     * @return The first token retrieved.
     */
    public TokenResponse next(UUID ... streamIds) {
        try (Timer.Context context = MetricsUtils.getConditionalContext(sequencerNextOneStream)){
            return layoutHelper(e -> CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
                    .nextToken(Arrays.asList(streamIds), 1)));
        }
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

    public void trimCache(long address) {
        try (Timer.Context context = MetricsUtils.getConditionalContext(sequencerTrimCache)){
            runtime.getLayoutView().getRuntimeLayout().getPrimarySequencerClient().trimCache(address);
        }
    }
}