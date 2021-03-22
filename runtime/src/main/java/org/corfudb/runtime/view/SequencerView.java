package org.corfudb.runtime.view;

import com.google.common.collect.Lists;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.CFUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Created by mwei on 12/10/15.
 */

public class SequencerView extends AbstractView {

    // Timers used for measuring sequencer operations
    private Optional<Timer> queryRateTimer;
    private Optional<Timer> nextRateTimer;
    private Optional<Timer> resolutionRateTimer;
    private Optional<Timer> streamAddressRangeTimer;

    public SequencerView(CorfuRuntime runtime) {
        super(runtime);

        // Setup timers
        setupTimers();
    }

    /**
     * Set up timers for different sequencer request from the client perspective
     */
    private void setupTimers() {
        Optional<MeterRegistry> metricsRegistry = MeterRegistryProvider.getInstance();
        double[] percentiles = new double[]{0.50, 0.95, 0.99};
        queryRateTimer = metricsRegistry
                .map(registry ->
                        Timer.builder("sequencer.query")
                                .publishPercentiles(percentiles)
                                .publishPercentileHistogram(true)
                                .register(registry)
                );

        nextRateTimer = metricsRegistry
                .map(registry ->
                        Timer.builder("sequencer.next")
                                .publishPercentiles(percentiles)
                                .publishPercentileHistogram(true)
                                .register(registry)
                );

        resolutionRateTimer = metricsRegistry
                .map(registry -> Timer.builder("sequencer.tx_resolution")
                        .publishPercentiles(percentiles)
                        .publishPercentileHistogram(true)
                        .register(registry)
                );

        streamAddressRangeTimer = metricsRegistry.map(registry ->
                Timer.builder("sequencer.stream_address_range")
                        .publishPercentiles(percentiles)
                        .publishPercentileHistogram(true)
                        .register(registry));
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
        return queryRateTimer.map(timer -> timer.record(querySupplier)).orElseGet(() -> querySupplier.get());

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

        return queryRateTimer.map(timer -> timer.record(tailSupplier)).orElseGet(() -> tailSupplier.get());
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
        return nextRateTimer.map(timer -> timer.record(tokenSupplier)).orElseGet(() -> tokenSupplier.get());
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
        Supplier<Map<UUID, StreamAddressSpace>> streamsAddressResponseSupplier = () ->
                layoutHelper(e ->
                        CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
                                .getStreamsAddressSpace(streamsAddressesRange)).getAddressMap());

        return streamAddressRangeTimer.map(timer -> timer.record(streamsAddressResponseSupplier))
                .orElseGet(() -> streamsAddressResponseSupplier.get());
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
        return resolutionRateTimer.map(timer -> timer.record(tokenSupplier)).orElseGet(() -> tokenSupplier.get());
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