package org.corfudb.infrastructure.log.statetransfer.streamprocessor;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import org.corfudb.infrastructure.log.statetransfer.batch.BatchResult;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;


@Builder(toBuilder = true)
@Getter
public class SlidingWindow {
    @Default
    /**
     * A fixed size of a sliding window.
     */
    private final long windowSize = 10L;

    @Default
    /**
     * If a sum of the processed and failed entries >= this value,
     * invoke the dynamic protocols:
     * dynamicDistributionPolicy and/or BatchProcessorFailureHandlingPolicy.
     */
    private final long dynamicProtocolWindowSize = 2 * windowSize;
    @Default
    /**
     * All servers currently available for data distribution.
     */
    private final ImmutableList<String> allServers = ImmutableList.of();

    @Default
    /**
     * Total number of addresses transferred so far.
     */
    private final CompletableFuture<Long> totalAddressesTransferred = CompletableFuture.completedFuture(0L);


    @Default
    /**
     * Batch results that succeeded within the current dynamic protocol window.
     */
    private final ImmutableList<CompletableFuture<BatchResult>> processed = ImmutableList.of();
    @Default
    /**
     * Batch results that failed within the current dynamic protocol window.
     */
    private final ImmutableList<CompletableFuture<BatchResult>> failed = ImmutableList.of();
    @Default
    /**
     * A current window of the pending batch results.
     * Slides when the first element completes.
     */
    private final ImmutableList<CompletableFuture<BatchResult>> window = ImmutableList.of();

    /**
     * Returns true if the oldest element has completed.
     *
     * @return True if the window can be slid.
     */
    public final boolean canSlideWindow() {
        if (window.isEmpty()) {
            return true;
        } else {
            return window.get(0).isDone();
        }
    }

    public final boolean canInvokeDynamicPolicies() {
        return getCurrentDynamicProtocolWindow() == dynamicProtocolWindowSize;
    }

    private long getCurrentDynamicProtocolWindow() {
        return failed.size() + processed.size();
    }
}
