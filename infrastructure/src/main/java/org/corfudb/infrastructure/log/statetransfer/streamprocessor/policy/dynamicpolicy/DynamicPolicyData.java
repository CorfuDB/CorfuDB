package org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.dynamicpolicy;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.PolicyStreamProcessor.SlidingWindow;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * A piece of data needed for the dynamic policy invocation.
 */
@AllArgsConstructor
@Getter
public class DynamicPolicyData {
    /**
     * A current tail of a stream (batches of data yet to be processed).
     */
    private final Stream<Optional<Batch>> tail;
    /**
     * A sliding window with the aggregated statistics and/or data.
     */
    private final SlidingWindow slidingWindow;

    /**
     * Invoked on the instance
     * of a dynamic policy to purge the data
     * after it was utilized in the dynamic policy protocols.
     * @return A new instance of a dynamic policy data.
     */
    public final DynamicPolicyData resetDynamicWindow() {
        return new DynamicPolicyData(
                tail,
                slidingWindow.toBuilder()
                        .succeeded(ImmutableList.of())
                        .failed(ImmutableList.of())
                        .build()
        );
    }
}
