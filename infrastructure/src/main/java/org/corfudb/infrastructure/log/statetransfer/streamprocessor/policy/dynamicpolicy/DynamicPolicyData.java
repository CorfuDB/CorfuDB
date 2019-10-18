package org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.dynamicpolicy;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.SlidingWindow;

import java.util.Optional;
import java.util.stream.Stream;

@AllArgsConstructor
@Getter
public class DynamicPolicyData {
    private final Stream<Optional<Batch>> tail;
    private final SlidingWindow slidingWindow;

    public DynamicPolicyData resetDynamicWindow() {
        return new DynamicPolicyData(
                tail,
                slidingWindow.toBuilder()
                        .processed(ImmutableList.of())
                        .failed(ImmutableList.of())
                        .build()
        );
    }
}
