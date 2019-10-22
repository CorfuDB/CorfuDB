package org.corfudb.infrastructure.log.statetransfer.streamprocessor;

import com.google.common.collect.Lists;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.PolicyStreamProcessor.SlidingWindow;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.dynamicpolicy.DynamicPolicy;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.dynamicpolicy.DynamicPolicyData;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class StreamTest {

    public DynamicPolicyData createDynamicPolicyData(int batchSize,
                                                     List<Long> addresses,
                                                     Optional<List<String>> servers,
                                                     SlidingWindow slidingWindow) {
        List<List<Long>> batches = Lists.partition(addresses, batchSize);
        Stream<Optional<Batch>> stream = IntStream.range(0, batches.size()).boxed().map(batchIndex -> new Batch(batches.get(batchIndex),
                servers.map(list -> list.get(batchIndex % list.size())))).map(Optional::of);

        return new DynamicPolicyData(stream, slidingWindow, batches.size());
    }
}
