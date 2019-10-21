package org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.staticpolicy;

import com.google.common.collect.Lists;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * A static policy that distributes the load among the available servers
 * in the round robin fashion, if they present, otherwise just initializes a stream of batches.
 */
public class RoundRobinPolicy implements StaticPolicy {
    @Override
    public InitialBatchStream applyPolicy(StaticPolicyData data) {
        int defaultBatchSize = data.getDefaultBatchSize();
        List<Long> addresses = data.getAddresses();
        Optional<List<String>> availableServers = data.getAvailableServers();
        List<List<Long>> plan = Lists.partition(addresses, defaultBatchSize);
        long size = plan.size();

        Stream<Batch> stream = IntStream.range(0, plan.size()).boxed().map(batchIndex -> new Batch(plan.get(batchIndex),
                availableServers.map(list -> list.get(batchIndex % list.size()))));
        return new InitialBatchStream(stream, size);
    }
}
