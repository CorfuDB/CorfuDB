package org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.staticpolicy;

import com.google.common.collect.Lists;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * A static policy that distributes the load among the available servers
 * in the round robin fashion, if they present, otherwise just initializes a stream of batches.
 */
public class RoundRobinPolicy implements StaticPolicy {
    @Override
    public Stream<Batch> applyPolicy(StaticPolicyData data) {
        int defaultBatchSize = data.getDefaultBatchSize();
        List<Long> addresses = data.getAddresses();
        Optional<List<String>> availableServers = data.getAvailableServers();
        AtomicInteger index = new AtomicInteger(0);
        return Lists.partition(addresses, defaultBatchSize).stream().map(batch -> {
            if (availableServers.isPresent()) {
                return new Batch(addresses, Optional.empty());
            } else {
                return new Batch(addresses,
                        availableServers.map(servers ->
                                servers.get(index.getAndIncrement() % servers.size())));
            }
        });
    }
}
