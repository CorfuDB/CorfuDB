package org.corfudb.infrastructure.log.statetransfer.streamprocessor.policy.staticpolicy;

import com.google.common.collect.Lists;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class RoundRobinPolicy implements StaticPolicy {
    @Override
    public Stream<Optional<Batch>> applyPolicy(StaticPolicyData data) {
        int defaultBatchSize = data.getDefaultBatchSize();
        List<Long> addresses = data.getAddresses();
        List<String> availableServers = data.getAvailableServers();
        return Optional.ofNullable(addresses)
                .flatMap(
                        addrs -> {
                            if (defaultBatchSize > 0) {
                                return Optional.of(Lists.partition(addrs, defaultBatchSize));
                            } else {
                                return Optional.empty();
                            }
                        }
                ).map(partitionedList ->
                        IntStream
                                .range(0, partitionedList.size())
                                .boxed()
                                .map(index -> Optional.ofNullable(availableServers)
                                        .map(servers -> {
                                            String scheduledServer =
                                                    servers.get(index % servers.size());
                                            return new Batch(partitionedList.get(index), scheduledServer);
                                        }))).orElseGet(() -> Stream.of(Optional.empty()));
    }
}
