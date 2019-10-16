package org.corfudb.infrastructure.log.statetransfer.streamprocessor;

import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.statetransfer.batch.DestinationBatch;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferException;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.view.LayoutView;
import org.corfudb.runtime.view.RuntimeLayout;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class WeightedRoundRobinStreamProcessor {

    private final Stream<DestinationBatch> batchStream;

    private final Map<String, LogUnitClient> clients;

    @AllArgsConstructor
    public static class AggregatedStatistics {
        private final int actionWindowSize;
        private final int currentWindow;
        private final Map<String,
                List<CompletableFuture<Result<Long, StateTransferException>>>> processed;
        private final Map<String,
                List<CompletableFuture<Result<Long, StateTransferException>>>> failed;
        private final Map<String, Long> meanLatency;
    }

    @AllArgsConstructor
    public static class AggregatedResult {
        private final List<CompletableFuture<Result<Long, StateTransferException>>> result;
    }

    public WeightedRoundRobinStreamProcessor(List<Long> addresses,
                                             List<String> availableServers,
                                             int defaultBatchSize,
                                             RuntimeLayout runtimeLayout) {

        this.batchStream = preprocessStream(addresses, availableServers, defaultBatchSize);
        this.clients = buildServerMap(runtimeLayout, availableServers);

    }

    public WeightedRoundRobinStreamProcessor(List<Long> addresses,
                                             List<String> availableServers,
                                             int defaultBatchSize,
                                             Map<String, LogUnitClient> clients) {
        this.batchStream = preprocessStream(addresses, availableServers, defaultBatchSize);
        this.clients = clients;
    }

    private Stream<DestinationBatch> preprocessStream(List<Long> addresses,
                                                      List<String> availableServers,
                                                      int defaultBatchSize) {
        List<List<Long>> partitionedList = Lists.partition(addresses, defaultBatchSize);
        return IntStream.range(0, partitionedList.size()).boxed().map(index -> {
            String scheduledServer = availableServers.get(index % availableServers.size());
            List<Long> batch = partitionedList.get(index);
            return new DestinationBatch(batch, 0L, scheduledServer);
        });
    }

    private Map<String, LogUnitClient> buildServerMap(RuntimeLayout runtimeLayout, List<String> availableServers) {
        return availableServers
                .stream()
                .map(runtimeLayout::getLogUnitClient)
                .collect(Collectors.toMap(LogUnitClient::getHost, client -> client));
    }

    public List<CompletableFuture<Result<Long, StateTransferException>>> processStream() {


        return null;
    }


}
