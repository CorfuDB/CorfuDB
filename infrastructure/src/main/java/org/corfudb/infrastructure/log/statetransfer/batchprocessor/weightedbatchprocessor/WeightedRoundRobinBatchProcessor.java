package org.corfudb.infrastructure.log.statetransfer.batchprocessor.weightedbatchprocessor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.batch.Batch;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.RegularBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferException;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.BatchProcessorFailure;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.clients.LogUnitClient;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;

/**
 * Weighted round robin processor schedules the workload to the particular destination
 * endpoint by taking into the account the number of records currently present in the endpoint producer
 * queue. It most often picks the producer with the lowest number of records pending.
 */
public class WeightedRoundRobinBatchProcessor implements RegularBatchProcessor {

    @Override
    public CompletableFuture<Result<Long, StateTransferException>> transfer(Batch batchTransferPlan) {
        return null;
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    public class LoadProducerKey implements Comparable<LoadProducerKey> {
        private final int currentScore;

        @Override
        public int compareTo(LoadProducerKey otherKey) {
            return Double.compare(this.currentScore, otherKey.currentScore);
        }
    }

    @AllArgsConstructor
    public class LoadProducer {
        private final BlockingQueue<CompletableFuture
                <Result<Long, StateTransferException>>> workQueue;
        private final LogUnitClient sourceClient;
        private final int currentNumElements;
    }

    @Getter
    private final int queueCapacityLimit;

    @Getter
    private final StreamLog streamlog;

    @Getter
    private final ImmutableMap<UUID, LoadProducer> loadProducerMap;

    @Getter
    private final Random randomGenerator;

    public WeightedRoundRobinBatchProcessor(Set<LogUnitClient> logUnitClients, StreamLog streamlog, int queueCapacityLimit) {
        this.queueCapacityLimit = queueCapacityLimit;
        this.randomGenerator = new Random();
        this.streamlog = streamlog;
        this.loadProducerMap = initMap(logUnitClients, queueCapacityLimit);
    }

    private ImmutableMap<UUID, LoadProducer> initMap
            (Set<LogUnitClient> logUnitClients, int queueCapacityLimit) {
        Map<UUID, LoadProducer> map =
                logUnitClients.stream().map(client -> new SimpleEntry<>(
                        UUID.randomUUID(),
                        new LoadProducer(new LinkedBlockingDeque<>(queueCapacityLimit),
                                client,
                                0)
                )).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));

        return ImmutableMap.copyOf(map);
    }

    private CompletableFuture<Result<Long, StateTransferException>>
    doTransfer(LogUnitClient client, List<Long> addresses) {
        return client.readAll(addresses).thenApply(records -> {
            List<LogData> logData = ImmutableList.copyOf(records.getAddresses().values());
            return writeRecords(logData, streamlog);
        }).exceptionally(e -> Result.error(new BatchProcessorFailure(e)));
    }
}
