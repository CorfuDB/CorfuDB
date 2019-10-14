package org.corfudb.infrastructure.log.statetransfer.transferbatchprocessor.weightedbatchprocessor;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.corfudb.common.result.Result;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.view.RuntimeLayout;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.log.statetransfer.transferbatchprocessor.weightedbatchprocessor.WeightedRoundRobinBatchProcessor.LoadProducerStatus.ACTIVE;

/**
 * Weighted round robin processor schedules the workload to the particular destination
 * endpoint by taking into the account the number of records currently present in the endpoint producer
 * queue. It most often picks the producer with the lowest number of records pending.
 */
public class WeightedRoundRobinBatchProcessor {

    @AllArgsConstructor
    public class ScheduleScore {
        private final int score;
    }

    public enum LoadProducerStatus {
        ACTIVE,
        BLOCKED
    }

    @AllArgsConstructor
    public class LoadProducerKey implements Comparable<LoadProducerKey> {
        private final ScheduleScore currentScore;
        private final UUID entryID;

        @Override
        public int compareTo(LoadProducerKey otherKey) {
            return Integer.compare(this.currentScore.score, otherKey.currentScore.score);
        }
    }

    @AllArgsConstructor
    public class LoadProducer {
        private final BlockingQueue<List<Long>> workQueue;
        private final LogUnitClient sourceClient;
        private LoadProducerStatus currentStatus;
    }

    @Getter
    private final int queueCapacityLimit;

    @Getter
    private final AtomicInteger currentTotalWeight;

    @Getter
    private final ConcurrentSkipListMap<LoadProducerKey, LoadProducer> weightedMap;

    @Getter
    private final Random randomGenerator;

    public WeightedRoundRobinBatchProcessor(Set<LogUnitClient> logUnitClients, int queueCapacityLimit) {
        this.queueCapacityLimit = queueCapacityLimit;
        this.currentTotalWeight = new AtomicInteger(0);
        this.randomGenerator = new Random();
        this.weightedMap = new ConcurrentSkipListMap<>();
        Map<LoadProducerKey, LoadProducer> map =
                logUnitClients.stream().map(client -> new SimpleEntry<>(
                        new LoadProducerKey(new ScheduleScore(0), UUID.randomUUID()),
                        new LoadProducer(new LinkedBlockingDeque<>(queueCapacityLimit), client, ACTIVE)
                )).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
        weightedMap.putAll(map);
    }

//    public Result<LoadProducer, WeightedRoundRobinBatchProcessorException> getNext(){
//        randomGenerator.nextInt(this.)
//    }

    // up to the caller of this method what to do if the queue is full


}
