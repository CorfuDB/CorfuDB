package org.corfudb;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxnContext;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class ReadWriteQueuesWorkflow extends Workflow {
    private Duration interval;
    private int queueSize;
    private int payloadSize;
    private CorfuRuntime corfuRuntime;
    private CorfuStore corfuStore;
    private CommonUtils commonUtils;
    private String namespace;
    private List<String> writeQueueNames;
    private List<Double> writeDistribution;

    public ReadWriteQueuesWorkflow(String name, String propFilePath) {
        super(name, propFilePath);
    }

    @Override
    void init(CorfuRuntime corfuRuntime, CommonUtils commonUtils) {
        log.info("Write and read from queues");
        this.corfuRuntime = corfuRuntime;
        this.corfuStore = new CorfuStore(corfuRuntime);
        this.commonUtils = commonUtils;

        this.namespace = properties.getProperty("txn.namespace");
        this.queueSize = Integer.parseInt(properties.getProperty("queue.size"));
        this.payloadSize = Integer.parseInt(properties.getProperty("payload.size"));
        this.writeQueueNames = Arrays.asList(properties.getProperty("write.queuenames").split(","));
        this.writeDistribution = Arrays.stream(properties.getProperty("write.distribution").split(","))
                .map(d -> Double.parseDouble(d) * queueSize / 100)
                .collect(Collectors.toList());
        this.interval = Duration.ofSeconds(Long.parseLong(properties.getProperty("task.interval.seconds")));

        try {
            for(String tableName : this.writeQueueNames) {
                commonUtils.openQueue(namespace, tableName);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    void start() {
        executor.submit(() -> submitTask(interval));
    }

    @Override
    void executeTask(long loadSize) {
        for (int j = 0; j < loadSize; j++) {
            try (TxnContext txn = corfuStore.txn(namespace)) {
                for (int i = 0; i < writeQueueNames.size(); i++) {
                    if (ThreadLocalRandom.current().nextDouble(queueSize) < writeDistribution.get(i)) {
                        txn.enqueue(commonUtils.getQueue(namespace, writeQueueNames.get(i)),
                                commonUtils.getRandomValue(queueSize, payloadSize));
                        log.debug("Writing to table {}", writeQueueNames.get(i));
                    }
                }
                txn.commit();
            }

            log.trace("Deleting from overflowing queues...");
            try (TxnContext txn = corfuStore.txn(namespace)) {
                for (String queueName : writeQueueNames) {
                    int count = txn.count(commonUtils.getQueue(namespace, queueName));
                    if (count > queueSize) {
                        log.info("Deleting records from queue {} with count {}", queueName, count);
                        txn.entryList(commonUtils.getQueue(namespace, queueName)).subList(0, count - queueSize - 1)
                                .forEach(e -> {
                                    log.trace("queueName: {}, recordId: {}", queueName, e.getRecordId());
                                    txn.delete(queueName, e.getRecordId());
                                });
                    }
                }
                txn.commit();
            }
        }
    }
}
