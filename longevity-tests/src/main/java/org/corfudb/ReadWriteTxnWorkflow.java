package org.corfudb;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.ExampleSchemas.ExampleKey;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxnContext;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class ReadWriteTxnWorkflow extends Workflow {
    CorfuRuntime corfuRuntime;
    CorfuStore corfuStore;
    CommonUtils commonUtils;
    String namespace;
    List<String> readTableNames;
    List<String> writeTableNames;
    List<Integer> readDistribution;
    List<Integer> writeDistribution;
    Duration interval;
    private static int BOUND = 100;
    private static int PAYLOAD_SIZE = 500;

    public ReadWriteTxnWorkflow(String name, String propFilePath) {
        super(name, propFilePath);
    }

    @Override
    public void init(CorfuRuntime corfuRuntime, CommonUtils commonUtils) {
        this.corfuRuntime = corfuRuntime;
        this.corfuStore = new CorfuStore(corfuRuntime);
        this.commonUtils = commonUtils;

        this.namespace = properties.getProperty("txn.namespace");
        this.readTableNames = Arrays.asList(properties.getProperty("read.tablenames").split(","));
        this.writeTableNames = Arrays.asList(properties.getProperty("write.tablenames").split(","));
        this.readDistribution = Arrays.stream(properties.getProperty("read.distribution").split(","))
                .map(Integer::parseInt)
                .collect(Collectors.toList());
        this.writeDistribution = Arrays.stream(properties.getProperty("write.distribution").split(","))
                .map(Integer::parseInt)
                .collect(Collectors.toList());
        this.interval = Duration.ofSeconds(Long.parseLong(properties.getProperty("task.interval.seconds")));

        try {
            for(String tableName : this.readTableNames) {
                commonUtils.openTable(namespace, tableName);
            }
            for(String tableName : this.writeTableNames) {
                commonUtils.openTable(namespace, tableName);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void start() {
        executor.submit(() -> submitTask(interval));
    }

    @Override
    void executeTask(long loadSize) {
        try (TxnContext txn = corfuStore.txn(namespace)) {
            for (int i = 0; i < writeTableNames.size(); i++) {
                if (ThreadLocalRandom.current().nextInt(BOUND) < writeDistribution.get(i)) {
                    ExampleKey key = commonUtils.getRandomKey(BOUND);
                    txn.putRecord(commonUtils.getTable(namespace, writeTableNames.get(i)), key,
                            commonUtils.getRandomValue(PAYLOAD_SIZE),
                            ExampleSchemas.ManagedMetadata.newBuilder().setLastModifiedTime(
                                    System.currentTimeMillis()).build());
                    log.debug("Writing to table {} with key {}", writeTableNames.get(i), key.getKey());
                }
            }

            for (int i = 0; i < readTableNames.size(); i++) {
                if (ThreadLocalRandom.current().nextInt(BOUND) < readDistribution.get(i)) {
                    ExampleKey key = commonUtils.getRandomKey(BOUND);
                    Set<Message> keySet = txn.keySet(readTableNames.get(i));
                    log.debug("Print keySet: {}", keySet.toString());
                }
            }
            txn.commit();
        }
        catch (Exception e) {
            log.error("Encountered exception in ReadWriteTxnWorkflow: ", e);
        }
    }
}
