package org.corfudb;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.ExampleSchemas.ExampleValue;
import org.corfudb.runtime.ExampleSchemas.ExampleKey;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
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

    public ReadWriteTxnWorkflow(String name) {
        super(name);
    }

    @Override
    public void init(String propFilePath, CorfuRuntime corfuRuntime, CommonUtils commonUtils) {
        this.corfuRuntime = corfuRuntime;
        this.corfuStore = new CorfuStore(corfuRuntime);
        this.commonUtils = commonUtils;

        try (InputStream input = Files.newInputStream(Paths.get(propFilePath))) {
            Properties properties = new Properties();
            properties.load(input);
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
        this.executor.scheduleWithFixedDelay(this::submitTask,
                1, interval.toMillis()/10, TimeUnit.MILLISECONDS);
    }

    @Override
    void executeTask() {
        try (TxnContext txn = corfuStore.txn(namespace)) {
            for (int i = 0; i < writeTableNames.size(); i++) {
                if (ThreadLocalRandom.current().nextInt(BOUND) < writeDistribution.get(i)) {
                    ExampleKey key = commonUtils.getRandomKey(BOUND);
                    txn.putRecord(commonUtils.getTable(namespace, writeTableNames.get(i)), key,
                            commonUtils.getRandomValue(PAYLOAD_SIZE),
                            ExampleSchemas.ManagedMetadata.newBuilder().setLastModifiedTime(System.currentTimeMillis()).build());
                    log.debug("Writing to table {} with key {}", writeTableNames.get(i), key.getKey());
                }
            }

            for (int i = 0; i < readTableNames.size(); i++) {
                if (ThreadLocalRandom.current().nextInt(BOUND) < readDistribution.get(i)) {
                    ExampleKey key = commonUtils.getRandomKey(BOUND);
                    CorfuStoreEntry corfuStoreEntry = txn.getRecord(readTableNames.get(i), key);
                    if (corfuStoreEntry.getPayload() != null) {
                        ExampleValue value = (ExampleValue) corfuStoreEntry.getPayload();
                        log.debug("Print record: Key: {} Value: {}", key, value.getPayload());
                    }
                }
            }

            txn.commit();
        }
        catch (Exception e) {
            log.error("Encountered exception in ReadWriteTxnWorkflow: ", e);
        }
    }
}
