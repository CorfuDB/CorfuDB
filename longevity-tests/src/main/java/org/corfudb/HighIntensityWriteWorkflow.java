package org.corfudb;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.commons.math3.distribution.ZipfDistribution;

@Slf4j
public class HighIntensityWriteWorkflow extends Workflow {

    CorfuRuntime corfuRuntime;
    CorfuStore corfuStore;
    CommonUtils commonUtils;
    private String namespace;
    private List<String> writeTableNames;
    private List<String> rareUpdateTableNames;
    private int payloadSize;
    private Duration interval;
    private Duration randomSleepInterval;
    private long tableSize;
    private double rareUpdateRatio;
    private boolean isRunning = false;

    public HighIntensityWriteWorkflow(String name, String propFilePath) {
        super(name, propFilePath);
    }

    @Override
    void init(CorfuRuntime corfuRuntime, CommonUtils commonUtils) {
        this.corfuRuntime = corfuRuntime;
        this.corfuStore = new CorfuStore(corfuRuntime);
        this.commonUtils = commonUtils;

        this.namespace = properties.getProperty("txn.namespace");
        this.writeTableNames = Arrays.asList(properties.getProperty("write.tablenames").split(","));
        this.rareUpdateTableNames = Arrays.asList(properties.getProperty("rare.update.tablenames").split(","));
        this.payloadSize = Integer.parseInt(properties.getProperty("payload.size.bytes"));
        this.randomSleepInterval = Duration.ofSeconds(Long.parseLong(properties.getProperty("random.sleepinterval.seconds")));
        this.interval = Duration.ofSeconds(Long.parseLong(properties.getProperty("task.interval.seconds")));
        this.tableSize = Long.parseLong(properties.getProperty("table.size"));
        this.rareUpdateRatio = Double.parseDouble(properties.getProperty("rare.update.ratio"));

        try {
            for(String tableName : this.writeTableNames) {
                    commonUtils.openTable(namespace, tableName);
            }
            for(String tableName : this.rareUpdateTableNames) {
                commonUtils.openTable(namespace, tableName);
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
        //write sampleload number of txns
        for (int i = 0; i < loadSize; i++) {
            try (TxnContext txn = corfuStore.txn(namespace)) {
                writeTableNames.forEach((t) -> {
                    txn.putRecord(commonUtils.getTable(namespace, t), commonUtils.getRandomKey(tableSize),
                            commonUtils.getRandomValue(payloadSize),
                            ExampleSchemas.ManagedMetadata.newBuilder().setLastModifiedTime(
                                    System.currentTimeMillis()).build());
                    log.info("Put table name : {}", t);
                });
                //Random sleep to simulate non-db related work in-between a txn
//                TimeUnit.MILLISECONDS.sleep(randomSleepInterval.toMillis());
                rareUpdateTableNames.forEach((t) -> {
                    if (ThreadLocalRandom.current().nextDouble(100) < rareUpdateRatio) {
                        txn.keySet(t);
                        log.info("Get KeySet from table: {}", t);
                    }
                });
                txn.commit();
            } catch (TransactionAbortedException tae) {
                log.error("Aborted transaction: ", tae);
            } catch (Exception e) {
                log.error("Encountered Exception: ", e);
                throw new RuntimeException(e);
            }
        }
        MicroMeterUtils.measure(loadSize, "workflow.loadsize", "name", "HighIntensityWrite");
    }

    @Override
    public void stop() {

        this.executor.shutdownNow();
    }
}
