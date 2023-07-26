package org.corfudb;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxnContext;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WastefulTxnsWorkflow extends Workflow {
    CorfuRuntime corfuRuntime;
    CorfuStore corfuStore;
    CommonUtils commonUtils;
    Duration interval = Duration.ofSeconds(60);

    public WastefulTxnsWorkflow(String name) {
        super(name);
    }

    @Override
    void init(String propFilePath, CorfuRuntime corfuRuntime, CommonUtils commonUtils) {
        this.corfuRuntime = corfuRuntime;
        this.corfuStore = new CorfuStore(corfuRuntime);
        this.commonUtils = commonUtils;

        try (InputStream input = Files.newInputStream(Paths.get(propFilePath))) {
            Properties properties = new Properties();
            properties.load(input);


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    void start() {
        this.executor.scheduleWithFixedDelay(this::executeTask,
                1, interval.toMillis()/10, TimeUnit.MILLISECONDS);
    }

    void executeTask() {
        try (TxnContext txn = corfuStore.txn("Random")) {
            //do nothing
            TimeUnit.MINUTES.sleep(1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
