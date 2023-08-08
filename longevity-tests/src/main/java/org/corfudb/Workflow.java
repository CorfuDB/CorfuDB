package org.corfudb;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.runtime.CorfuRuntime;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public abstract class Workflow {
    ScheduledExecutorService executor;

    public Workflow(String name) {
        executor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setNameFormat(name)
                        .build());
    }
    abstract void init(String propFilePath, CorfuRuntime corfuRuntime, CommonUtils commonUtils);
    abstract void start();
    abstract void executeTask();
    public void submitTask() {
        MicroMeterUtils.time(this::executeTask, "workflow.latency", "type", "single");
    }
    public void stop() {
        this.executor.shutdownNow();
    }

}


//Add bursts and baseline - use zipf distribution?
//end to end latency of workflow, notification receive timer -- Done
