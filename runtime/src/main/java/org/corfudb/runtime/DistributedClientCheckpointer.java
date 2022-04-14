package org.corfudb.runtime;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Runs the checkpointing of locally opened tables from within the client's JVM
 * <p>
 */
@Slf4j
public class DistributedClientCheckpointer {

    private final ScheduledExecutorService compactionScheduler;
    private final DistributedCompactor distributedCompactor;

    public DistributedClientCheckpointer(@Nonnull CorfuRuntime runtime) {
        if (runtime.getParameters().checkpointTriggerFreqMillis <= 0) {
            this.compactionScheduler = null;
            this.distributedCompactor = null;
            return;
        }
        this.distributedCompactor = new DistributedCompactor(runtime);
        this.compactionScheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat(runtime.getParameters().getClientName() + "-chkpter")
                        .build());
        compactionScheduler.scheduleAtFixedRate(this::checkpointAllMyOpenedTables,
                runtime.getParameters().getCheckpointTriggerFreqMillis()*2,
                runtime.getParameters().getCheckpointTriggerFreqMillis(),
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * Attempt to checkpoint all the tables already materialized in my JVM heap
     */
    private synchronized void checkpointAllMyOpenedTables() {
        this.distributedCompactor.checkpointOpenedTables();
    }

    /**
     * Shutdown the streaming manager and clean up resources.
     */
    public synchronized void shutdown() {
        if (compactionScheduler != null) {
            this.compactionScheduler.shutdown();
        }
    }
}