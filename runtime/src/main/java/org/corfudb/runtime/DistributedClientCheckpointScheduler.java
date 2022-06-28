package org.corfudb.runtime;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Runs the checkpointing of locally opened tables from within the client's JVM
 * <p>
 */
@Slf4j
public class DistributedClientCheckpointScheduler {

    private final ScheduledExecutorService compactionScheduler;
    private final ClientTriggeredCheckpointer distributedCheckpointer;

    public DistributedClientCheckpointScheduler(@Nonnull CorfuRuntime runtime) {
        CheckpointerBuilder checkpointerBuilder = CheckpointerBuilder.builder()
                .corfuRuntime(runtime)
                .persistedCacheRoot(Optional.empty())
                .cpRuntime(Optional.empty())
                .isClient(true)
                .build();
        this.distributedCheckpointer = new ClientTriggeredCheckpointer(checkpointerBuilder);
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
    private void checkpointAllMyOpenedTables() {
        this.distributedCheckpointer.checkpointTables();
    }

    /**
     * Shutdown the streaming manager and clean up resources.
     */
    public void shutdown() {
        if (compactionScheduler != null) {
            this.compactionScheduler.shutdown();
        }
    }
}
