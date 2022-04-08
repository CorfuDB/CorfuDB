package org.corfudb.runtime;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.proto.RpcCommon;

import javax.annotation.Nonnull;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.corfudb.runtime.CorfuCompactorManagement.*;

/**
 * Runs the checkpointing of locally opened tables from within the client's JVM
 * <p>
 */
@Slf4j
public class DistributedClientCheckpointer {

    private final ScheduledExecutorService compactionScheduler;
    private final DistributedCompactor distributedCompactor;

    public DistributedClientCheckpointer(@Nonnull CorfuRuntime runtime) {
        if (runtime.getParameters().checkpointTriggerFreqSecs <= 0) {
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
                runtime.getParameters().getCheckpointTriggerFreqSecs()*2,
                runtime.getParameters().getCheckpointTriggerFreqSecs(),
                TimeUnit.SECONDS
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