package org.corfudb.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.LambdaUtils;
import org.corfudb.util.Sleep;
import org.corfudb.util.concurrent.SingletonResource;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Orchestrates distributed compaction
 * <p>
 * Created by Sundar Sridharan on 3/2/22.
 */
@Slf4j
public class CompactorService implements ManagementService {

    private static final long CONN_RETRY_DELAY_MILLISEC = 500;

    private static final Duration TRIGGER_POLICY_RATE = Duration.ofMinutes(1);

    private final ServerContext serverContext;
    private final SingletonResource<CorfuRuntime> runtimeSingletonResource;
    private final ScheduledExecutorService compactionScheduler;
    private final ICompactionTriggerPolicy compactionTriggerPolicy;
    volatile Process checkpointerProcess = null;

    //TODO: make it a prop file and maybe pass it from the server
    List<CorfuStoreMetadata.TableName> sensitiveTables = new ArrayList<>();

    CompactorService(@NonNull ServerContext serverContext,
                     @NonNull SingletonResource<CorfuRuntime> runtimeSingletonResource) {
        this.serverContext = serverContext;
        this.runtimeSingletonResource = runtimeSingletonResource;
        this.compactionScheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat(serverContext.getThreadPrefix() + "CompactorService")
                        .build());
        this.compactionTriggerPolicy = new DynamicTriggerPolicy(runtimeSingletonResource.get(), sensitiveTables);
    }

    CorfuRuntime getCorfuRuntime() {
        return runtimeSingletonResource.get();
    }

    /**
     * Starts the long running service.
     *
     * @param interval interval to run the service
     */
    @Override
    public void start(Duration interval) {
        // Have the trigger logic which is computed every min
        compactionScheduler.scheduleAtFixedRate(
                () -> {
                    if(compactionTriggerPolicy.shouldTrigger(interval.toMillis())) {
                        runCompactionOrchestrator();
                    }
                },
                TRIGGER_POLICY_RATE.toMinutes(),
                TRIGGER_POLICY_RATE.toMinutes(),
                TimeUnit.MINUTES
        );
    }

    @VisibleForTesting
    public synchronized void runCompactionOrchestrator() {
        Layout currentLayout = null;

        int MAX_COMPACTION_RETRIES = 8;
        for (int i = 1; i <= MAX_COMPACTION_RETRIES; i++) {
            try {
                // Do not perform compaction orchestration if current node is not primary sequencer.
                currentLayout = updateLayoutAndGet();
                boolean isLeader = isNodePrimarySequencer(currentLayout);
                if (!isLeader) {
                    log.trace("runCompactionOrchestrator: Node not primary sequencer, stop");
                }

                String compactionCmd;
                if (isLeader) {
                    compactionCmd = serverContext.getCompactorCommand()
                            + " --host=" + serverContext.getLocalEndpoint()
                            + " --isLeader=true";
                } else {
                    compactionCmd  = serverContext.getCompactorCommand()
                            + " --host=" + serverContext.getLocalEndpoint()
                            + " --isLeader=false";

                }
                if (this.checkpointerProcess != null && this.checkpointerProcess.isAlive()) {
                    this.checkpointerProcess.destroy();
                }
                this.checkpointerProcess = new ProcessBuilder(compactionCmd).start();
                this.checkpointerProcess.wait();
                this.checkpointerProcess = null;

                log.debug("runCompactionOrchestrator: successfully finished a cycle");
                break;

            } catch (RuntimeException re) {
                log.trace("runCompactionOrchestrator: encountered an exception on attempt {}/{}.",
                        i, MAX_COMPACTION_RETRIES, re);

                if (i >= MAX_COMPACTION_RETRIES) {
                    log.error("runCompactionOrchestrator: retry exhausted.", re);
                    break;
                }

                if (re instanceof NetworkException || re.getCause() instanceof TimeoutException) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(CONN_RETRY_DELAY_MILLISEC);
                    } catch (InterruptedException e) {
                        log.error("Interrupted in network retry delay sleep");
                        break;
                    }
                }
            } catch (Throwable t) {
                log.error("runCompactionOrchestrator: encountered unexpected exception", t);
                t.printStackTrace();
            }
        }
    }

    private Layout updateLayoutAndGet() {
        return getCorfuRuntime()
                .invalidateLayout()
                .join();
    }

    private boolean isNodePrimarySequencer(Layout layout) {
        return layout.getPrimarySequencer().equals(serverContext.getLocalEndpoint());
    }

    /**
     * Clean up.
     */
    @Override
    public void shutdown() {
        if (this.checkpointerProcess != null && this.checkpointerProcess.isAlive()) {
            this.checkpointerProcess.destroy();
            this.checkpointerProcess = null;
        }
        compactionScheduler.shutdownNow();
        log.info("Compactor Orchestrator service shutting down.");
    }
}
