package org.corfudb.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;
import org.corfudb.util.LambdaUtils;
import org.corfudb.util.Sleep;
import org.corfudb.util.concurrent.SingletonResource;

import java.time.Duration;
import java.util.Collections;
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

    private static final Duration CONN_RETRY_RATE = Duration.ofMillis(500);

    private final ServerContext serverContext;
    private final SingletonResource<CorfuRuntime> runtimeSingletonResource;
    private final ScheduledExecutorService compactionScheduler;

    CompactorService(@NonNull ServerContext serverContext,
                     @NonNull SingletonResource<CorfuRuntime> runtimeSingletonResource) {
        this.serverContext = serverContext;
        this.runtimeSingletonResource = runtimeSingletonResource;
        this.compactionScheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat(serverContext.getThreadPrefix() + "CompactorService")
                        .build());
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
        compactionScheduler.scheduleAtFixedRate(
                () -> LambdaUtils.runSansThrow(this::runCompactionOrchestrator),
                interval.toMillis() / 2,
                interval.toMillis(),
                TimeUnit.MILLISECONDS
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
                if (!isNodePrimarySequencer(currentLayout)) {
                    log.trace("runCompactionOrchestrator: Node not primary sequencer, stop");
                    return;
                }
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
                    Sleep.sleepUninterruptibly(CONN_RETRY_RATE);
                }

            } catch (Throwable t) {
                log.error("runCompactionOrchestrator: encountered unexpected exception", t);
                throw t;
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
        compactionScheduler.shutdownNow();
        log.info("Compactor Orchestrator service shutting down.");
    }
}
