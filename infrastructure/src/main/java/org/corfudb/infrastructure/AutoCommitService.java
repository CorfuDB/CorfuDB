package org.corfudb.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.LambdaUtils;
import org.corfudb.util.Sleep;
import org.corfudb.util.concurrent.SingletonResource;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * An auto-commit service that periodically commits the unwritten addresses
 * in the global log, continuously consolidating the log prefix.
 * <p>
 * Created by WenbinZhu on 9/19/19.
 */
@Slf4j
public class AutoCommitService implements ManagementService {

    private static final int COMMIT_BATCH_SIZE = 500;
    private static final int COMMIT_RETRY_LIMIT = 5;

    private final ServerContext serverContext;
    private final SingletonResource<CorfuRuntime> runtimeSingletonResource;
    private final ScheduledExecutorService autoCommitScheduler;
    // The global log tail fetch at last auto commit cycle, which would be
    // the commit upper bound in the current cycle.
    private long lastLogTail = Address.NON_ADDRESS;

    AutoCommitService(@NonNull ServerContext serverContext,
                      @NonNull SingletonResource<CorfuRuntime> runtimeSingletonResource) {
        this.serverContext = serverContext;
        this.runtimeSingletonResource = runtimeSingletonResource;
        this.autoCommitScheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat(serverContext.getThreadPrefix() + "AutoCommitService")
                        .build());
    }

    private CorfuRuntime getCorfuRuntime() {
        return runtimeSingletonResource.get();
    }

    /**
     * Starts the long running service.
     *
     * @param interval interval to run the service
     */
    @Override
    public void start(Duration interval) {
        autoCommitScheduler.scheduleAtFixedRate(
                () -> LambdaUtils.runSansThrow(this::runAutoCommit),
                0,
                interval.toMillis(),
                TimeUnit.MILLISECONDS
        );
    }

    @VisibleForTesting
    void runAutoCommit() {
        log.trace("runAutoCommit: start committing addresses.");

        // Deterministically do auto commit if the current node is primary sequencer.
        if (!isCurrentNodePrimarySequencer(updateLayoutAndGet())) {
            return;
        }

        // Initialize lastLogTail if necessary.
        // We only commit up to the global tail at the time of last auto commit cycle.
        if (!Address.isAddress(lastLogTail)) {
            lastLogTail = getCorfuRuntime().getSequencerView().query().getSequence();
            log.info("runAutoCommit: lastLogTail unknown, initialized to {}", lastLogTail);
            return;
        }

        // Fetch the minimum committed tail from all the log units.
        // This step is needed as this node might just be elected as the auto committer.
        long committedTail = getCorfuRuntime().getAddressSpaceView().getCommittedTail();
        log.info("runAutoCommit: trying to commit [{}, {}].", committedTail + 1, lastLogTail);

        // Commit addresses in batches, retry limit is shared by all batches in this cycle.
        int retry = 0;
        while (committedTail < lastLogTail) {
            long commitStart = committedTail + 1;
            long commitEnd = Math.min(committedTail + COMMIT_BATCH_SIZE, lastLogTail);
            try {
                getCorfuRuntime().getAddressSpaceView().commit(commitStart, commitEnd);
                log.trace("runAutoCommit: successfully committed [{}, {}]", committedTail, commitEnd);
                committedTail = commitEnd;
            } catch (RuntimeException re) {
                log.info("runAutoCommit: encountered an exception when trying to commit " +
                        "[{}, {}] on retry {}, cause: {}", commitStart, commitEnd, retry, re.getCause());
                if (++retry >= COMMIT_RETRY_LIMIT) {
                    log.warn("runAutoCommit: retry exhausted, abort and wait for next cycle");
                    break;
                }
                // Invalidate runtime layout.
                if (!isCurrentNodePrimarySequencer(updateLayoutAndGet())) {
                    return;
                }
                Sleep.sleepUninterruptibly(getCorfuRuntime().getParameters().getConnectionRetryRate());
            } catch (Throwable t) {
                log.error("runAutoCommit: encountered unexpected exception", t);
                lastLogTail = getCorfuRuntime().getSequencerView().query().getSequence();
                throw t;
            }
        }

        // Update lastLogTail no matter commit succeeds or not.
        lastLogTail = getCorfuRuntime().getSequencerView().query().getSequence();
    }

    private Layout updateLayoutAndGet() {
        return getCorfuRuntime()
                .invalidateLayout()
                .thenApply(serverContext::saveManagementLayout)
                .join();
    }

    private boolean isCurrentNodePrimarySequencer(Layout layout) {
        return layout.getPrimarySequencer().equals(serverContext.getLocalEndpoint());
    }

    /**
     * Clean up.
     */
    @Override
    public void shutdown() {
        autoCommitScheduler.shutdownNow();
        log.info("Auto commit service shutting down.");
    }
}
