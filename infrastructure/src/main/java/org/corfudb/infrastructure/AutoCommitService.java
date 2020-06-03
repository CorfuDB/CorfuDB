package org.corfudb.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.LambdaUtils;
import org.corfudb.util.Sleep;
import org.corfudb.util.concurrent.SingletonResource;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An auto-commit service that periodically commits the unwritten addresses
 * in the global log, continuously consolidating the log prefix.
 * <p>
 * Created by WenbinZhu on 5/5/20.
 */
@Slf4j
public class AutoCommitService implements ManagementService {

    private static final int COMMIT_BATCH_SIZE = 500;
    private static final int MAX_COMMIT_RETRY = 8;
    private static final Duration CONN_RETRY_RATE = Duration.ofMillis(500);

    private final ServerContext serverContext;
    private final SingletonResource<CorfuRuntime> runtimeSingletonResource;
    private final ScheduledExecutorService autoCommitScheduler;

    // Global log tail fetched at last auto commit cycle, which
    // would be the commit upper bound in the current cycle.
    private long lastLogTail = Address.NON_ADDRESS;
    // Cached committed tail so that we do not need to fetch every cycle.
    private long committedTail = Address.NON_ADDRESS;

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
        autoCommitScheduler.scheduleAtFixedRate(
                () -> LambdaUtils.runSansThrow(this::runAutoCommit),
                interval.toMillis() / 2,
                interval.toMillis(),
                TimeUnit.MILLISECONDS
        );
    }

    @VisibleForTesting
    public void runAutoCommit() {
        long lastEpoch = -1;

        for (int i = 1; i <= MAX_COMMIT_RETRY; i++) {
            try {
                // Do not perform auto commit if current node is not primary sequencer.
                if (!isNodePrimarySequencer(updateLayoutAndGet())) {
                    resetTails();
                    return;
                }

                log.debug("runAutoCommit: start committing addresses.");

                // Initialize lastLogTail and committedTail if the first time. We only
                // commit up to the global tail at the time of last auto commit cycle.
                if (!Address.isAddress(lastLogTail)) {
                    initializeTails();
                    return;
                }

                // Fetch maximum trim mark from log units and compare with committed tail.
                // In order not to fetch trim mark for every retry, we only do this when
                // the epoch changes after last attempt.
                long currEpoch = getCorfuRuntime().getLayoutView().getLayout().getEpoch();
                if (lastEpoch < 0 || currEpoch != lastEpoch) {
                    Token trimMark = getCorfuRuntime().getAddressSpaceView().getTrimMark(false);
                    // Make sure all log units have same trim mark, then we can start committing
                    // from the trim mark instead of the trailing committed tail.
                    if (committedTail < trimMark.getSequence() - 1) {
                        Token trimToken = new Token(trimMark.getEpoch(), trimMark.getSequence() - 1);
                        getCorfuRuntime().getAddressSpaceView().prefixTrim(trimToken, false);
                        committedTail = trimMark.getSequence() - 1;
                    }
                    lastEpoch = currEpoch;
                }

                log.debug("runAutoCommit: trying to commit [{}, {}].", committedTail + 1, lastLogTail);

                // Commit addresses in batches, retry limit is shared by all batches in this cycle.
                // NOTE: This implementation relies on the fact that state transfer invokes the
                // read protocol and fill holes, otherwise the committedTail could be invalid.
                // (e.g. 1. State transferred a hole at address 100 from A to B; 2. Commit address
                // 100, which only goes to A as B is not in this address segment; 3. B finishes
                // state transfer and segments merged; 4. Send a new committedTail to B which is
                // invalid as 100 is still a hole on B.
                while (committedTail < lastLogTail) {
                    long commitStart = committedTail + 1;
                    long commitEnd = Math.min(committedTail + COMMIT_BATCH_SIZE, lastLogTail);
                    getCorfuRuntime().getAddressSpaceView().commit(commitStart, commitEnd);
                    log.trace("runAutoCommit: successfully committed batch [{}, {}]", committedTail, commitEnd);
                    committedTail = commitEnd;
                }

                log.debug("runAutoCommit: successfully finished auto commit cycle. " +
                        "New committed tail: {}.", committedTail);
                break;

            } catch (RuntimeException re) {
                log.warn("runAutoCommit: encountered an exception on attempt {}/{}.",
                        i, MAX_COMMIT_RETRY, re);

                if (i >= MAX_COMMIT_RETRY) {
                    log.warn("runAutoCommit: retry exhausted, abort and wait for next cycle");
                }

                Throwable cause = re.getCause();
                if (cause instanceof TimeoutException || cause instanceof NetworkException) {
                    Sleep.sleepUninterruptibly(CONN_RETRY_RATE);
                }

            } catch (Throwable t) {
                log.error("runAutoCommit: encountered unexpected exception", t);
                lastLogTail = getCorfuRuntime().getSequencerView().query().getSequence();
                throw t;
            }
        }

        // Update lastLogTail no matter commit cycle succeeds or not.
        lastLogTail = getCorfuRuntime().getSequencerView().query().getSequence();
    }

    private Layout updateLayoutAndGet() {
        return getCorfuRuntime()
                .invalidateLayout()
                .thenApply(serverContext::saveManagementLayout)
                .join();
    }

    private boolean isNodePrimarySequencer(Layout layout) {
        return layout.getPrimarySequencer().equals(serverContext.getLocalEndpoint());
    }

    private void resetTails() {
        lastLogTail = Address.NON_ADDRESS;
        committedTail = Address.NON_ADDRESS;
    }

    private void initializeTails() {
        committedTail = getCorfuRuntime().getAddressSpaceView().getCommittedTail();
        lastLogTail = getCorfuRuntime().getSequencerView().query().getSequence();
        log.info("initTails: committedTail initialized to {}, lastLogTail " +
                "initialized to {}", committedTail, lastLogTail);
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
