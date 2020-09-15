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
 * An auto-commit service that periodically commits the unwritten addresses
 * in the global log, continuously consolidating the log prefix.
 * <p>
 * Created by WenbinZhu on 5/5/20.
 */
@Slf4j
public class AutoCommitService implements ManagementService {

    public static final int COMMIT_BATCH_SIZE = 500;
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
    public synchronized void runAutoCommit() {
        boolean trimmed = false;
        long lastEpoch = -1;
        Layout currentLayout = null;

        for (int i = 1; i <= MAX_COMMIT_RETRY; i++) {
            try {
                // Do not perform auto commit if current node is not primary sequencer.
                currentLayout = updateLayoutAndGet();
                if (!isNodePrimarySequencer(currentLayout)) {
                    log.trace("runAutoCommit: not primary sequencer, stop auto commit.");
                    resetTails();
                    return;
                }

                log.trace("runAutoCommit: start committing addresses.");

                // Initialize lastLogTail and committedTail if the first time. We only
                // commit up to the global tail at the time of last auto commit cycle.
                if (!Address.isAddress(lastLogTail)) {
                    log.info("runAutoCommit: initializing auto commit.");
                    initializeTails(currentLayout);
                    log.info("runAutoCommit: initialized tails, committedTail: {}, " +
                            "lastLogTail: {}", committedTail, lastLogTail);
                    return;
                }

                // Fetch maximum trim mark from log units and compare with committed tail.
                // In order not to fetch trim mark for every retry, we only do this when
                // the epoch changes after last attempt.
                long currEpoch = currentLayout.getEpoch();
                if (lastEpoch < 0 || trimmed || currEpoch != lastEpoch) {
                    Token trimMark = getCorfuRuntime().getAddressSpaceView().getTrimMark(false);
                    long lastTrimmed = trimMark.getSequence() - 1;
                    // Make sure all log units have same trim mark, then we can start committing
                    // from the trim mark instead of the trailing committed tail.
                    if (committedTail < lastTrimmed) {
                        log.info("runAutoCommit: committedTail {} < last trimmed address {}, " +
                                "starting prefix trim.", committedTail, lastTrimmed);
                        Token trimToken = new Token(trimMark.getEpoch(), lastTrimmed);
                        getCorfuRuntime().getAddressSpaceView().prefixTrim(trimToken, false);
                        committedTail = trimMark.getSequence() - 1;
                    }
                    lastEpoch = currEpoch;
                    trimmed = false;
                }

                log.trace("runAutoCommit: trying to commit [{}, {}].", committedTail + 1, lastLogTail);

                // Commit addresses in batches, retry limit is shared by all batches in this cycle.
                // NOTE: This implementation relies on the fact that state transfer invokes the
                // read protocol and fill holes, otherwise the committedTail could be invalid.
                // (e.g. 1. State transferred a hole at address 100 from A to B; 2. Commit address
                // 100, which only goes to A as B is not in this address segment; 3. B finishes
                // state transfer and segments merged; 4. Send a new committedTail to B which is
                // invalid as 100 is still a hole on B.
                long oldCommittedTail = committedTail;
                while (committedTail < lastLogTail) {
                    long commitStart = committedTail + 1;
                    long commitEnd = Math.min(committedTail + COMMIT_BATCH_SIZE, lastLogTail);
                    getCorfuRuntime().getAddressSpaceView().commit(commitStart, commitEnd);
                    log.trace("runAutoCommit: successfully committed batch [{}, {}]", committedTail, commitEnd);
                    committedTail = commitEnd;
                }

                log.debug("runAutoCommit: successfully finished auto commit cycle, [{}, {}].",
                        oldCommittedTail + 1, committedTail);
                break;

            } catch (RuntimeException re) {
                log.trace("runAutoCommit: encountered an exception on attempt {}/{}.",
                        i, MAX_COMMIT_RETRY, re);

                if (i >= MAX_COMMIT_RETRY) {
                    log.error("runAutoCommit: retry exhausted, abort and wait for next cycle.", re);
                    break;
                }

                // When inspecting address, log unit server could throw a TrimmedException if
                // the address to inspect is trimmed, then we need to adjust committed tail.
                if (re instanceof TrimmedException) {
                    trimmed = true;
                }

                if (re instanceof NetworkException || re.getCause() instanceof TimeoutException) {
                    Sleep.sleepUninterruptibly(CONN_RETRY_RATE);
                }

            } catch (Throwable t) {
                log.error("runAutoCommit: encountered unexpected exception", t);
                updateLastLogTail(currentLayout);
                throw t;
            }
        }

        // Update lastLogTail no matter commit cycle succeeds or not.
        updateLastLogTail(currentLayout);
    }

    private Layout updateLayoutAndGet() {
        return getCorfuRuntime()
                .invalidateLayout()
                .join();
    }

    private boolean isNodePrimarySequencer(Layout layout) {
        return layout.getPrimarySequencer().equals(serverContext.getLocalEndpoint());
    }

    private void resetTails() {
        lastLogTail = Address.NON_ADDRESS;
        committedTail = Address.NON_ADDRESS;
    }

    private void initializeTails(Layout layout) {
        committedTail = getCorfuRuntime().getAddressSpaceView().getCommittedTail();
        updateLastLogTail(layout);
    }

    private void updateLastLogTail(Layout layout) {
        if (layout == null) {
            return;
        }

        TokenResponse token = CFUtils.getUninterruptibly(getCorfuRuntime()
                .getLayoutView()
                .getRuntimeLayout(layout)
                .getPrimarySequencerClient()
                .nextToken(Collections.emptyList(), 0)
        );

        lastLogTail = token.getSequence();
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
