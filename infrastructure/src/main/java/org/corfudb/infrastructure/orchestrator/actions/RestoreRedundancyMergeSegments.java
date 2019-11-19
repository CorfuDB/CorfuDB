package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.StateTransferDataStore;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor.ProtocolBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.exceptions.TransferSegmentException;
import org.corfudb.infrastructure.log.statetransfer.metrics.StateTransferStats;
import org.corfudb.infrastructure.log.statetransfer.metrics.StateTransferStats.StateTransferAttemptStats.StateTransferAttemptStatsBuilder;
import org.corfudb.infrastructure.orchestrator.Action;
import org.corfudb.infrastructure.redundancy.RedundancyCalculator;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.LayoutBuilder;
import org.corfudb.runtime.view.LayoutManagementView;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.RetryNeededException;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegment;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegmentStatus.SegmentState.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegmentStatus.SegmentState.TRANSFERRED;

/**
 * This action attempts to restore the redundancy for the segments, for which the current node is
 * missing. It transfers multiple segments at once and opens the segments that have been transferred.
 */
@Slf4j
@Getter
@Builder
public class RestoreRedundancyMergeSegments extends Action {

    @Getter
    @NonNull
    private final String currentNode;

    @Getter
    @NonNull
    private final StreamLog streamLog;

    @Getter
    @NonNull
    private final RedundancyCalculator redundancyCalculator;

    @Getter
    @Default
    private final Optional<StateTransferDataStore> dataStore = Optional.empty();

    @Default
    private final int retryBase = 3;

    @Default
    private final Duration backoffDuration = Duration.ofSeconds(10);

    @Default
    private final Duration extraWait = Duration.ofMillis(20);

    @Default
    private final float randomPart = 0.5f;

    @Default
    private final int restoreRetries = 3;

    public static void updateStateTransferStats(AtomicReference<StateTransferStats> oldStats,
                                             StateTransferAttemptStatsBuilder statsBuilder) {
        if (statsBuilder == null) {
            throw new IllegalStateException("Builder should be present.");
        }
        StateTransferStats.StateTransferAttemptStats attemptStats = statsBuilder.build();
        StateTransferStats newStats = new StateTransferStats(ImmutableList.of(attemptStats));
        oldStats.updateAndGet(os -> {
            if (os == newStats) {
                throw new IllegalStateException("Non-atomic update to the stats state.");
            }

            return newStats;
        });

    }

    /**
     * Perform a state transfer on a current node, if needed, and then
     * propose a new layout based on a transfer result.
     * If a state transfer was not needed, try merging the segments
     * of a current layout and then proposing it.
     * We utilize an exponential backoff since there can be cases
     * when multiple nodes are proposing a new layout simultaneously.
     *
     * @param runtime         A corfu runtime.
     * @param transferManager A transfer manager that runs the state transfer.
     * @return A new layout, if a redundancy restoration occurred; a current layout otherwise.
     */
    Layout restoreWithBackOff(CorfuRuntime runtime, StateTransferManager transferManager)
            throws InterruptedException {

        // Set up retry settings.
        Consumer<ExponentialBackoffRetry> retrySettings = settings -> {
            settings.setBase(retryBase);
            settings.setExtraWait(extraWait.toMillis());
            settings.setBackoffDuration(backoffDuration);
            settings.setRandomPortion(randomPart);
        };

        // Configure a number of retries.
        AtomicInteger retries = new AtomicInteger(restoreRetries);
        AtomicReference<StateTransferStats> stats = new AtomicReference<>();

        return IRetry.build(ExponentialBackoffRetry.class, RetryExhaustedException.class, () -> {

            StateTransferAttemptStatsBuilder stateTransferAttemptStats =
                    StateTransferStats.StateTransferAttemptStats
                            .builder()
                            .localEndpoint(currentNode);
            try {
                // Retrieve a current layout.
                runtime.invalidateLayout();
                Layout currentLayout = runtime.getLayoutView().getLayout();

                stateTransferAttemptStats.layoutBeforeTransfer(currentLayout);

                log.info("State transfer on {}: Layout before transfer: {}",
                        currentNode, currentLayout);

                // Trim a current stream log and retrieve a global trim mark.
                long trimMark = setTrimOnNewLogUnit(currentLayout, runtime);

                // Create a pre transfer state list.
                ImmutableList<TransferSegment> preTransferList =
                        redundancyCalculator.createStateList(currentLayout, trimMark);

                stateTransferAttemptStats.initTransferSegments(preTransferList);

                // Perform a state transfer for each segment synchronously and update the state list.
                long transferStart = System.currentTimeMillis();
                ImmutableList<TransferSegment> transferList = transferManager
                        .handleTransfer(preTransferList);

                long transferDuration = System.currentTimeMillis() - transferStart;

                stateTransferAttemptStats.durationOfTransfer(Duration.ofMillis(transferDuration));

                // Get all the transfers that failed.
                List<TransferSegment> failedList = transferList.stream()
                        .filter(segment -> segment.getStatus().getSegmentState() == FAILED)
                        .collect(Collectors.toList());

                // Throw the first transfer segment exception if any of the transfers have failed.
                Optional<TransferSegmentException> transferSegmentFailure = failedList.stream()
                        .findFirst()
                        .flatMap(ts -> ts.getStatus().getCauseOfFailure());

                transferSegmentFailure.ifPresent(failure -> {
                    throw failure;
                });

                // Filter all the segments with a status TRANSFERRED.
                List<TransferSegment> transferredSegments = transferList.stream()
                        .filter(segment -> segment.getStatus().getSegmentState() == TRANSFERRED)
                        .collect(Collectors.toList());

                LayoutManagementView layoutManagementView = runtime.getLayoutManagementView();
                long restoreStart = System.currentTimeMillis();
                // State transfer did not happen. Try merging segments if possible.
                Layout newLayout;
                if (transferredSegments.isEmpty()) {
                    log.info("State transfer on: {}: No transfer occurred, " +
                            "try merging the segments.", currentNode);
                    layoutManagementView.mergeSegments(currentLayout);
                }
                // State transfer happened.
                else {
                    log.info("State transfer on {}: Transferred segments: {}.", currentNode,
                            transferredSegments);
                    // Create a new layout after the segments were transferred.
                    // After this action is performed a current node will be present
                    // in all the segments that previously had a status 'TRANSFERRED'.
                    newLayout = redundancyCalculator.updateLayoutAfterRedundancyRestoration(
                            transferredSegments, currentLayout);

                    log.info("State transfer on {}: New layout: {}.", currentNode, newLayout);

                    // Merge the segments of the new layout if possible.
                    if (RedundancyCalculator.canMergeSegments(newLayout)) {
                        layoutManagementView.mergeSegments(newLayout);
                    }
                    // If the segments can't be merged, just propose a new layout.
                    else {
                        // Since we seal with a new epoch,
                        // we also need to bump the epoch of the new layout.
                        LayoutBuilder builder = new LayoutBuilder(newLayout);
                        newLayout = builder.setEpoch(currentLayout.getEpoch() + 1).build();
                        layoutManagementView
                                .runLayoutReconfiguration(currentLayout, newLayout,
                                        false);
                    }
                }
                long restoreDuration = System.currentTimeMillis() - restoreStart;
                stateTransferAttemptStats.durationOfRestoration(Optional.of(Duration.ofMillis(restoreDuration)));
                // Return the latest layout.
                runtime.invalidateLayout();

                stateTransferAttemptStats.layoutAfterTransfer(Optional.of(runtime.getLayoutView().getLayout()));
                stateTransferAttemptStats.succeeded(true);

                updateStateTransferStats(stats, stateTransferAttemptStats);

                return runtime.getLayoutView().getLayout();

            } catch (WrongEpochException | QuorumUnreachableException | OutrankedException e) {

                updateStateTransferStats(stats, stateTransferAttemptStats);

                log.warn("Got: {}. Retrying: {} times.", e.getMessage(), retries.get());
                if (retries.decrementAndGet() < 0) {
                    throw new RetryExhaustedException("Retries exhausted.", e);
                } else {
                    throw new RetryNeededException();
                }
            } catch (TransferSegmentException e) {
                throw new RetryExhaustedException("Transfer segment exception occurred.", e);
            } finally {
                log.info("State Transfer Stats: {}", stats.get().toJson());
                dataStore.ifPresent(ds -> ds.saveStateTransferStats(stats.get()));
            }

        }).setOptions(retrySettings).run();

    }

    /**
     * Sets the trim mark on this endpoint's log unit and also perform a prefix trim.
     *
     * @param layout  A current layout.
     * @param runtime A current runtime.
     * @return A retrieved trim mark.
     */
    long setTrimOnNewLogUnit(Layout layout, CorfuRuntime runtime) {

        long trimMark = runtime.getAddressSpaceView().getTrimMark().getSequence();

        Token prefixToken = new Token(layout.getEpoch(), trimMark - 1);
        runtime.getLayoutView().getRuntimeLayout(layout)
                .getLogUnitClient(currentNode)
                .prefixTrim(prefixToken)
                .join();
        return trimMark;
    }

    @Nonnull
    @Override
    public String getName() {
        return "RestoreRedundancyAndMergeSegments";
    }

    @Override
    public void impl(@Nonnull CorfuRuntime runtime) throws Exception {

        // Refresh a layout.
        runtime.invalidateLayout();
        Layout layout = runtime.getLayoutView().getLayout();

        // Create a chain replication protocol batch processor.
        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .addressSpaceView(runtime.getAddressSpaceView())
                .streamLog(streamLog)
                .build();

        // Create a state transfer manager.
        StateTransferManager transferManager =
                StateTransferManager
                        .builder()
                        .streamLog(streamLog)
                        .batchSize(runtime.getParameters().getBulkReadSize())
                        .batchProcessor(batchProcessor)
                        .build();

        // While a redundancy can be restored or segments can be merged, perform a state transfer
        // and then restore a layout redundancy on the current node.
        while (RedundancyCalculator.canRestoreRedundancyOrMergeSegments(layout, currentNode)) {
            layout = restoreWithBackOff(runtime, transferManager);
        }
        log.info("State transfer on {}: Restored.", currentNode);
    }
}
