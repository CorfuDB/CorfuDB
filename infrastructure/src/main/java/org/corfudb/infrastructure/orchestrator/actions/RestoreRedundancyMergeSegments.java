package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.Tuple;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessorData;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor.ProtocolBatchProcessor;
import org.corfudb.infrastructure.orchestrator.RestoreAction;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.LayoutBuilder;
import org.corfudb.runtime.view.LayoutManagementView;
import org.corfudb.runtime.view.RuntimeLayout;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.RetryNeededException;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CurrentTransferSegment;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.TRANSFERRED;
import static org.corfudb.infrastructure.orchestrator.actions.RestoreRedundancyMergeSegments.RestoreStatus.RESTORED;

/**
 * This action attempts to restore the redundancy for the segments, for which the current node is
 * missing. It transfers multiple segments at once and opens the segments that have been transferred.
 */
@Slf4j
public class RestoreRedundancyMergeSegments extends RestoreAction {

    @Getter
    private final String currentNode;

    public RestoreRedundancyMergeSegments(String currentNode) {
        this.currentNode = currentNode;
    }


    public enum RestoreStatus {
        RESTORED,
        NOT_RESTORED
    }

    private static final int RETRY_BASE = 3;
    private static final int BACKOFF_DURATION_SECONDS = 10;
    private static final int EXTRA_WAIT_MILLIS = 20;
    private static final float RANDOM_PART = 0.5f;

    private static final Consumer<ExponentialBackoffRetry> RETRY_SETTINGS = settings -> {
        settings.setBase(RETRY_BASE);
        settings.setExtraWait(EXTRA_WAIT_MILLIS);
        settings.setBackoffDuration(Duration.ofSeconds(
                BACKOFF_DURATION_SECONDS));
        settings.setRandomPortion(RANDOM_PART);
    };

    RestoreStatus restoreWithBackOff(List<CurrentTransferSegment> stateList,
                                     CorfuRuntime runtime) throws Exception {
        AtomicInteger retries = new AtomicInteger(3);
        try {
            return IRetry.build(ExponentialBackoffRetry.class, RetryExhaustedException.class, () -> {
                try {

                    runtime.invalidateLayout();
                    Layout oldLayout = runtime.getLayoutView().getLayout();
                    LayoutManagementView layoutManagementView = runtime.getLayoutManagementView();
                    return tryRestoreRedundancyAndMergeSegments(stateList, oldLayout, layoutManagementView);
                } catch (WrongEpochException | QuorumUnreachableException | OutrankedException e) {
                    log.warn("Got: {}. Retrying: {} times.", e.getMessage(), retries.get());
                    if (retries.decrementAndGet() < 0) {
                        log.error("Retries exhausted.");
                        throw new RetryExhaustedException("Retries exhausted.");
                    } else {
                        throw new RetryNeededException();
                    }
                }
            }).setOptions(RETRY_SETTINGS).run();
        } catch (Exception e) {
            throw e;
        }

    }

    /**
     * Try restore redundancy for all the currently transferred segments.
     *
     * @param stateList A list that holds the state for every segment.
     */
    RestoreStatus tryRestoreRedundancyAndMergeSegments(List<CurrentTransferSegment> stateList,
                                                       Layout oldLayout,
                                                       LayoutManagementView layoutManagementView) {
        // Get all the transfers that are done.
        List<CurrentTransferSegment> doneTransfers =
                stateList.stream().filter(segment -> segment.getStatus().isDone())
                        .collect(Collectors.toList());

        // Get all the transfers that failed.
        List<CurrentTransferSegment> failed = doneTransfers.stream()
                .filter(segment -> segment.getStatus().join().getSegmentState().equals(FAILED))
                .collect(Collectors.toList());

        // Throw the first failure.
        if (!failed.isEmpty()) {
            throw failed.get(0).getStatus().join().getCauseOfFailure().get();
        }

        // Filter all the transfers that have been completed.
        List<CurrentTransferSegment> transferredSegments
                = doneTransfers.stream().filter(segment -> segment
                .getStatus().join().getSegmentState().equals(TRANSFERRED))
                .collect(Collectors.toList());


        // Case 1: The transfer has occurred -> Create a new layout with a restored node, merge if possible.
        if (!transferredSegments.isEmpty()) {

            log.info("State transfer on {}: Transferred segments: {}.", currentNode,
                    transferredSegments);

            RedundancyCalculator calculator = new RedundancyCalculator(currentNode);

            // Create a new layout after the segments are transferred
            Layout newLayout =
                    calculator.updateLayoutAfterRedundancyRestoration(
                            transferredSegments, oldLayout);
            log.info("State transfer on {}: New layout: {}.", currentNode, newLayout);

            // If segments can be merged, merge, if not, just reconfigure.
            if (RedundancyCalculator.canMergeSegments(newLayout, currentNode)) {
                log.info("State transfer on: {}: Can merge segments.", currentNode);
                layoutManagementView.mergeSegments(newLayout);
            } else {
                // Since we seal with a new epoch, we also need to bump the epoch of the new layout.
                LayoutBuilder builder = new LayoutBuilder(newLayout);
                newLayout = builder.setEpoch(oldLayout.getEpoch() + 1).build();
                layoutManagementView
                        .runLayoutReconfiguration(oldLayout, newLayout, false);
            }
            log.info("State transfer on {}: Reconfiguration is successful.", currentNode);
            return RestoreStatus.RESTORED;
        }
        // Case 2: The transfer has not occurred but the segments can still be merged.
        else if (RedundancyCalculator.canMergeSegments(oldLayout, currentNode)) {
            log.info("State transfer on: {}: Can merge segments.", currentNode);
            layoutManagementView.mergeSegments(oldLayout);
            return RestoreStatus.RESTORED;
        }
        // Case 3: Nothing to do.
        else {
            return RestoreStatus.NOT_RESTORED;
        }

    }

    @Nonnull
    @Override
    public String getName() {
        return "RestoreRedundancyAndMergeSegments";
    }

    @FunctionalInterface
    public interface LogUnitClientMap {
        Map<String, LogUnitClient> generate(CorfuRuntime runtime);
    }

    @Override
    public void impl(@Nonnull CorfuRuntime runtime, @NonNull StreamLog streamLog) throws Exception {

        // Refresh layout.
        runtime.invalidateLayout();
        Layout layout = runtime.getLayoutView().getLayout();

        // Create a helper to perform the state calculations.
        PrefixTrimRedundancyCalculator redundancyCalculator =
                new PrefixTrimRedundancyCalculator(currentNode, runtime);

        // Create a state transfer manager.
        StateTransferManager transferManager =
                new StateTransferManager(streamLog, runtime.getParameters().getBulkReadSize());

        // Create lambda that, given a runtime, creates the log unit clients map.
        LogUnitClientMap map = (CorfuRuntime currentRuntime) -> {
            Layout currentLayout = currentRuntime.getLayoutView().getLayout();
            Set<String> allActiveServers = currentLayout.getAllActiveServers();

            return allActiveServers.stream().map(server -> {
                RuntimeLayout runtimeLayout = currentRuntime.getLayoutView().getRuntimeLayout();
                return new Tuple<>(server, runtimeLayout.getLogUnitClient(server));
            }).collect(Collectors.toMap(tuple -> tuple.first, tuple -> tuple.second));
        };

        // Create the initial state map.
        ImmutableList<CurrentTransferSegment> stateList =
                redundancyCalculator.createStateList(layout);

        log.info("State transfer on {}: Initial state list: {}", currentNode, stateList);

        while (RedundancyCalculator.requiresRedundancyRestoration(layout, currentNode) ||
                RedundancyCalculator.requiresMerge(layout, currentNode)) {
            StateTransferBatchProcessorData batchProcessorData = new StateTransferBatchProcessorData(
                    streamLog,
                    runtime.getAddressSpaceView(),
                    map.generate(runtime)
            );

            // Create a chain replication protocol batch processor.
            ProtocolBatchProcessor batchProcessor = new ProtocolBatchProcessor(
                    batchProcessorData.getStreamLog(),
                    batchProcessorData.getAddressSpaceView()
            );

            // Perform a state transfer for each segment synchronously and update the map.
            stateList = transferManager.handleTransfer(stateList, batchProcessor);

            // Restore redundancy for the segments that were restored.
            // If possible, also merge the segments.
            RestoreStatus restoreStatus = restoreWithBackOff(stateList, runtime);

            // Invalidate the layout.
            if (restoreStatus.equals(RESTORED)) {

                log.info("State transfer on {}: Updating status map.", currentNode);
                runtime.invalidateLayout();

                // Get a new layout after the consensus is reached.
                layout = runtime.getLayoutView().getLayout();

                // Create a new map from the layout.
                ImmutableList<CurrentTransferSegment>
                        newLayoutStateList = redundancyCalculator.createStateList(layout);

                // Merge the new and the old list into the current list.
                stateList = redundancyCalculator.mergeLists(stateList, newLayoutStateList);
            }

        }
        log.info("State transfer on {}: Restored.", currentNode);
    }
}
