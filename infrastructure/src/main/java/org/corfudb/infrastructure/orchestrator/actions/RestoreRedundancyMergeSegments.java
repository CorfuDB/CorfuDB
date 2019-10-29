package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.Tuple;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.StateTransferBatchProcessorData;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor.ProtocolBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.streamprocessor.StreamProcessFailure;
import org.corfudb.infrastructure.orchestrator.Action;
import org.corfudb.infrastructure.redundancy.PrefixTrimRedundancyCalculator;
import org.corfudb.infrastructure.redundancy.RedundancyCalculator;
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
import java.util.Optional;
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
@Builder
@Getter
public class RestoreRedundancyMergeSegments extends Action {

    @Getter
    private final String currentNode;

    @Getter
    private final StreamLog streamLog;

    public RestoreRedundancyMergeSegments(String currentNode, StreamLog streamLog) {
        this.currentNode = currentNode;
        this.streamLog = streamLog;
    }


    public enum RestoreStatus {
        RESTORED,
        NOT_RESTORED
    }

    private final int retryBase = 3;
    private final int backoffDurationSeconds = 10;
    private final int extraWaitMillis = 20;
    private final float randomPart = 0.5f;
    private final int restoreRetries = 3;

    private final Consumer<ExponentialBackoffRetry> retrySettings = settings -> {
        settings.setBase(retryBase);
        settings.setExtraWait(extraWaitMillis);
        settings.setBackoffDuration(Duration.ofSeconds(
                backoffDurationSeconds));
        settings.setRandomPortion(randomPart);
    };

    RestoreStatus restoreWithBackOff(List<CurrentTransferSegment> stateList,
                                     CorfuRuntime runtime) throws Exception {
        AtomicInteger retries = new AtomicInteger(restoreRetries);
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
        }).setOptions(retrySettings).run();

    }

    /**
     * Try restore redundancy for all the currently transferred segments.
     *
     * @param stateList A list that holds the state for every segment.
     */
    RestoreStatus tryRestoreRedundancyAndMergeSegments(
            List<CurrentTransferSegment> stateList, Layout oldLayout,
            LayoutManagementView layoutManagementView) {

        // Get all the transfers that failed.
        List<CurrentTransferSegment> failed = stateList.stream()
                .filter(segment -> segment.getStatus().getSegmentState() == FAILED)
                .collect(Collectors.toList());

        // Throw the first failure if present.
        Optional<StreamProcessFailure> streamProcessFailure = failed.stream()
                .findFirst()
                .flatMap(ts -> ts.getStatus().getCauseOfFailure());

        streamProcessFailure.ifPresent(failure -> {
            throw failure;
        });

        // Filter all the transfers that have been completed.
        List<CurrentTransferSegment> transferredSegments = stateList.stream()
                .filter(segment -> segment.getStatus().getSegmentState() == TRANSFERRED)
                .collect(Collectors.toList());

        // Case 1: The transfer has occurred -> Create a new layout with a restored node, merge if possible.
        if (!transferredSegments.isEmpty()) {

            log.info("State transfer on {}: Transferred segments: {}.", currentNode,
                    transferredSegments);

            RedundancyCalculator calculator = new RedundancyCalculator(currentNode);

            // Create a new layout after the segments are transferred
            Layout newLayout = calculator.updateLayoutAfterRedundancyRestoration(
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
    public void impl(@Nonnull CorfuRuntime runtime) throws Exception {

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
        LogUnitClientMap map = (CorfuRuntime rt) -> {
            Layout currentLayout = rt.getLayoutView().getLayout();
            Set<String> allActiveServers = currentLayout.getAllActiveServers();

            return allActiveServers.stream()
                    .map(server -> {
                        RuntimeLayout runtimeLayout = rt.getLayoutView().getRuntimeLayout();
                        return new Tuple<>(server, runtimeLayout.getLogUnitClient(server));
                    })
                    .collect(Collectors.toMap(tuple -> tuple.first, tuple -> tuple.second));
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
            ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                    .builder()
                    .addressSpaceView(batchProcessorData.getAddressSpaceView())
                    .streamLog(batchProcessorData.getStreamLog())
                    .build();

            // Perform a state transfer for each segment synchronously and update the map.
            stateList = transferManager.handleTransfer(stateList, batchProcessor);

            // Restore redundancy for the segments that were restored.
            // If possible, also merge the segments.
            RestoreStatus restoreStatus = restoreWithBackOff(stateList, runtime);

            // Invalidate the layout.
            if (restoreStatus == RESTORED) {

                log.info("State transfer on {}: Updating status map.", currentNode);
                layout = runtime.invalidateLayout().join();

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
