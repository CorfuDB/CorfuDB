package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ImmutableMap;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.NonNull;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager;
import org.corfudb.infrastructure.log.statetransfer.StateTransferWriter;
import org.corfudb.infrastructure.log.statetransfer.transferbatchprocessor.RegularBatchProcessor;
import org.corfudb.infrastructure.orchestrator.RestoreAction;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.view.Layout;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.*;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.TRANSFERRED;

/**
 * This action attempts to restore the redundancy for the segments, for which the current node is
 * missing. It transfers multiple seconds at once and open the segments that are transferred.
 */
public class RestoreRedundancyMergeSegments extends RestoreAction {

    @Getter
    private final String currentNode;

    public RestoreRedundancyMergeSegments(String currentNode){
        this.currentNode = currentNode;
    }

    /**
     * Restore redundancy with backoff.
     * @param stateMap A map that holds state for every segment.
     * @param runtime An instance of a runtime.
     */
    private void restoreRedundancyWithBackOff(Map<CurrentTransferSegment,
            CompletableFuture<CurrentTransferSegmentStatus>> stateMap,
                                              CorfuRuntime runtime){
        Runnable tryRestoreRedundancyAction =
                () -> tryRestoreRedundancyAndMergeSegments(stateMap, runtime);

        RestorationExponentialBackOff.execute(tryRestoreRedundancyAction);
    }

    /**
     * Try restore redundancy for all the currently transferred segments.
     * @param stateMap A map that holds state for every segment.
     * @param runtime An instance of a runtime.
     */
    private void tryRestoreRedundancyAndMergeSegments(Map<CurrentTransferSegment,
            CompletableFuture<CurrentTransferSegmentStatus>> stateMap,
                                                      CorfuRuntime runtime) {

        // Filter all the transfers that has been completed.
        List<Map.Entry<CurrentTransferSegment, CurrentTransferSegmentStatus>> completedEntries
                = stateMap.entrySet().stream().filter(entry -> {
            CompletableFuture<CurrentTransferSegmentStatus> status = entry.getValue();
            return status.isDone();
        }).map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue()
                .join()))
                .collect(Collectors.toList());

        // If any failed transfers exist -> fail.
        List<Map.Entry<CurrentTransferSegment, CurrentTransferSegmentStatus>> failedEntries =
                completedEntries
                        .stream()
                        .filter(entry -> entry.getValue().getSegmentStateTransferState()
                                .equals(FAILED))
                        .collect(Collectors.toList());

        if (!failedEntries.isEmpty()) {
            throw new IllegalStateException("Transfer failed for one or more segments.");
        }

        // Filter all the segments that have been transferred.
        List<CurrentTransferSegment> transferredSegments = completedEntries
                .stream()
                .filter(entry -> entry.getValue().getSegmentStateTransferState()
                        .equals(TRANSFERRED)).map(Map.Entry::getKey)
                .collect(Collectors.toList());

        if (!transferredSegments.isEmpty()) {

            // If there are transferred segments, invalidate
            runtime.invalidateLayout();

            Layout oldLayout = runtime.getLayoutView().getLayout();

            RedundancyCalculator calculator = new RedundancyCalculator(currentNode);

            // Create a new layout after the segments are transferred
            Layout newLayout =
                    calculator.updateLayoutAfterRedundancyRestoration(
                            transferredSegments, oldLayout);

            // If segments can be merged, merge, if not, just reconfigure.
            if (RedundancyCalculator.canMergeSegments(newLayout)) {
                runtime.getLayoutManagementView().mergeSegments(newLayout);
            } else {
                runtime.getLayoutManagementView()
                        .runLayoutReconfiguration(oldLayout, newLayout, false);
            }
        }
    }

    @Nonnull
    @Override
    public String getName() {
        return "RestoreRedundancyAndMergeSegments";
    }

    @Override
    public void impl(@Nonnull CorfuRuntime runtime, @NonNull StreamLog streamLog) throws Exception {

        // Refresh layout.
        runtime.invalidateLayout();
        Layout layout = runtime.getLayoutView().getLayout();

        // Create a helper class to perform state calculations.
        PrefixTrimRedundancyCalculator redundancyCalculator =
                new PrefixTrimRedundancyCalculator(currentNode, runtime);

        // Create a transfer manager instance.
        RegularBatchProcessor transferBatchProcessor =
                new RegularBatchProcessor(streamLog, runtime.getAddressSpaceView());

        StateTransferWriter stateTransferWriter =
                new StateTransferWriter(transferBatchProcessor);

        StateTransferManager transferManager =
                new StateTransferManager(streamLog,
                        stateTransferWriter,
                        runtime.getParameters().getBulkReadSize());

        // Create an initial state map.
        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> stateMap =
                redundancyCalculator.createStateMap(layout);


        while(!redundancyCalculator.redundancyIsRestored(stateMap)){
            // Initialize a transfer for each segment and update the map.
            stateMap = transferManager.handleTransfer(stateMap);

            // Try restore redundancy for the segments that are restored.
            // If possible, also merge segments.
            // Utilize backoff, in case multiple transfers happen.
            restoreRedundancyWithBackOff(stateMap, runtime);

            // Invalidate the layout.
            runtime.invalidateLayout();

            // Get a new layout after the consensus.
            layout = runtime.getLayoutView().getLayout();

            // Create a new map from the layout.
            ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>
                    newLayoutStateMap =
                    redundancyCalculator.createStateMap(layout);

            // Merge the new and the old map into the current map.
            stateMap = redundancyCalculator.mergeMaps(stateMap, newLayoutStateMap);
        }
    }
}
