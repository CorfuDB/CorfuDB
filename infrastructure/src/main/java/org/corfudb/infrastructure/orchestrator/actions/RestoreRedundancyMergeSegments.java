package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ImmutableMap;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager;
import org.corfudb.infrastructure.log.statetransfer.StateTransferWriter;
import org.corfudb.infrastructure.log.statetransfer.transferbatchprocessor.RegularBatchProcessor;
import org.corfudb.infrastructure.orchestrator.RestoreAction;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.LayoutBuilder;

import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.*;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.TRANSFERRED;

/**
 * This action attempts to restore the redundancy for the segments, for which the current node is
 * missing. It transfers multiple segments at once and opens the segments that have been transferred.
 */
@Slf4j
public class RestoreRedundancyMergeSegments extends RestoreAction {

    @Getter
    private final String currentNode;

    public RestoreRedundancyMergeSegments(String currentNode){
        this.currentNode = currentNode;
    }

    /**
     * Restore redundancy with an exponential backoff.
     * @param stateMap A map that holds state for every segment.
     * @param runtime An instance of a runtime.
     */
    private boolean restoreRedundancyWithBackOff(Map<CurrentTransferSegment,
            CompletableFuture<CurrentTransferSegmentStatus>> stateMap,
                                              CorfuRuntime runtime){
        Supplier<Boolean> tryRestoreRedundancyAction =
                () -> tryRestoreRedundancyAndMergeSegments(stateMap, runtime);

        return RestorationExponentialBackOff.execute(tryRestoreRedundancyAction);
    }

    /**
     * Try restore redundancy for all the currently transferred segments.
     * @param stateMap A map that holds state for every segment.
     * @param runtime An instance of a runtime.
     */
    private boolean tryRestoreRedundancyAndMergeSegments(Map<CurrentTransferSegment,
            CompletableFuture<CurrentTransferSegmentStatus>> stateMap,
                                                      CorfuRuntime runtime) {

        // Filter all the transfers that have been completed.
        List<Entry<CurrentTransferSegment, CurrentTransferSegmentStatus>> completedEntries
                = stateMap.entrySet().stream().filter(entry -> entry.getValue().isDone())
                .map(entry -> new SimpleEntry<>(entry.getKey(), entry.getValue()
                .join()))
                .collect(Collectors.toList());

        // If any failed transfers exist -> fail.
        List<Entry<CurrentTransferSegment, CurrentTransferSegmentStatus>> failedEntries =
                completedEntries
                        .stream()
                        .filter(entry -> entry.getValue().getSegmentStateTransferState()
                                .equals(FAILED))
                        .collect(Collectors.toList());

        // Throw the first failure.
        if (!failedEntries.isEmpty()) {
            throw failedEntries.get(0).getValue().getCauseOfFailure();
        }

        // Filter all the segments that have been transferred.
        List<CurrentTransferSegment> transferredSegments = completedEntries
                .stream()
                .filter(entry -> entry.getValue().getSegmentStateTransferState()
                        .equals(TRANSFERRED)).map(Entry::getKey)
                .collect(Collectors.toList());

        // If there are transferred segments, invalidate.
        runtime.invalidateLayout();

        Layout oldLayout = runtime.getLayoutView().getLayout();

        // Case 1: The transferred has occurred -> Create a new layout with restored node, merge if possible.
        if (!transferredSegments.isEmpty()) {

            log.info("State transfer on {}: Transferred segments: {}.", currentNode, transferredSegments);

            RedundancyCalculator calculator = new RedundancyCalculator(currentNode);

            // Create a new layout after the segments are transferred
            Layout newLayout =
                    calculator.updateLayoutAfterRedundancyRestoration(
                            transferredSegments, oldLayout);
            log.info("State transfer on {}: New layout: {}.", currentNode, newLayout);

            // If segments can be merged, merge, if not, just reconfigure.
            if (RedundancyCalculator.canMergeSegments(newLayout, currentNode)) {
                log.info("State transfer on: {}: Can merge segments.", currentNode);
                runtime.getLayoutManagementView().mergeSegments(newLayout);
            } else {
                // Since we seal with a new epoch, we also need to bump the epoch of the new layout.
                LayoutBuilder builder = new LayoutBuilder(newLayout);
                newLayout = builder.setEpoch(oldLayout.getEpoch() + 1).build();
                runtime.getLayoutManagementView()
                        .runLayoutReconfiguration(oldLayout, newLayout, false);
            }
            log.info("State transfer on {}: Reconfiguration is successful.", currentNode);
            return true;
        }
        // Case 2: The transfer has not occurred but the segments can still be merged.
        else if(RedundancyCalculator.canMergeSegments(oldLayout, currentNode)){
            log.info("State transfer on: {}: Can merge segments.", currentNode);
            runtime.getLayoutManagementView().mergeSegments(oldLayout);
            return true;
        }
        // Case 3: Nothing to do.
        else{
            return false;
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

        // Create a helper to perform the state calculations.
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

        // Create the initial state map.
        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> stateMap =
                redundancyCalculator.createStateMap(layout);

        log.info("State transfer on {}: Initial state map: {}", currentNode, stateMap);

        while(RedundancyCalculator.requiresRedundancyRestoration(layout, currentNode)){
            // Initialize a transfer for each segment and update the map.

            stateMap = transferManager.handleTransfer(stateMap);
            // Restore redundancy for the segments that are restored.
            // If possible, also merge the segments.
            // Utilize backoff, in case multiple transfers are happening in parallel.
            boolean restored = restoreRedundancyWithBackOff(stateMap, runtime);

            // Invalidate the layout.
            if(restored){

                log.info("State transfer on {}: Updating status map.", currentNode);
                runtime.invalidateLayout();

                // Get a new layout after the consensus is reached.
                layout = runtime.getLayoutView().getLayout();

                // Create a new map from the layout.
                ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>
                        newLayoutStateMap =
                        redundancyCalculator.createStateMap(layout);

                // Merge the new and the old map into the current map.
                stateMap = redundancyCalculator.mergeMaps(stateMap, newLayoutStateMap);
            }

        }
        log.info("State transfer on {}: Restored.", currentNode);
    }
}
