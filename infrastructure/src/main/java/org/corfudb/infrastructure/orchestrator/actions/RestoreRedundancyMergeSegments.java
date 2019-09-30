package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ImmutableMap;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.NonNull;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager;
import org.corfudb.infrastructure.log.statetransfer.StateTransferWriter;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.RegularBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.TransferBatchProcessor;
import org.corfudb.infrastructure.orchestrator.RestoreAction;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.view.Layout;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.*;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.TRANSFERRED;

/**
 * This action attempts to restore redundancy for all servers across all segments
 * starting from the oldest segment. It then collapses the segments once the set of
 * servers in the 2 oldest subsequent segments are equal.
 * Created by Zeeshan on 2019-02-06.
 */
public class RestoreRedundancyMergeSegments extends RestoreAction {

    @Getter
    private final String currentNode;

    public RestoreRedundancyMergeSegments(String currentNode){
        this.currentNode = currentNode;
    }


    /**
     * Try restore redundancy for all the transferred segments.
     * @param stateMap A map that holds state for every segment.
     * @param runtime An instance of a runtime.
     * @throws OutrankedException if the consensus on the new layout can not be reached.
     */
    private void tryRestoreRedundancyAndMergeSegments(Map<CurrentTransferSegment,
            CompletableFuture<CurrentTransferSegmentStatus>> stateMap,
                                                      CorfuRuntime runtime)
            throws OutrankedException {

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
            runtime.invalidateLayout();

            Layout oldLayout = runtime.getLayoutView().getLayout();

            RedundancyCalculator calculator = new RedundancyCalculator(currentNode);

            Layout newLayout =
                    calculator.updateLayoutAfterRedundancyRestoration(
                            transferredSegments, oldLayout);

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

    private ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>
    initTransferAndUpdateMap
            (Map<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> stateMap,
             StateTransferManager manager) {

        Map<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> newStateMap =
                manager.handleTransfer(stateMap).stream()
                        .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

        return ImmutableMap.copyOf(newStateMap);
    }

    @Override
    public void impl(@Nonnull CorfuRuntime runtime, @NonNull StreamLog streamLog) throws Exception {

        // Refresh layout.
        runtime.invalidateLayout();
        Layout layout = runtime.getLayoutView().getLayout();

        // Create a helper class to perform state calculations.
        RedundancyCalculator redundancyCalculator = new PrefixTrimRedundancyCalculator(currentNode, runtime);

        // Create a transfer batch processor to initiate a transfer manager.
        TransferBatchProcessor transferBatchProcessor = new RegularBatchProcessor(streamLog);

        StateTransferWriter stateTransferWriter = StateTransferWriter
                .builder()
                .batchProcessor(transferBatchProcessor)
                .build();

        StateTransferManager transferManager =
                builder().stateTransferWriter(stateTransferWriter).streamLog(streamLog).build();

        // Create an initial state map.
        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> stateMap =
                redundancyCalculator.createStateMap(layout);


        while(!redundancyCalculator.redundancyIsRestored(stateMap)){
            // Initialize a transfer for each segment and update the map.
            stateMap = initTransferAndUpdateMap(stateMap, transferManager);

            // Try restore redundancy for the segments that are restored.
            // If possible, also merge segments.
            tryRestoreRedundancyAndMergeSegments(stateMap, runtime);

            // Invalidate the layout.
            runtime.invalidateLayout();

            // Get a new layout after the consensus.
            layout = runtime.getLayoutView().getLayout();

            // Get the new map from the layout.
            ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>
                    newLayoutStateMap =
                    redundancyCalculator.createStateMap(layout);

            // Merge the new and the old map into the current map.
            stateMap = redundancyCalculator.mergeMaps(stateMap, newLayoutStateMap);
        }
    }
}
