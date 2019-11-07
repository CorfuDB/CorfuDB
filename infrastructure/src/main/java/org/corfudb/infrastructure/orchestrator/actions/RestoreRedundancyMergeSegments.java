package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor.ProtocolBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.exceptions.TransferSegmentException;
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
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegment;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegmentStatus.SegmentState.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegmentStatus.SegmentState.TRANSFERRED;
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
    @NonNull
    private final String currentNode;

    @Getter
    @NonNull
    private final StreamLog streamLog;

    public enum RestoreStatus {
        RESTORED,
        NOT_RESTORED
    }

    @Default
    private final int retryBase = 3;

    @Default
    private final int backoffDurationSeconds = 10;

    @Default
    private final int extraWaitMillis = 20;

    @Default
    private final float randomPart = 0.5f;

    @Default
    private final int restoreRetries = 3;

    /**
     * Invoke a tryRestoreRedundancyAndMergeSegments method with an exponential backoff.
     * A backoff is utilized in case when the multiple actions are proposing a new layout simultaneously.
     * If the exception occurs, a function will retrieve a layout and retry updating it using
     * the state list. The goal is to propose a new layout at the end without having to lose
     * the state of the transferred segments and retry this workflow all together.
     *
     * @param stateList A list of transfer segments after a state transfer is done.
     * @param runtime   A corfu runtime.
     * @return A status RESTORED, if the transferred segments of the state list are now present
     * in the accepted layout, NOT_RESTORED otherwise.
     */
    RestoreStatus restoreWithBackOff(List<TransferSegment> stateList,
                                     CorfuRuntime runtime) throws Exception {

        Consumer<ExponentialBackoffRetry> retrySettings = settings -> {
            settings.setBase(retryBase);
            settings.setExtraWait(extraWaitMillis);
            settings.setBackoffDuration(Duration.ofSeconds(
                    backoffDurationSeconds));
            settings.setRandomPortion(randomPart);
        };

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
     * Try restore redundancy for all the currently transferred segments on the current node.
     *
     * @param stateList            A list that holds the state for every segment.
     * @param oldLayout            A latest layout.
     * @param layoutManagementView A layout view.
     * @return A status of restoration: failed or succeeded.
     */
    RestoreStatus tryRestoreRedundancyAndMergeSegments(
            List<TransferSegment> stateList, Layout oldLayout,
            LayoutManagementView layoutManagementView) {

        // Get all the transfers that failed.
        List<TransferSegment> failed = stateList.stream()
                .filter(segment -> segment.getStatus().getSegmentState() == FAILED)
                .collect(Collectors.toList());

        // Throw the first failure if present.
        Optional<TransferSegmentException> transferSegmentFailure = failed.stream()
                .findFirst()
                .flatMap(ts -> ts.getStatus().getCauseOfFailure());

        transferSegmentFailure.ifPresent(failure -> {
            throw failure;
        });

        // Filter all the transfers that have been completed.
        List<TransferSegment> transferredSegments = stateList.stream()
                .filter(segment -> segment.getStatus().getSegmentState() == TRANSFERRED)
                .collect(Collectors.toList());

        // Case 1: The transfer has occurred for the current node ->
        // Create a new layout with a restored node, merge if possible.
        if (!transferredSegments.isEmpty()) {

            log.info("State transfer on {}: Transferred segments: {}.", currentNode,
                    transferredSegments);

            RedundancyCalculator calculator = new RedundancyCalculator(currentNode);

            // Create a new layout after the segments are transferred.
            // After this action is performed a node will be present in all the segments
            // that previously had a status 'TRANSFERRED'.
            Layout newLayout = calculator.updateLayoutAfterRedundancyRestoration(
                    transferredSegments, oldLayout);
            log.info("State transfer on {}: New layout: {}.", currentNode, newLayout);

            // If now the segments can be merged, merge, if not, just propose a new layout.
            if (RedundancyCalculator.canMergeSegments(newLayout)) {
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
        // Case 2: The transfer has not occurred for the current node
        // but the segments can still be merged ->
        // Merge the segments of the layout and propose it.
        else if (RedundancyCalculator.canMergeSegments(oldLayout)) {
            log.info("State transfer on: {}: Can merge segments.", currentNode);
            layoutManagementView.mergeSegments(oldLayout);
            return RestoreStatus.RESTORED;
        }
        // Case 3: Nothing to do.
        else {
            return RestoreStatus.NOT_RESTORED;
        }

    }

    /**
     * Sets the trim mark on this endpoint's log unit and also perform a prefix trim.
     *
     * @param layout   A current layout.
     * @param runtime  A current runtime.
     * @param endpoint A current endpoint.
     * @return A retrieved trim mark.
     */
    long setTrimOnNewLogUnit(Layout layout, CorfuRuntime runtime,
                             String endpoint) {

        long trimMark = runtime.getAddressSpaceView().getTrimMark().getSequence();

        Token prefixToken = new Token(layout.getEpoch(), trimMark - 1);
        runtime.getLayoutView().getRuntimeLayout(layout)
                .getLogUnitClient(endpoint)
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

        // Refresh layout.
        runtime.invalidateLayout();
        Layout layout = runtime.getLayoutView().getLayout();
        log.info("State transfer on {}: Initial layout: {}", currentNode, layout);

        // Create a helper to perform the state calculations.
        RedundancyCalculator redundancyCalculator = new RedundancyCalculator(currentNode);

        // Create a state transfer manager.
        StateTransferManager transferManager =
                new StateTransferManager(streamLog, runtime.getParameters().getBulkReadSize());

        // Trim a current stream log and retrieve a global trim mark.
        long trimMark = setTrimOnNewLogUnit(layout, runtime, currentNode);

        // Create the initial state map.
        ImmutableList<TransferSegment> stateList =
                redundancyCalculator.createStateList(layout, trimMark);

        log.info("State transfer on {}: Initial state list: {}", currentNode, stateList);

        while (RedundancyCalculator.canRestoreRedundancyOrMergeSegments(layout, currentNode)) {

            // Create a chain replication protocol batch processor.
            ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                    .builder()
                    .addressSpaceView(runtime.getAddressSpaceView())
                    .streamLog(streamLog)
                    .build();

            // Perform a state transfer for each segment synchronously and update the map.
            stateList = transferManager.handleTransfer(stateList, batchProcessor);

            // Restore redundancy for the segments that were restored.
            // If possible, also merge the segments.
            RestoreStatus restoreStatus = restoreWithBackOff(stateList, runtime);

            // Invalidate the layout.
            if (restoreStatus == RESTORED) {
                log.info("State transfer on {}: Updating status map.", currentNode);

                // Refresh layout.
                runtime.invalidateLayout();
                layout = runtime.getLayoutView().getLayout();

                // Trim a current stream log and retrieve a global trim mark.
                trimMark = setTrimOnNewLogUnit(layout, runtime, currentNode);

                // Create a new map from the layout.
                ImmutableList<TransferSegment>
                        newLayoutStateList = redundancyCalculator.createStateList(layout, trimMark);

                // Merge the new and the old lists into the current list.
                stateList = redundancyCalculator.mergeLists(stateList, newLayoutStateList);
            }

        }
        log.info("State transfer on {}: Restored.", currentNode);
    }
}
