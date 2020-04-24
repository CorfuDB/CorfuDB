package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegment;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor.ProtocolBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.exceptions.TransferSegmentException;
import org.corfudb.infrastructure.orchestrator.Action;
import org.corfudb.infrastructure.redundancy.RedundancyCalculator;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.exceptions.ServerNotReadyException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.LayoutBuilder;
import org.corfudb.runtime.view.LayoutManagementView;
import org.corfudb.util.CFUtils;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.RetryNeededException;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
    private final RedundancyCalculator redundancyCalculator;

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

    @AllArgsConstructor
    private static class LayoutTransferSegments {
        @Getter
        private final Layout layout;
        @Getter
        private final ImmutableList<TransferSegment> transferSegments;
    }

    /**
     * Trim the current log and perform the state transfer.
     * Retry for the common exceptions. If a sequencer is not currently ready or the state transfer
     * failed, throw an exception.
     *
     * @param runtime         Current runtime.
     * @param transferManager Transfer manager instance to perform a state transfer.
     * @return Current layout and the list of transferred segments.
     */
    private LayoutTransferSegments performStateTransfer(CorfuRuntime runtime, StateTransferManager transferManager)
            throws InterruptedException {
        Consumer<ExponentialBackoffRetry> retrySettings = settings -> {
            settings.setBase(retryBase);
            settings.setExtraWait(extraWait.toMillis());
            settings.setBackoffDuration(backoffDuration);
            settings.setRandomPortion(randomPart);
        };

        AtomicInteger retries = new AtomicInteger(restoreRetries);
        return IRetry.build(ExponentialBackoffRetry.class, RetryExhaustedException.class, () -> {
            try {
                // Retrieve a current layout.
                runtime.invalidateLayout();
                Layout currentLayout = runtime.getLayoutView().getLayout();
                log.info("State transfer on {}: Layout before transfer: {}",
                        currentNode, currentLayout);
                // Retrieve a current cluster trim mark.
                long trimMark = trimLog(runtime);
                // Execute state transfer and return the transferred segments and the layout.
                return new LayoutTransferSegments(currentLayout,
                        ImmutableList.copyOf(
                                getTransferSegments(transferManager, currentLayout, trimMark))
                );
            } catch (TimeoutException | NetworkException | WrongEpochException e) {
                log.warn("Got: {}. Retrying: {} times.", e.getMessage(), retries.get());
                if (retries.decrementAndGet() < 0) {
                    throw new RetryExhaustedException("Retries exhausted.", e);
                } else {
                    throw new RetryNeededException();
                }
            } catch (TransferSegmentException e) {
                throw new RetryExhaustedException("Transfer segment exception occurred.", e);
            } catch (ServerNotReadyException e) {
                throw new RetryExhaustedException("Sequencer server is not ready.", e);
            }
        }).setOptions(retrySettings).run();
    }

    /**
     * Merge the state transfer segments and propose the new cluster layout.
     *
     * @param baseClient             Base client to the current node.
     * @param layoutManagementView   Layout management view.
     * @param layoutTransferSegments Layout and the transferred segments.
     */
    private void mergeSegments(BaseClient baseClient, LayoutManagementView layoutManagementView,
                               LayoutTransferSegments layoutTransferSegments) {
        Layout currentLayout = layoutTransferSegments.getLayout();
        ImmutableList<TransferSegment> transferredSegments =
                layoutTransferSegments.getTransferSegments();

        // Seal the next epoch for the current server
        // to avoid the wrong epoch exceptions.
        CFUtils.getUninterruptibly(baseClient.sealRemoteServer(currentLayout.getEpoch() + 1));

        // State transfer did not happen. Try merging segments if possible.
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
            Layout newLayout = redundancyCalculator.updateLayoutAfterRedundancyRestoration(
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
    }

    /**
     * Perform a state transfer on a current node, if needed, and then
     * propose a new layout based on a transfer result.
     * If a state transfer was not needed, try merging the segments
     * of a current layout and then proposing it.
     *
     * @param runtime         A corfu runtime.
     * @param transferManager A transfer manager that runs the state transfer.
     * @return A new layout, if a redundancy restoration occurred; a current layout otherwise.
     */
    Layout restore(CorfuRuntime runtime, StateTransferManager transferManager)
            throws InterruptedException {

        LayoutTransferSegments layoutTransferSegments = performStateTransfer(runtime, transferManager);
        BaseClient baseClient = runtime.getLayoutView()
                .getRuntimeLayout(layoutTransferSegments.getLayout())
                .getBaseClient(currentNode);
        mergeSegments(baseClient, runtime.getLayoutManagementView(), layoutTransferSegments);
        runtime.invalidateLayout();
        return runtime.getLayoutView().getLayout();
    }

    /**
     * Perform a state transfer for the current node.
     * Throw an exception if at least one of the segments have failed.
     *
     * @param transferManager Transfer manager.
     * @param currentLayout   Current layout.
     * @param trimMark        Current cluster trim mark.
     * @return List of transferred segments.
     */
    private List<TransferSegment> getTransferSegments(
            StateTransferManager transferManager, Layout currentLayout, long trimMark) {
        // Create a pre transfer state list.
        ImmutableList<TransferSegment> preTransferList =
                redundancyCalculator.createStateList(currentLayout, trimMark);

        // Perform a state transfer for each segment synchronously and update the state list.
        ImmutableList<TransferSegment> transferList = transferManager
                .handleTransfer(preTransferList);

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
        return transferList.stream()
                .filter(segment -> segment.getStatus().getSegmentState() == TRANSFERRED)
                .collect(Collectors.toList());
    }

    /**
     * Sets the trim mark on this endpoint's log unit, performs a prefix trim and then compaction.
     *
     * @param runtime A current runtime.
     * @return A retrieved trim mark.
     */
    private long trimLog(CorfuRuntime runtime) throws TimeoutException {

        long trimMark = runtime.getAddressSpaceView().getTrimMark(true).getSequence();

        Layout layout = runtime.getLayoutView().getLayout();

        Token prefixToken = new Token(layout.getEpoch(), trimMark - 1);

        LogUnitClient logUnitClient = runtime
                .getLayoutView()
                .getRuntimeLayout(layout)
                .getLogUnitClient(currentNode);

        CFUtils.getUninterruptibly(logUnitClient.prefixTrim(prefixToken),
                TimeoutException.class, NetworkException.class, WrongEpochException.class);

        CFUtils.getUninterruptibly(logUnitClient.compact(),
                TimeoutException.class, NetworkException.class, WrongEpochException.class);

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

        // Get the log unit client for the current node.
        LogUnitClient logUnitClient =
                runtime.getLayoutView().getRuntimeLayout(layout).getLogUnitClient(currentNode);

        // Create a chain replication protocol batch processor.
        ProtocolBatchProcessor batchProcessor = ProtocolBatchProcessor
                .builder()
                .addressSpaceView(runtime.getAddressSpaceView())
                .logUnitClient(logUnitClient)
                .build();

        // Create a state transfer manager.
        StateTransferManager transferManager =
                StateTransferManager
                        .builder()
                        .logUnitClient(logUnitClient)
                        .batchSize(runtime.getParameters().getBulkReadSize())
                        .batchProcessor(batchProcessor)
                        .build();

        // While a redundancy can be restored or segments can be merged, perform a state transfer
        // and then restore a layout redundancy on the current node.
        while (RedundancyCalculator.canRestoreRedundancyOrMergeSegments(layout, currentNode)) {
            layout = restore(runtime, transferManager);
        }
        log.info("State transfer on {}: Restored.", currentNode);
    }
}
