package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.committedbatchprocessor.CommittedBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.batchprocessor.protocolbatchprocessor.ProtocolBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.exceptions.TransferSegmentException;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegment;
import org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentRange;
import org.corfudb.infrastructure.log.statetransfer.transferprocessor.BasicTransferProcessor;
import org.corfudb.infrastructure.log.statetransfer.transferprocessor.ParallelTransferProcessor;
import org.corfudb.infrastructure.orchestrator.Action;
import org.corfudb.infrastructure.redundancy.RedundancyCalculator;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.LayoutBuilder;
import org.corfudb.runtime.view.LayoutManagementView;
import org.corfudb.runtime.view.LayoutView;
import org.corfudb.runtime.view.RuntimeLayout;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Utils;
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
import java.util.stream.Stream;

import static org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentStatus.SegmentState.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.segment.TransferSegmentStatus.SegmentState.TRANSFERRED;

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

    /**
     * A data class that stores both the layout and the transferred segments.
     */
    @AllArgsConstructor
    private static class LayoutTransferSegments {
        @Getter
        @NonNull
        private final Layout layout;

        @Getter
        @NonNull
        private final ImmutableList<TransferSegment> transferSegments;
    }

    /**
     * Perform state transfer and return all the transferred segments along with the latest layout.
     *
     * @param runtime         Current runtime.
     * @param transferManager Transfer manager instance to perform a state transfer.
     * @return Current layout and the list of transferred segments.
     */
    private LayoutTransferSegments performStateTransfer(CorfuRuntime runtime,
                                                        StateTransferManager transferManager)
            throws InterruptedException {
        // Settings for the retries
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
                if (currentLayout.getUnresponsiveServers().contains(currentNode)) {
                    throw new IllegalStateException("Node is in the unresponsive list.");
                }
                log.info("State transfer on {}: Layout before transfer: {}",
                        currentNode, currentLayout);
                // Retrieve a current cluster trim mark.
                long trimMark = trimLog(runtime, currentLayout);

                // Retrieve a current cluster committed tail.
                Optional<Long> committedTail =
                        tryGetCommittedTail(runtime.getLayoutView().getRuntimeLayout());

                // Execute state transfer and return the list of transferred transfer segments.
                ImmutableList<TransferSegment> transferredSegments = ImmutableList.copyOf(
                        getTransferSegments(transferManager, currentLayout,
                                trimMark, committedTail));

                // Get segments that were trimmed but do not include the current node, if any.
                ImmutableList<TransferSegment> trimmedNotRestoredSegments =
                        redundancyCalculator.getTrimmedNotRestoredSegments(currentLayout, trimMark);

                ImmutableList<TransferSegment> segmentsToGetRestored =
                        Stream.concat(trimmedNotRestoredSegments.stream(), transferredSegments.stream())
                                .collect(ImmutableList.toImmutableList());
                // Get all the restorable segments as well as the current layout.
                LayoutTransferSegments layoutTransferSegments =
                        new LayoutTransferSegments(currentLayout, segmentsToGetRestored);

                // Transfer a new committed tail after all segments are transferred.
                // The new committed tail is the last transferred address.
                layoutTransferSegments.getTransferSegments()
                        .stream()
                        .map(TransferSegment::getEndAddress)
                        .max(Long::compare)
                        .ifPresent(addr -> transferCommittedTail(runtime, currentLayout, addr));

                return layoutTransferSegments;
            } catch (IllegalStateException e) {
                throw new RetryExhaustedException(e);
            } catch (RuntimeException re) {
                log.warn("performStateTransfer: Retrying: {} times after exception: {}.",
                        retries.get(), re);
                if (retries.decrementAndGet() < 0) {
                    throw new RetryExhaustedException("Retries exhausted.", re);
                } else {
                    throw new RetryNeededException();
                }
            }
        }).setOptions(retrySettings).run();
    }

    /**
     * Merge the state transfer segments and propose the new cluster layout.
     *
     * @param layoutView             Layout view.
     * @param layoutManagementView   Layout management view.
     * @param layoutTransferSegments Layout and the transferred segments.
     * @throws OutrankedException If the layout was outranked.
     */
    private void mergeSegments(LayoutView layoutView, LayoutManagementView layoutManagementView,
                               LayoutTransferSegments layoutTransferSegments)
            throws OutrankedException {
        Layout currentLayout = layoutTransferSegments.getLayout();
        ImmutableList<TransferSegment> transferredSegments =
                layoutTransferSegments.getTransferSegments();

        // Seal the next epoch for the current server.
        // This is done in order to prevent the unnecessary OutRankedExceptions during the
        // layout reconfiguration that can happen if the min server set is sealed but the current node
        // was not part of this set.
        BaseClient baseClient = layoutView
                .getRuntimeLayout(layoutTransferSegments.getLayout())
                .getBaseClient(currentNode);
        CFUtils.getUninterruptibly(
                baseClient.sealRemoteServer(currentLayout.getEpoch() + 1));

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
    private Layout restore(CorfuRuntime runtime, StateTransferManager transferManager)
            throws InterruptedException, OutrankedException {

        LayoutTransferSegments layoutTransferSegments = performStateTransfer(runtime, transferManager);
        mergeSegments(runtime.getLayoutView(), runtime.getLayoutManagementView(), layoutTransferSegments);
        runtime.invalidateLayout();
        return runtime.getLayoutView().getLayout();
    }


    /**
     * Perform a state transfer for the current node.
     * Return all the segments that are transferred.
     * Throw an exception if at least one of the segments have failed.
     *
     * @param transferManager Transfer manager.
     * @param currentLayout   Current layout.
     * @param trimMark        Current cluster trim mark.
     * @param committedTail   An optional cluster committed tail.
     * @return List of transferred segments.
     */
    private List<TransferSegment> getTransferSegments(
            StateTransferManager transferManager, Layout currentLayout, long trimMark,
            Optional<Long> committedTail) {
        // Create a pre transfer segment list.
        ImmutableList<TransferSegment> preTransferList =
                redundancyCalculator.createStateList(currentLayout, trimMark);

        log.info("getTransferSegments: pre-transfer list: {}", preTransferList);
        // Create a transfer workload. This will turn the list of segments into the list of
        // ranges. The reason for this is the fact that a present committed tail can split the segment
        // into the committed and non-committed halves that should be handled differently
        // during a state transfer.
        ImmutableList<TransferSegmentRange> transferSegmentRanges = redundancyCalculator
                .prepareTransferWorkload(preTransferList, committedTail);

        log.info("getTransferSegments: transfer workload: {}", transferSegmentRanges);

        // Perform a state transfer on the transfer segment ranges and return the updated
        // transferred transfer segment list.
        ImmutableList<TransferSegment> transferList = transferManager
                .handleTransfer(transferSegmentRanges);

        log.info("getTransferSegments: Transfer segments after transfer: {}", transferList);
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
     * Tries to retrieve a committed tail. If this fails, returns an empty option.
     * State transfer will still finish but all the addresses
     * will be transferred via replication protocol.
     *
     * @param runtimeLayout Corfu runtimeLayout
     * @return Maybe a committed tail.
     */
    private Optional<Long> tryGetCommittedTail(RuntimeLayout runtimeLayout) {
        for (int i = 0; i < retryBase; i++) {
            try {
                long committedTail = Utils.getCommittedTail(runtimeLayout);
                log.debug("tryGetCommittedTail: retrieved a committed tail: {}.", committedTail);
                return Optional.of(committedTail);
            } catch (NetworkException ne) {
                log.warn("tryGetCommittedTail: encountered a network exception. Retry {}/{}",
                        i, retryBase, ne);
            } catch (WrongEpochException wee) {
                log.error("tryGetCommittedTail: encountered a wrong epoch exception." +
                        "Retry the entire workflow.", wee);
                throw wee;
            } catch (RuntimeException re) {
                if (re.getCause() instanceof TimeoutException) {
                    log.warn("tryGetCommittedTail: encountered a time out exception. Retry {}/{}",
                            i, retryBase, re);
                } else {
                    throw new IllegalStateException("Encountered an unexpected exception " +
                            "when trying to get the committed tail.", re);
                }
            }
        }
        log.warn("tryGetCommittedTail: failed to retrieve the committed tail after the retries. " +
                "State transfer will proceed via replication protocol.");
        return Optional.empty();
    }

    /**
     * Sets the trim mark on this endpoint's log unit, performs a prefix trim and then compaction.
     * If the exceptions are: NetworkException, WrongEpochException or TimeoutException, propagate
     * to the caller. If it's some other exception, wrap it into IllegalStateException and propagate
     * to the caller.
     *
     * @param runtime A current runtime.
     * @param layout  A current layout.
     * @return A retrieved trim mark.
     */
    private long trimLog(CorfuRuntime runtime, Layout layout) {
        RuntimeLayout runtimeLayout = runtime.getLayoutView().getRuntimeLayout(layout);

        for (int i = 0; i < retryBase; i++) {
            try {
                long trimMark = Utils.getTrimMark(runtimeLayout).getSequence();

                Token prefixToken = new Token(layout.getEpoch(), trimMark - 1);

                LogUnitClient logUnitClient = runtime
                        .getLayoutView()
                        .getRuntimeLayout(layout)
                        .getLogUnitClient(currentNode);

                CFUtils.getUninterruptibly(logUnitClient.prefixTrim(prefixToken));

                CFUtils.getUninterruptibly(logUnitClient.compact());

                log.debug("trimLog: retrieved a trim mark: {}", trimMark);

                return trimMark;
            } catch (NetworkException ne) {
                log.warn("trimLog: encountered a NetworkException, retrying: ", ne);
            }
            catch (WrongEpochException wee) {
                log.error("trimLog: encountered a WrongEpochException: ", wee);
                throw wee;
            }
            catch (RuntimeException re) {
                if (re.getCause() instanceof TimeoutException) {
                    log.warn("trimLog: encountered a TimeoutException, retrying: ", re);
                }
                else {
                    throw new IllegalStateException("Encountered an unexpected exception " +
                            "when trimming the log.", re);
                }
            }
        }
        throw new RetryExhaustedException("Exhausted retries while trying to trim the log.");
    }

    /**
     * Send the last transferred address as the committed tail to the target log unit.
     * This is required to prevent loss of committed tail after state transfer finishes
     * and then all other log units failed and auto commit service is paused.
     */
    private void transferCommittedTail(CorfuRuntime runtime, Layout layout, long committedTail) {
        LogUnitClient logUnitClient = runtime.getLayoutView()
                .getRuntimeLayout(layout)
                .getLogUnitClient(currentNode);

        for (int i = 0; i < retryBase; i++) {
            try {
                CFUtils.getUninterruptibly(logUnitClient.updateCommittedTail(committedTail),
                        TimeoutException.class, NetworkException.class);
                log.debug("transferCommittedTail: transferred committed tail {}", committedTail);
                return;
            } catch (TimeoutException | NetworkException e) {
                log.error("transferCommittedTail: encountered network issue " +
                        "when transferring a new committed tail.");
            }
        }
        log.warn("transferCommittedTail: failed to transfer the latest committed tail of {}.",
                committedTail);
    }

    @Nonnull
    @Override
    public String getName() {
        return "RestoreRedundancyAndMergeSegments";
    }

    @Override
    public void impl(@Nonnull CorfuRuntime runtime) throws Exception {

        // Retrieve a layout.
        Layout layout = runtime.getLayoutView().getLayout();

        if (layout.getUnresponsiveServers().contains(currentNode)) {
            throw new IllegalStateException("The node is in the unresponsive list.");
        }
        // Get the log unit client for the current node
        LogUnitClient logUnitClient =
                runtime.getLayoutView().getRuntimeLayout(layout).getLogUnitClient(currentNode);

        // Create a chain replication protocol batch processor
        ProtocolBatchProcessor protocolBatchProcessor = ProtocolBatchProcessor
                .builder()
                .addressSpaceView(runtime.getAddressSpaceView())
                .logUnitClient(logUnitClient)
                .build();

        // Create a basic transfer processor for the chain replication transfer
        BasicTransferProcessor basicTransferProcessor =
                new BasicTransferProcessor(protocolBatchProcessor);

        // Create a committed batch processor
        CommittedBatchProcessor committedBatchProcessor = CommittedBatchProcessor
                .builder()
                .currentNode(currentNode)
                .runtimeLayout(runtime.getLayoutView().getRuntimeLayout(layout))
                .build();

        // Create a parallel transfer processor for the committed transfer
        ParallelTransferProcessor parallelTransferProcessor =
                new ParallelTransferProcessor(committedBatchProcessor);

        // Create a state transfer manager
        StateTransferManager transferManager =
                StateTransferManager
                        .builder()
                        .logUnitClient(logUnitClient)
                        .batchSize(runtime.getParameters().getBulkReadSize())
                        .basicTransferProcessor(basicTransferProcessor)
                        .parallelTransferProcessor(parallelTransferProcessor)
                        .build();

        // While a redundancy can be restored or segments can be merged, perform a state transfer
        // and then restore a layout redundancy on the current node.
        while (RedundancyCalculator.canRestoreRedundancyOrMergeSegments(layout, currentNode)) {
            layout = restore(runtime, transferManager);
        }
        log.info("State transfer on {}: Restored.", currentNode);
    }
}
