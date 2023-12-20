package org.corfudb.infrastructure.management.failuredetector;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.RemoteMonitoringService.DetectorTask;
import org.corfudb.infrastructure.management.ClusterStateContext;
import org.corfudb.infrastructure.management.PollReport;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.concurrent.SingletonResource;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

@Builder
@Slf4j
public class FailureDetectorService {
    @NonNull
    private final EpochHandler epochHandler;
    @NonNull
    private final FailuresAgent failuresAgent;
    @NonNull
    private final HealingAgent healingAgent;
    @NonNull
    private final SequencerBootstrapper sequencerBootstrapper;
    @NonNull
    private final ExecutorService failureDetectorWorker;


    /**
     * <pre>
     * Failure/Healing detector task. Detects faults in the cluster and heals servers also corrects wrong epochs:
     *  - correct wrong epochs by resealing the servers and updating trailing layout servers.
     *    Fetch quorum layout and update the epoch according to the quorum.
     *  - check if current layout slot is unfilled then update layout to latest one.
     *  - handle healed nodes.
     *  - Looking for link failures in the cluster, handle failure if found.
     *  - Restore redundancy and merge segments if present in the layout.
     *  - bootstrap sequencer if needed
     * </pre>
     *
     * @param pollReport cluster status
     * @return async detection task
     */
    public CompletableFuture<DetectorTask> runFailureDetectorTask(PollReport pollReport, Layout ourLayout, String endpoint) {

        // Corrects out of phase epoch issues if present in the report. This method
        // performs re-sealing of all nodes if required and catchup of a layout server to
        // the current state.
        CompletableFuture<Layout> wrongEpochTask = epochHandler.correctWrongEpochs(pollReport, ourLayout);

        return wrongEpochTask.thenApplyAsync(latestLayout -> {

            // This is just an optimization in case we receive a WrongEpochException
            // while one of the other management clients is trying to move to a new layout.
            // This check is merely trying to minimize the scenario in which we end up
            // filling the slot with an outdated layout.
            if (!pollReport.areAllResponsiveServersSealed()) {
                log.debug("All responsive servers have not been sealed yet. Skipping.");
                return DetectorTask.COMPLETED;
            }

            Result<DetectorTask, RuntimeException> failure = Result.of(() -> {

                Optional<Long> unfilledSlot = pollReport.getLayoutSlotUnFilled(latestLayout);
                // If the latest slot has not been filled, fill it with the previous known layout.
                if (unfilledSlot.isPresent()) {
                    log.info("Trying to fill an unfilled slot {}. PollReport: {}",
                            unfilledSlot.get(), pollReport);
                    failuresAgent.handleFailure(latestLayout, Collections.emptySet(), pollReport, endpoint).join();
                    return DetectorTask.COMPLETED;
                }

                return DetectorTask.NOT_COMPLETED;
            });

            if (!pollReport.getWrongEpochs().isEmpty()) {
                log.debug("Wait for next iteration. Poll report contains wrong epochs: {}",
                        pollReport.getWrongEpochs()
                );
                return DetectorTask.COMPLETED;
            }

            // If layout was updated by correcting wrong epochs,
            // we can't continue with failure detection,
            // as the cluster state has changed.
            if (!latestLayout.equals(ourLayout)) {
                log.warn("Layout was updated by correcting wrong epochs. " +
                        "Cancel current round of failure detection.");
                return DetectorTask.COMPLETED;
            }

            failure.ifError(err -> log.error("Can't fill slot. Poll report: {}", pollReport, err));

            if (failure.isValue() && failure.get() == DetectorTask.COMPLETED) {
                return DetectorTask.COMPLETED;
            }

            DetectorTask healing = healingAgent.detectAndHandleHealing(pollReport, ourLayout, endpoint).join();

            //If local node healed it causes change in the cluster state which means the layout is changed also.
            //If the cluster status is changed let failure detector detect the change on next iteration and
            //behave according to latest cluster state.
            if (healing == DetectorTask.COMPLETED) {
                return DetectorTask.COMPLETED;
            }

            // Analyze the poll report and trigger failure handler if needed.
            DetectorTask handleFailure = failuresAgent.detectAndHandleFailure(pollReport, ourLayout, endpoint);

            //If a failure is detected (which means we have updated a layout)
            // then don't try to heal anything, wait for next iteration.
            if (handleFailure == DetectorTask.COMPLETED) {
                return DetectorTask.COMPLETED;
            }

            // Restores redundancy and merges multiple segments if present.
            healingAgent.restoreRedundancyAndMergeSegments(ourLayout, endpoint);

            sequencerBootstrapper.handleSequencer(ourLayout);

            return DetectorTask.COMPLETED;

        }, failureDetectorWorker);
    }

    /**
     * This tuple maintains, in an epoch, how many heartbeats the primary sequencer has responded
     * in not bootstrapped (NOT_READY) state.
     */
    @Getter
    @Setter
    @AllArgsConstructor
    private static class SequencerNotReadyCounter {
        private final long epoch;
        private int counter;
    }

    @Builder
    public static class SequencerBootstrapper {
        private static final CompletableFuture<DetectorTask> DETECTOR_TASK_SKIPPED
                = CompletableFuture.completedFuture(DetectorTask.SKIPPED);

        @NonNull
        private final ClusterStateContext clusterContext;
        @NonNull
        @Default
        private volatile SequencerNotReadyCounter sequencerNotReadyCounter = new SequencerNotReadyCounter(0, 0);
        @NonNull
        private final SingletonResource<CorfuRuntime> runtimeSingletonResource;
        @NonNull
        private final ExecutorService failureDetectorWorker;


        /**
         * Checks sequencer state, triggers a new task to bootstrap the sequencer for the specified layout (if needed).
         *
         * @param layout current layout
         */
        public CompletableFuture<DetectorTask> handleSequencer(Layout layout) {
            log.trace("Handling sequencer failures");

            ClusterState clusterState = clusterContext.getClusterView();
            Optional<NodeState> primarySequencer = clusterState.getNode(layout.getPrimarySequencer());
            if (primarySequencer.isPresent() && primarySequencer.get().getSequencerMetrics() == SequencerMetrics.READY) {
                log.trace("Primary sequencer is already ready at: {} in {}", primarySequencer.get(), clusterState);
                return DETECTOR_TASK_SKIPPED;
            }

            // If failures are not present we can check if the primary sequencer has been
            // bootstrapped from the heartbeat responses received.
            if (sequencerNotReadyCounter.getEpoch() != layout.getEpoch()) {
                // If the epoch is different from the poll epoch, we reset the timeout state.
                log.trace("Current epoch is different to layout epoch. Update current epoch to: {}", layout.getEpoch());
                sequencerNotReadyCounter = new SequencerNotReadyCounter(layout.getEpoch(), 1);
                return DETECTOR_TASK_SKIPPED;
            }

            // Launch task to bootstrap the primary sequencer.
            log.trace("Attempting to bootstrap the primary sequencer. ClusterState {}", clusterState);
            // We do not care about the result of the trigger.
            // If it fails, we detect this again and retry in the next polling cycle.
            return runtimeSingletonResource.get()
                    .getLayoutManagementView()
                    .asyncSequencerBootstrap(layout, failureDetectorWorker)
                    .thenApply(DetectorTask::fromBool);
        }
    }
}
