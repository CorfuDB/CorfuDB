package org.corfudb.infrastructure.management;

import java.time.Duration;
import java.util.Set;

import javax.annotation.Nonnull;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.IReconfigurationHandlerPolicy;
import org.corfudb.runtime.view.Layout;

/**
 * The ReconfigurationEventHandler handles the trigger provided by any source
 * or policy detecting a failure or healing in the cluster. It performs the following functions:
 * Cluster recovery:        Recovers the cluster on trigger at startup.
 * Handle failures:         Handles any failures in the layout based on the desired policy.
 * Handle healing:          Handles healing of unresponsive marked nodes which are now responsive.
 * Handle mergeSegments:    Handles merging of multiple segments.
 *
 * <p>Created by zlokhandwala on 11/18/16.
 */
@Slf4j
public class ReconfigurationEventHandler {

    @Getter
    @Setter
    private static int workflowRetries = 3;

    private static final Duration DEFAULT_HEAL_TIMEOUT = Duration.ofMinutes(5);

    private static final double TIMEOUT_MULTIPLICATIVE_FACTOR = 1.5;

    /**
     * Takes in the existing layout and a set of failed nodes.
     * It first generates a new layout by removing the failed nodes from the existing layout.
     * It then seals the epoch to prevent any client from accessing the stale layout.
     * Finally we run paxos to update all servers with the new layout.
     *
     * @param currentLayout The current layout
     * @param corfuRuntime  Connected corfu runtime instance
     * @param failedServers Set of failed server addresses
     */
    public static boolean handleFailure(@Nonnull IReconfigurationHandlerPolicy failureHandlerPolicy,
                                        @Nonnull Layout currentLayout,
                                        @Nonnull CorfuRuntime corfuRuntime,
                                        @Nonnull Set<String> failedServers) {
        try {
            corfuRuntime.getLayoutManagementView().handleFailure(failureHandlerPolicy,
                    currentLayout, failedServers);
            return true;
        } catch (Exception e) {
            log.error("Error: handleFailure: {}", e);
            return false;
        }
    }

    /**
     * Estimate a timeout to complete stateTransfer depending on the number of addresses to be transferred,
     * the RPC timeout of the corfu runtime and the bulk size  of the transfer.
     * TODO: Resize the bulk size in state transfer in case of timeouts or failures.
     *
     * @param runtime Connected corfu runtime instance
     * @return A reasonable timeout to complete state transfer and rebuild logging unit.
     */
    private static Duration getStateTransferTimeoutEstimate(CorfuRuntime runtime) {
        // TODO(xin): this state transfer timeout estimation relies on the fact that the address space is continuous,
        //  whic in not true in the context of sparse-trim garbage collection.
        long tail = runtime.getAddressSpaceView().getLogTail();
        long rangeToReplicate = tail;

        // Since the orchestrator client and the fault detector client use
        // the same configuration its reasonable to use these arguments.
        // TODO(Maithem): AddNode should use a similar mechanism to set the timeout
        long numSections = rangeToReplicate / runtime.getParameters().getBulkReadSize();
        long rpcTimeout
                = (long) (runtime.getParameters().getRequestTimeout().toMillis() * TIMEOUT_MULTIPLICATIVE_FACTOR);
        long timeoutInMs = Math.max(DEFAULT_HEAL_TIMEOUT.toMillis(), numSections * rpcTimeout);
        return Duration.ofMillis(timeoutInMs);
    }

    /**
     * Takes in the existing layout and a set of healed nodes.
     * The LayoutManagementView launches a workflow to heal the specified node. This call is
     * blocked until the workflow is completed or aborted.
     * The node is healed and added back to the layout with all components enabled i.e., with its
     * layout, sequencer and log unit server enabled and as an active participant in the
     * data replication view.
     *
     * @param runtime           Connected corfu runtime instance
     * @param healedServers     Set of healed server addresses
     * @param retryQueryTimeout Timeout to poll for workflow status.
     */
    public static boolean handleHealing(@Nonnull CorfuRuntime runtime,
                                        @Nonnull Set<String> healedServers,
                                        @Nonnull Duration retryQueryTimeout) {
        log.info("Handle healing: {}", healedServers);

        try {
            for (String healedServer : healedServers) {

                Duration workflowTimeout = getStateTransferTimeoutEstimate(runtime);
                log.info("handleHealing: Workflow to heal {} timeout set to {} ms", healedServer, workflowTimeout);

                runtime.getManagementView().healNode(
                        healedServer,
                        workflowRetries,
                        workflowTimeout,
                        retryQueryTimeout);
            }
            return true;
        } catch (Exception e) {
            log.error("Error: handleHealing: {}", e);
            return false;
        }
    }

    /**
     * Launches a workflow to restore redundancy and merge all segments.
     * This method sends an orchestrator request to restore redundancy and merge all segments. This task is run
     * asynchronously by the failure detector if it detects multiple segments in the layout.
     * This is a blocking task and will complete if the workflow completes with a success or a failure.
     * To avoid multiple workflow being submitted by different management agents, the management agents need to
     * deterministically provide the same identifiable endpoint to the orchestrator. In this case, we choose the
     * last added endpoint in the first stripe.
     *
     * @param runtime           Connected corfu runtime instance.
     * @param layout            Latest known layout.
     * @param retryQueryTimeout Timeout to poll for workflow status.
     * @return True if workflow completed successfully. False otherwise.
     */
    public static boolean handleMergeSegments(@Nonnull CorfuRuntime runtime,
                                              @Nonnull Layout layout,
                                              @Nonnull Duration retryQueryTimeout) {

        log.info("Handling split segments");

        try {
            Duration workflowTimeout = getStateTransferTimeoutEstimate(runtime);
            log.info("handleMergeSegments: Workflow to merge segments for layout {} timeout set to {} ms",
                    layout, workflowTimeout);

            // Multiple invocations of this workflow can be avoided if the workflow are mapped to the same endpoint.
            // Thus to deterministically choose an endpoint we always choose the last added log unit in the first
            // stripe.
            runtime.getManagementView().mergeSegments(
                    layout.getLastAddedNodeInLastSegment(),
                    workflowRetries,
                    workflowTimeout,
                    retryQueryTimeout);
            return true;
        } catch (Exception e) {
            log.error("Error: handleMergeSegments: ", e);
            return false;
        }
    }
}