package org.corfudb.runtime.clients;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;

import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.orchestrator.CreateWorkflowResponse;
import org.corfudb.protocols.wireprotocol.orchestrator.QueryResponse;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;

import static org.corfudb.protocols.service.CorfuProtocolManagement.getAddNodeRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getBootstrapManagementRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getForceRemoveNodeRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getHealFailureRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getHealNodeRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getManagementLayoutRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getQueryNodeRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getQueryWorkflowRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getRemoveNodeRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getReportFailureRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getRestoreRedundancyMergeSegmentsRequestMsg;

/**
 * A client to the Management Server.
 *
 * <p>Failure Detection:
 * This client allows a client to trigger failures handlers with relevant failures.
 *
 * <p>Created by zlokhandwala on 11/4/16.
 */
public class ManagementClient extends AbstractClient {

    public ManagementClient(IClientRouter router, long epoch, UUID clusterID) {
        super(router, epoch, clusterID);
    }

    /**
     * Bootstraps a management server.
     *
     * @param l The layout to bootstrap with.
     * @return A completable future which will return TRUE if the
     * bootstrap was successful, false otherwise.
     */
    public CompletableFuture<Boolean> bootstrapManagement(Layout l) {
        return sendRequestWithFuture(getBootstrapManagementRequestMsg(l), ClusterIdCheck.IGNORE, EpochCheck.IGNORE);
    }

    /**
     * Sends the failure detected to the relevant management server.
     *
     * @param detectorEpoch  The epoch in which the polling was conducted
     * @param failedNodes    The failed nodes set to be handled.
     * @return               A future which will return TRUE if completed successfully else returns FALSE.
     */
    public CompletableFuture<Boolean> handleFailure(long detectorEpoch, Set<String> failedNodes) {
        return sendRequestWithFuture(getReportFailureRequestMsg(detectorEpoch, failedNodes), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
    }

    /**
     * Sends the healed nodes detected to the relevant management server.
     *
     * @param detectorEpoch  The epoch in which the polling was conducted
     * @param healedNodes    The healed nodes set to be handled.
     * @return               A future which will be return TRUE if completed successfully else returns FALSE.
     */
    public CompletableFuture<Boolean> handleHealing(long detectorEpoch, Set<String> healedNodes) {
        return sendRequestWithFuture(getHealFailureRequestMsg(detectorEpoch, healedNodes), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
    }

    public CompletableFuture<NodeState> sendNodeStateRequest() {
        return sendRequestWithFuture(getQueryNodeRequestMsg(), ClusterIdCheck.CHECK, EpochCheck.CHECK);
    }

    /**
     * Requests for the layout persisted by the management server.
     *
     * @return A future which returns the layout persisted by the management server on completion.
     */
    public CompletableFuture<Layout> getLayout() {
        return sendRequestWithFuture(getManagementLayoutRequestMsg(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
    }

    /**
     * Send a add node request to an orchestrator
     * @param endpoint the endpoint to add to the cluster
     * @return create workflow response that contains the uuid of the workflow
     * @throws TimeoutException when the rpc times out
     */
    public CreateWorkflowResponse addNodeRequest(@Nonnull String endpoint) throws TimeoutException {
        RequestPayloadMsg payload = getAddNodeRequestMsg(endpoint);

        return CFUtils.getUninterruptibly(
                sendRequestWithFuture(payload, ClusterIdCheck.IGNORE, EpochCheck.IGNORE),
                TimeoutException.class
        );
    }

    /**
     * Creates a workflow request to heal a node.
     *
     * @param endpoint          Endpoint of the node to be healed.
     * @param isLayoutServer    True if the node to be healed is a layout server.
     * @param isSequencerServer True if the node to be healed is a sequencer server.
     * @param isLogUnitServer   True if the node to be healed is a logunit server.
     * @param stripeIndex       Stripe index of the node if it is a logunit server.
     * @return CreateWorkflowResponse which gives the workflowId.
     * @throws TimeoutException when the rpc times out
     */
    public CreateWorkflowResponse healNodeRequest(@Nonnull String endpoint,
                                                  boolean isLayoutServer,
                                                  boolean isSequencerServer,
                                                  boolean isLogUnitServer,
                                                  int stripeIndex) throws TimeoutException {

        RequestPayloadMsg payload = getHealNodeRequestMsg(endpoint,
                stripeIndex, isLayoutServer, isSequencerServer, isLogUnitServer);

        return CFUtils.getUninterruptibly(
                sendRequestWithFuture(payload, ClusterIdCheck.IGNORE, EpochCheck.IGNORE),
                TimeoutException.class
        );
    }

    /**
     * Creates a workflow request to restore all redundancies and merge all segments.
     *
     * @param endpoint Endpoint to restore redundancy.
     * @return CreateWorkflowResponse which gives the workflowId.
     * @throws TimeoutException when the rpc times out
     */
    public CreateWorkflowResponse mergeSegments(@Nonnull String endpoint) throws TimeoutException {
        RequestPayloadMsg payload = getRestoreRedundancyMergeSegmentsRequestMsg(endpoint);

        return CFUtils.getUninterruptibly(
                sendRequestWithFuture(payload, ClusterIdCheck.IGNORE, EpochCheck.IGNORE),
                TimeoutException.class
        );
    }

    /**
     * Query the state of a workflow on a particular orchestrator.
     * @param workflowId the workflow to query
     * @return Query response that contains whether the workflow is running or not
     * @throws TimeoutException when the rpc times out
     */
    public QueryResponse queryRequest(@Nonnull UUID workflowId) throws TimeoutException {
        RequestPayloadMsg payload = getQueryWorkflowRequestMsg(workflowId);

        return CFUtils.getUninterruptibly(
                sendRequestWithFuture(payload, ClusterIdCheck.IGNORE, EpochCheck.IGNORE),
                TimeoutException.class
        );
    }

    /**
     * Return a workflow id for this remove operation request.
     * @param endpoint the endpoint to remove
     * @return uuid of the remove workflow
     * @throws TimeoutException when the rpc times out
     */
    public CreateWorkflowResponse removeNode(@Nonnull String endpoint) throws TimeoutException {
        RequestPayloadMsg payload = getRemoveNodeRequestMsg(endpoint);

        return CFUtils.getUninterruptibly(
                sendRequestWithFuture(payload, ClusterIdCheck.IGNORE, EpochCheck.IGNORE),
                TimeoutException.class
        );
    }

    /**
     *
     * Send a force remove node request to an orchestrator service node.
     *
     * @param endpoint the endpoint to force remove
     * @return CreateWorkflowResponse
     * @throws TimeoutException when the rpc times out
     */
    public CreateWorkflowResponse forceRemoveNode(@Nonnull String endpoint) throws TimeoutException {
        RequestPayloadMsg payload = getForceRemoveNodeRequestMsg(endpoint);

        return CFUtils.getUninterruptibly(
                sendRequestWithFuture(payload, ClusterIdCheck.IGNORE, EpochCheck.IGNORE),
                TimeoutException.class
        );
    }
}
