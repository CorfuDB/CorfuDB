package org.corfudb.protocols.service;

import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg;
import org.corfudb.runtime.proto.service.Management.BootstrapManagementRequestMsg;
import org.corfudb.runtime.proto.service.Management.BootstrapManagementResponseMsg;
import org.corfudb.runtime.proto.service.Management.HealFailureRequestMsg;
import org.corfudb.runtime.proto.service.Management.HealFailureResponseMsg;
import org.corfudb.runtime.proto.service.Management.ManagementLayoutRequestMsg;
import org.corfudb.runtime.proto.service.Management.ManagementLayoutResponseMsg;
import org.corfudb.runtime.proto.service.Management.OrchestratorRequestMsg;
import org.corfudb.runtime.proto.service.Management.OrchestratorResponseMsg;
import org.corfudb.runtime.proto.service.Management.QueryNodeRequestMsg;
import org.corfudb.runtime.proto.service.Management.QueryNodeResponseMsg;
import org.corfudb.runtime.proto.service.Management.ReportFailureRequestMsg;
import org.corfudb.runtime.proto.service.Management.ReportFailureResponseMsg;
import org.corfudb.runtime.view.Layout;

import static org.corfudb.protocols.CorfuProtocolCommon.getLayoutMsg;
import static org.corfudb.protocols.CorfuProtocolCommon.getSequencerMetrics;
import static org.corfudb.protocols.CorfuProtocolCommon.getSequencerMetricsMsg;
import static org.corfudb.protocols.CorfuProtocolNodeConnectivity.getNodeConnectivity;
import static org.corfudb.protocols.CorfuProtocolNodeConnectivity.getNodeConnectivityMsg;
import static org.corfudb.protocols.CorfuProtocolWorkflows.getAddNodeWorkflowMsg;
import static org.corfudb.protocols.CorfuProtocolWorkflows.getCreatedWorkflowMsg;
import static org.corfudb.protocols.CorfuProtocolWorkflows.getForceRemoveNodeWorkflowMsg;
import static org.corfudb.protocols.CorfuProtocolWorkflows.getHealNodeWorkflowMsg;
import static org.corfudb.protocols.CorfuProtocolWorkflows.getQueriedWorkflowMsg;
import static org.corfudb.protocols.CorfuProtocolWorkflows.getQueryWorkflowMsg;
import static org.corfudb.protocols.CorfuProtocolWorkflows.getRemoveNodeWorkflowMsg;
import static org.corfudb.protocols.CorfuProtocolWorkflows.getRestoreRedundancyMergeSegmentsWorkflowMsg;

/**
 * This class provides methods for creating the Protobuf objects defined
 * in management.proto. These provide the interface for performing the RPCs
 * handled by the Management server and Orchestrator.
 */
@Slf4j
public final class CorfuProtocolManagement {
    // Prevent class from being instantiated
    private CorfuProtocolManagement() {}

    /**
     * Returns a QUERY_NODE request message that can be sent by the client.
     *
     * @return   a RequestPayloadMsg containing the QUERY_NODE request
     */
    public static RequestPayloadMsg getQueryNodeRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setQueryNodeRequest(QueryNodeRequestMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a QUERY_NODE response message that can be sent by the server.
     *
     * @param nodeState   the current node state provided by the failure detector
     * @return            a ResponsePayloadMsg containing the QUERY_NODE response
     */
    public static ResponsePayloadMsg getQueryNodeResponseMsg(NodeState nodeState) {
        return ResponsePayloadMsg.newBuilder()
                .setQueryNodeResponse(QueryNodeResponseMsg.newBuilder()
                        .setNodeConnectivity(getNodeConnectivityMsg(nodeState.getConnectivity()))
                        .setSequencerMetrics(getSequencerMetricsMsg(nodeState.getSequencerMetrics()))
                        .build())
                .build();
    }

    /**
     * Returns a NodeState object from its Protobuf representation.
     *
     * @param msg   the desired Protobuf QueryNodeResponse message
     * @return      an equivalent Java NodeState object
     */
    public static NodeState getNodeState(QueryNodeResponseMsg msg) {
        return new NodeState(getNodeConnectivity(msg.getNodeConnectivity()),
                getSequencerMetrics(msg.getSequencerMetrics()));
    }

    /**
     * Returns a REPORT_FAILURE request message that can be sent by the client.
     *
     * @param epoch         the epoch in which the polling was conducted
     * @param failedNodes   the set of failed nodes
     * @return              a RequestPayloadMsg containing the REPORT_FAILURE request
     */
    public static RequestPayloadMsg getReportFailureRequestMsg(long epoch, Set<String> failedNodes) {
        return RequestPayloadMsg.newBuilder()
                .setReportFailureRequest(ReportFailureRequestMsg.newBuilder()
                        .setDetectorEpoch(epoch)
                        .addAllFailedNode(failedNodes)
                        .build())
                .build();
    }

    /**
     * Returns a REPORT_FAILURE response message that can be sent by the server.
     *
     * @param handlingSuccessful   true if the failures were handled successfully
     * @return                     a ResponsePayloadMsg containing the REPORT_FAILURE response
     */
    public static ResponsePayloadMsg getReportFailureResponseMsg(boolean handlingSuccessful) {
        return ResponsePayloadMsg.newBuilder()
                .setReportFailureResponse(ReportFailureResponseMsg.newBuilder()
                        .setHandlingSuccessful(handlingSuccessful)
                        .build())
                .build();
    }

    /**
     * Returns a HEAL_FAILURE request message that can be sent by the client.
     *
     * @param epoch         the epoch in which the polling was conducted
     * @param healedNodes   the set of nodes to try healing
     * @return              a RequestPayloadMsg containing the HEAL_FAILURE request
     */
    public static RequestPayloadMsg getHealFailureRequestMsg(long epoch, Set<String> healedNodes) {
        return RequestPayloadMsg.newBuilder()
                .setHealFailureRequest(HealFailureRequestMsg.newBuilder()
                        .setDetectorEpoch(epoch)
                        .addAllHealedNode(healedNodes)
                        .build())
                .build();
    }

    /**
     * Returns a HEAL_FAILURE response message that can be sent by the server.
     *
     * @param handlingSuccessful   true if the healing was handled successfully
     * @return                     a ResponsePayloadMsg containing the HEAL_FAILURE response
     */
    public static ResponsePayloadMsg getHealFailureResponseMsg(boolean handlingSuccessful) {
        return ResponsePayloadMsg.newBuilder()
                .setHealFailureResponse(HealFailureResponseMsg.newBuilder()
                        .setHandlingSuccessful(handlingSuccessful)
                        .build())
                .build();
    }

    /**
     * Returns a BOOTSTRAP_MANAGEMENT request message that can be sent by the client.
     *
     * @param layout   the Layout to bootstrap with
     * @return         a RequestPayloadMsg containing the BOOTSTRAP_MANAGEMENT request
     */
    public static RequestPayloadMsg getBootstrapManagementRequestMsg(Layout layout) {
        return RequestPayloadMsg.newBuilder()
                .setBootstrapManagementRequest(
                        BootstrapManagementRequestMsg.newBuilder()
                                .setLayout(getLayoutMsg(layout))
                                .build())
                .build();
    }

    /**
     * Returns a BOOTSTRAP_MANAGEMENT response message that can be sent by the server.
     *
     * @param bootstrapped   true if the bootstrap was successful but false otherwise
     * @return               a ResponsePayloadMsg containing the BOOTSTRAP_MANAGEMENT response
     */
    public static ResponsePayloadMsg getBootstrapManagementResponseMsg(boolean bootstrapped) {
        return ResponsePayloadMsg.newBuilder()
                .setBootstrapManagementResponse(
                        BootstrapManagementResponseMsg.newBuilder()
                                .setBootstrapped(bootstrapped)
                                .build())
                .build();
    }

    /**
     * Returns a MANAGEMENT_LAYOUT request message that can be sent by the client.
     *
     * @return   a RequestPayloadMsg containing the MANAGEMENT_LAYOUT request
     */
    public static RequestPayloadMsg getManagementLayoutRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setManagementLayoutRequest(ManagementLayoutRequestMsg.getDefaultInstance())
                .build();
    }

    /**
     * Returns a MANAGEMENT_LAYOUT response message that can be sent by the server.
     *
     * @param layout   the Management Layout
     * @return         a ResponsePayloadMsg containing the MANAGEMENT_LAYOUT response
     */
    public static ResponsePayloadMsg getManagementLayoutResponseMsg(Layout layout) {
        return ResponsePayloadMsg.newBuilder()
                .setManagementLayoutResponse(
                        ManagementLayoutResponseMsg.newBuilder()
                                .setLayout(getLayoutMsg(layout))
                                .build())
                .build();
    }

    // Orchestrator API Methods

    /**
     * Returns an ORCHESTRATOR request message containing a QueryWorkflowMsg that
     * can be sent by the client. Used to query the status of a particular workflow
     * on the Orchestrator.
     *
     * @param workflowId   the id of the workflow being queried
     * @return             a RequestPayloadMsg containing the ORCHESTRATOR request
     */
    public static RequestPayloadMsg getQueryWorkflowRequestMsg(UUID workflowId) {
        return RequestPayloadMsg.newBuilder()
                .setOrchestratorRequest(OrchestratorRequestMsg.newBuilder()
                        .setQuery(getQueryWorkflowMsg(workflowId))
                        .build())
                .build();
    }

    /**
     * Returns an ORCHESTRATOR request message containing an AddNodeWorkflowMsg
     * that can be sent by the client. Used to initiate a workflow that adds a
     * new node to the cluster.
     *
     * @param endpoint   the endpoint to try adding to the cluster
     * @return           a RequestPayloadMsg containing the ORCHESTRATOR request
     */
    public static RequestPayloadMsg getAddNodeRequestMsg(String endpoint) {
        return RequestPayloadMsg.newBuilder()
                .setOrchestratorRequest(OrchestratorRequestMsg.newBuilder()
                        .setAddNode(getAddNodeWorkflowMsg(endpoint))
                        .build())
                .build();
    }

    /**
     * Returns an ORCHESTRATOR request message containing a RemoveNodeWorkflowMsg
     * that can be sent by the client. Used to initiate a workflow that removes a
     * node from the cluster, if it exists.
     *
     * @param endpoint   the endpoint to try removing from the cluster
     * @return           a RequestPayloadMsg containing the ORCHESTRATOR request
     */
    public static RequestPayloadMsg getRemoveNodeRequestMsg(String endpoint) {
        return RequestPayloadMsg.newBuilder()
                .setOrchestratorRequest(OrchestratorRequestMsg.newBuilder()
                        .setRemoveNode(getRemoveNodeWorkflowMsg(endpoint))
                        .build())
                .build();
    }

    /**
     * Returns an ORCHESTRATOR request message containing a HealNodeWorkflowMsg
     * that can be sent by the client. Used to initiate a workflow that heals an
     * existing unresponsive node back into the cluster.
     *
     * @param endpoint      the endpoint to try healing back into the cluster
     * @param stripeIndex   the stripe index of the node if it is a LogUnit server
     * @param isLayout      true if the node is a Layout server
     * @param isSequencer   true if the node is a Sequencer server
     * @param isLogUnit     true if the node is a LogUnit server
     * @return              a RequestPayloadMsg containing the ORCHESTRATOR request
     */
    public static RequestPayloadMsg getHealNodeRequestMsg(String endpoint, int stripeIndex, boolean isLayout,
                                                          boolean isSequencer, boolean isLogUnit) {
        return RequestPayloadMsg.newBuilder()
                .setOrchestratorRequest(OrchestratorRequestMsg.newBuilder()
                        .setHealNode(getHealNodeWorkflowMsg(endpoint, stripeIndex, isLayout, isSequencer, isLogUnit))
                        .build())
                .build();
    }

    /**
     * Returns an ORCHESTRATOR request message containing a ForceRemoveNodeWorkflowMsg
     * that can be sent by the client. Used to initiate a workflow that removes an
     * endpoint from the cluster forcefully by bypassing consensus.
     *
     * @param endpoint   the endpoint to forcefully remove from the cluster
     * @return           a RequestPayloadMsg containing the ORCHESTRATOR request
     */
    public static RequestPayloadMsg getForceRemoveNodeRequestMsg(String endpoint) {
        return RequestPayloadMsg.newBuilder()
                .setOrchestratorRequest(OrchestratorRequestMsg.newBuilder()
                        .setForceRemoveNode(getForceRemoveNodeWorkflowMsg(endpoint))
                        .build())
                .build();
    }

    /**
     * Returns an ORCHESTRATOR request message containing a RestoreRedundancyMergeSegmentsWorkflowMsg
     * that can be sent by the client. Used to initiate a workflow to restore all redundancies and
     * merge all segments.
     *
     * @param endpoint   the endpoint to restore redundancy to
     * @return           a RequestPayloadMsg containing the ORCHESTRATOR request
     */
    public static RequestPayloadMsg getRestoreRedundancyMergeSegmentsRequestMsg(String endpoint) {
        return RequestPayloadMsg.newBuilder()
                .setOrchestratorRequest(OrchestratorRequestMsg.newBuilder()
                        .setRestoreRedundancyMergeSegments(getRestoreRedundancyMergeSegmentsWorkflowMsg(endpoint))
                        .build())
                .build();
    }

    /**
     * Returns an ORCHESTRATOR response message containing a QueriedWorkflowMsg that
     * can be sent by the server. Used to indicate the status of a previously queried
     * workflow.
     *
     * @param active   indicates whether the queried workflow is still active
     * @return         a ResponsePayloadMsg containing the ORCHESTRATOR response
     */
    public static ResponsePayloadMsg getQueriedWorkflowResponseMsg(boolean active) {
        return ResponsePayloadMsg.newBuilder()
                .setOrchestratorResponse(OrchestratorResponseMsg.newBuilder()
                        .setQueryResult(getQueriedWorkflowMsg(active))
                        .build())
                .build();
    }

    /**
     * Returns an ORCHESTRATOR response message containing a CreatedWorkflowMsg that
     * can be sent by the server. Used to indicate to the client that a corresponding
     * workflow was created. The id can be used by the client to query its status.
     *
     * @param workflowId   the id of the newly created workflow
     * @return             a ResponsePayloadMsg containing the ORCHESTRATOR response
     */
    public static ResponsePayloadMsg getCreatedWorkflowResponseMsg(UUID workflowId) {
        return ResponsePayloadMsg.newBuilder()
                .setOrchestratorResponse(OrchestratorResponseMsg.newBuilder()
                        .setWorkflowCreated(getCreatedWorkflowMsg(workflowId))
                        .build())
                .build();
    }
}
