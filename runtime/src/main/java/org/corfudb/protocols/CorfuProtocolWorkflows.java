package org.corfudb.protocols;

import java.util.UUID;
import org.corfudb.protocols.wireprotocol.orchestrator.AddNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.CreateWorkflowResponse;
import org.corfudb.protocols.wireprotocol.orchestrator.ForceRemoveNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.HealNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.QueryResponse;
import org.corfudb.protocols.wireprotocol.orchestrator.RemoveNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.RestoreRedundancyMergeSegmentsRequest;
import org.corfudb.runtime.proto.Workflows.AddNodeWorkflowMsg;
import org.corfudb.runtime.proto.Workflows.CreatedWorkflowMsg;
import org.corfudb.runtime.proto.Workflows.ForceRemoveNodeWorkflowMsg;
import org.corfudb.runtime.proto.Workflows.HealNodeWorkflowMsg;
import org.corfudb.runtime.proto.Workflows.RemoveNodeWorkflowMsg;
import org.corfudb.runtime.proto.Workflows.RestoreRedundancyMergeSegmentsWorkflowMsg;
import org.corfudb.runtime.proto.Workflows.QueriedWorkflowMsg;
import org.corfudb.runtime.proto.Workflows.QueryWorkflowMsg;

import static org.corfudb.protocols.CorfuProtocolCommon.getUUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;

/**
 * This class provides methods for creating the Protobuf objects defined
 * in workflows.proto. These are used by the Management client and server
 * to perform RPCs handled by the Orchestrator.
 */
public final class CorfuProtocolWorkflows {
    // Prevent class from being instantiated
    private CorfuProtocolWorkflows() {}

    /**
     * Returns a Protobuf message used to query the status of a
     * particular workflow on the Orchestrator.
     *
     * @param workflowId   the id of the workflow being queried
     * @return             a QueryWorkflowMsg to be used in an OrchestratorRequestMsg
     */
    public static QueryWorkflowMsg getQueryWorkflowMsg(UUID workflowId) {
        return QueryWorkflowMsg.newBuilder()
                .setWorkflowId(getUuidMsg(workflowId))
                .build();
    }

    /**
     * Returns a Protobuf message used to indicate the status of
     * a previously queried workflow.
     *
     * @param active   indicates whether the queried workflow is still active
     * @return         a QueriedWorkflowMsg to be used in an OrchestratorResponseMsg
     */
    public static QueriedWorkflowMsg getQueriedWorkflowMsg(boolean active) {
        return QueriedWorkflowMsg.newBuilder()
                .setActive(active)
                .build();
    }

    /**
     * Returns a QueryResponse object from its Protobuf representation.
     *
     * @param msg   the desired Protobuf QueriedWorkflow message
     * @return      an equivalent Java QueryResponse object
     */
    public static QueryResponse getQueryResponse(QueriedWorkflowMsg msg) {
        return new QueryResponse(msg.getActive());
    }

    /**
     * Returns a Protobuf message used to initiate a workflow that adds
     * a new node to the cluster.
     *
     * @param endpoint   the endpoint to try adding to the cluster
     * @return           an AddNodeWorkflowMsg to be used in an OrchestratorRequestMsg
     */
    public static AddNodeWorkflowMsg getAddNodeWorkflowMsg(String endpoint) {
        return AddNodeWorkflowMsg.newBuilder()
                .setEndpoint(endpoint)
                .build();
    }

    /**
     * Returns an AddNodeRequest object from its Protobuf representation.
     * Used by the Orchestrator.
     *
     * @param msg   the desired Protobuf AddNodeWorkflow message
     * @return      an equivalent Java AddNodeRequest object
     */
    public static AddNodeRequest getAddNodeRequest(AddNodeWorkflowMsg msg) {
        return new AddNodeRequest(msg.getEndpoint());
    }

    /**
     * Returns a Protobuf message used to initiate a workflow that removes
     * a node from the cluster, if it exists.
     *
     * @param endpoint   the endpoint to try removing from the cluster
     * @return           a RemoveNodeWorkflowMsg to be used in an OrchestratorRequestMsg
     */
    public static RemoveNodeWorkflowMsg getRemoveNodeWorkflowMsg(String endpoint) {
        return RemoveNodeWorkflowMsg.newBuilder()
                .setEndpoint(endpoint)
                .build();
    }

    /**
     * Returns a RemoveNodeRequest object from its Protobuf representation.
     * Used by the Orchestrator.
     *
     * @param msg   the desired Protobuf RemoveNodeWorkflow message
     * @return      an equivalent Java RemoveNodeRequest object
     */
    public static RemoveNodeRequest getRemoveNodeRequest(RemoveNodeWorkflowMsg msg)  {
        return new RemoveNodeRequest(msg.getEndpoint());
    }

    /**
     * Returns a Protobuf message used to initiate a workflow that heals an
     * existing unresponsive node back into the cluster.
     *
     * @param endpoint      the endpoint to try healing back into the cluster
     * @param stripeIndex   the stripe index of the node if it is a LogUnit server
     * @param isLayout      true if the node is a Layout server
     * @param isSequencer   true if the node is a Sequencer server
     * @param isLogUnit     true if the node is a LogUnit server
     * @return              a HealNodeWorkflowMsg to be used in an OrchestratorRequestMsg
     */
    public static HealNodeWorkflowMsg getHealNodeWorkflowMsg(String endpoint, int stripeIndex,
                                                             boolean isLayout, boolean isSequencer, boolean isLogUnit) {
        return HealNodeWorkflowMsg.newBuilder()
                .setEndpoint(endpoint)
                .setStripeIndex(stripeIndex)
                .setLayoutServer(isLayout)
                .setLogUnitServer(isLogUnit)
                .setSequencerServer(isSequencer)
                .build();
    }

    /**
     * Returns a HealNodeRequest object from its Protobuf representation.
     * Used by the Orchestrator.
     *
     * @param msg   the desired Protobuf HealNodeWorkflow message
     * @return      an equivalent Java HealNodeRequest object
     */
    public static HealNodeRequest getHealNodeRequest(HealNodeWorkflowMsg msg) {
        return new HealNodeRequest(msg.getEndpoint(),
                msg.getLayoutServer(),
                msg.getSequencerServer(),
                msg.getLogUnitServer(),
                msg.getStripeIndex());
    }

    /**
     * Returns a Protobuf message used to initiate a workflow that removes an
     * endpoint from the cluster forcefully by bypassing consensus.
     *
     * @param endpoint   the endpoint to forcefully remove from the cluster
     * @return           a ForceRemoveNodeWorkflowMsg to be used in an OrchestratorRequestMsg
     */
    public static ForceRemoveNodeWorkflowMsg getForceRemoveNodeWorkflowMsg(String endpoint) {
        return ForceRemoveNodeWorkflowMsg.newBuilder()
                .setEndpoint(endpoint)
                .build();
    }

    /**
     * Returns a ForceRemoveNodeRequest object from its Protobuf
     * representation. Used by the Orchestrator.
     *
     * @param msg   the desired Protobuf ForceRemoveNodeWorkflow message
     * @return      an equivalent Java ForceRemoveNodeRequest object
     */
    public static ForceRemoveNodeRequest getForceRemoveNodeRequest(ForceRemoveNodeWorkflowMsg msg) {
        return new ForceRemoveNodeRequest(msg.getEndpoint());
    }

    /**
     * Returns a Protobuf message used to initiate a workflow that to
     * restore all redundancies and merge all segments.
     *
     * @param endpoint   the endpoint to restore redundancy to
     * @return           a RestoreRedundancyMergeSegmentsWorkflowMsg to be used
     *                   in an OrchestratorRequestMsg
     */
    public static RestoreRedundancyMergeSegmentsWorkflowMsg getRestoreRedundancyMergeSegmentsWorkflowMsg(String endpoint) {
        return RestoreRedundancyMergeSegmentsWorkflowMsg.newBuilder()
                .setEndpoint(endpoint)
                .build();
    }

    /**
     * Returns a RestoreRedundancyMergeSegmentsRequest object from its
     * Protobuf representation. Used by the Orchestrator.
     *
     * @param msg   the desired Protobuf RestoreRedundancyMergeSegmentsWorkflow message
     * @return      an equivalent Java RestoreRedundancyMergeSegmentsRequest object
     */
    public static RestoreRedundancyMergeSegmentsRequest getRestoreRedundancyMergeSegmentsRequest(
            RestoreRedundancyMergeSegmentsWorkflowMsg msg) {
        return new RestoreRedundancyMergeSegmentsRequest(msg.getEndpoint());
    }

    /**
     * Returns a Protobuf message used to indicate to the client that
     * a corresponding workflow was created. The id can be used by the
     * client to query its status.
     *
     * @param workflowId   the id of the newly created workflow
     * @return             a CreatedWorkflowMsg to be used in an OrchestratorResponseMsg
     */
    public static CreatedWorkflowMsg getCreatedWorkflowMsg(UUID workflowId) {
        return CreatedWorkflowMsg.newBuilder()
                .setWorkflowId(getUuidMsg(workflowId))
                .build();
    }

    /**
     * Returns a CreateWorkflowResponse object from its Protobuf representation.
     *
     * @param msg   the desired Protobuf CreatedWorkflow message
     * @return      an equivalent Java CreateWorkflowResponse object
     */
    public static CreateWorkflowResponse getCreateWorkflowResponse(CreatedWorkflowMsg msg) {
        return new CreateWorkflowResponse(getUUID(msg.getWorkflowId()));
    }
}
