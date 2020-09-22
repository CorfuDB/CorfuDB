package org.corfudb.protocols;

import org.corfudb.protocols.wireprotocol.orchestrator.AddNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.ForceRemoveNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.HealNodeRequest;
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

import java.util.UUID;

import static org.corfudb.protocols.CorfuProtocolCommon.*;

public class CorfuProtocolWorkflows {
    public static QueryWorkflowMsg getQueryWorkflowMsg(UUID workflowId) {
        return QueryWorkflowMsg.newBuilder()
                .setWorkflowId(getUuidMsg(workflowId))
                .build();
    }

    public static AddNodeWorkflowMsg getAddNodeWorkflowMsg(String endpoint) {
        return AddNodeWorkflowMsg.newBuilder()
                .setEndpoint(endpoint)
                .build();
    }

    public static AddNodeRequest getAddNodeRequest(AddNodeWorkflowMsg msg) {
        return new AddNodeRequest(msg.getEndpoint());
    }

    public static RemoveNodeWorkflowMsg getRemoveNodeWorkflowMsg(String endpoint) {
        return RemoveNodeWorkflowMsg.newBuilder()
                .setEndpoint(endpoint)
                .build();
    }

    public static RemoveNodeRequest getRemoveNodeRequest(RemoveNodeWorkflowMsg msg)  {
        return new RemoveNodeRequest(msg.getEndpoint());
    }

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

    public static HealNodeRequest getHealNodeRequest(HealNodeWorkflowMsg msg) {
        return new HealNodeRequest(msg.getEndpoint(),
                msg.getLayoutServer(),
                msg.getSequencerServer(),
                msg.getLogUnitServer(),
                msg.getStripeIndex());
    }

    public static ForceRemoveNodeWorkflowMsg getForceRemoveNodeWorkflowMsg(String endpoint) {
        return ForceRemoveNodeWorkflowMsg.newBuilder()
                .setEndpoint(endpoint)
                .build();
    }

    public static ForceRemoveNodeRequest getForceRemoveNodeRequest(ForceRemoveNodeWorkflowMsg msg) {
        return new ForceRemoveNodeRequest(msg.getEndpoint());
    }

    public static RestoreRedundancyMergeSegmentsWorkflowMsg getRestoreRedundancyMergeSegmentsWorkflowMsg(String endpoint) {
        return RestoreRedundancyMergeSegmentsWorkflowMsg.newBuilder()
                .setEndpoint(endpoint)
                .build();
    }

    public static RestoreRedundancyMergeSegmentsRequest getRestoreRedundancyMergeSegmentsRequest(
            RestoreRedundancyMergeSegmentsWorkflowMsg msg) {
        return new RestoreRedundancyMergeSegmentsRequest(msg.getEndpoint());
    }

    public static QueriedWorkflowMsg getQueriedWorkflowMsg(boolean active) {
        return QueriedWorkflowMsg.newBuilder()
                .setActive(active)
                .build();
    }

    public static CreatedWorkflowMsg getCreatedWorkflowMsg(UUID workflowId) {
        return CreatedWorkflowMsg.newBuilder()
                .setWorkflowId(getUuidMsg(workflowId))
                .build();
    }
}
