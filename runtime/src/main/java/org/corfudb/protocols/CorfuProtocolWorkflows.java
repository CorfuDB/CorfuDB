package org.corfudb.protocols;

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

    public static RemoveNodeWorkflowMsg getRemoveNodeWorkflowMsg(String endpoint) {
        return RemoveNodeWorkflowMsg.newBuilder()
                .setEndpoint(endpoint)
                .build();
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

    public static ForceRemoveNodeWorkflowMsg getForceRemoveNodeWorkflowMsg(String endpoint) {
        return ForceRemoveNodeWorkflowMsg.newBuilder()
                .setEndpoint(endpoint)
                .build();
    }

    public static RestoreRedundancyMergeSegmentsWorkflowMsg getRestoreRedundancyMergeSegmentsWorkflowMsg(String endpoint) {
        return RestoreRedundancyMergeSegmentsWorkflowMsg.newBuilder()
                .setEndpoint(endpoint)
                .build();
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
