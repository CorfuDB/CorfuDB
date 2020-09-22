package org.corfudb.protocols.service;

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

import java.util.Set;
import java.util.UUID;

import static org.corfudb.protocols.CorfuProtocolCommon.*;
import static org.corfudb.protocols.CorfuProtocolNodeConnectivity.*;
import static org.corfudb.protocols.CorfuProtocolWorkflows.*;

@Slf4j
public class CorfuProtocolManagement {
    public static RequestPayloadMsg getQueryNodeRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setQueryNodeRequest(QueryNodeRequestMsg.getDefaultInstance())
                .build();
    }

    public static ResponsePayloadMsg getQueryNodeResponseMsg(NodeState nodeState) {
        return ResponsePayloadMsg.newBuilder()
                .setQueryNodeResponse(QueryNodeResponseMsg.newBuilder()
                        .setNodeConnectivity(getNodeConnectivityMsg(nodeState.getConnectivity()))
                        //TODO: .setSequencerMetrics()
                        .build())
                .build();
    }

    public static RequestPayloadMsg getReportFailureRequestMsg(long epoch, Set<String> failedNodes) {
        return RequestPayloadMsg.newBuilder()
                .setReportFailureRequest(ReportFailureRequestMsg.newBuilder()
                        .setDetectorEpoch(epoch)
                        .addAllFailedNode(failedNodes)
                        .build())
                .build();
    }

    public static ResponsePayloadMsg getReportFailureResponseMsg(ReportFailureResponseMsg.Type type) {
        return ResponsePayloadMsg.newBuilder()
                .setReportFailureResponse(ReportFailureResponseMsg.newBuilder()
                        .setRespType(type)
                        .build())
                .build();
    }

    public static RequestPayloadMsg getHealFailureRequestMsg(long epoch, Set<String> healedNodes) {
        return RequestPayloadMsg.newBuilder()
                .setHealFailureRequest(HealFailureRequestMsg.newBuilder()
                        .setDetectorEpoch(epoch)
                        .addAllHealedNode(healedNodes)
                        .build())
                .build();
    }

    public static ResponsePayloadMsg getHealFailureResponseMsg(HealFailureResponseMsg.Type type) {
        return ResponsePayloadMsg.newBuilder()
                .setHealFailureResponse(HealFailureResponseMsg.newBuilder()
                        .setRespType(type)
                        .build())
                .build();
    }

    public static RequestPayloadMsg getBootstrapManagementRequestMsg(Layout layout) {
        return RequestPayloadMsg.newBuilder()
                .setBootstrapManagementRequest(
                        BootstrapManagementRequestMsg.newBuilder()
                                .setLayout(getLayoutMsg(layout))
                                .build())
                .build();
    }

    public static ResponsePayloadMsg getBootstrapManagementResponseMsg(BootstrapManagementResponseMsg.Type type) {
        return ResponsePayloadMsg.newBuilder()
                .setBootstrapManagementResponse(
                        BootstrapManagementResponseMsg.newBuilder()
                                .setRespType(type)
                                .build())
                .build();
    }

    public static RequestPayloadMsg getManagementLayoutRequestMsg() {
        return RequestPayloadMsg.newBuilder()
                .setManagementLayoutRequest(ManagementLayoutRequestMsg.getDefaultInstance())
                .build();
    }

    public static ResponsePayloadMsg getManagementLayoutResponseMsg(Layout layout) {
        return ResponsePayloadMsg.newBuilder()
                .setManagementLayoutResponse(
                        ManagementLayoutResponseMsg.newBuilder()
                                .setLayout(getLayoutMsg(layout))
                                .build())
                .build();
    }

    // OrchestratorRequest and OrchestratorResponse API Methods

    public static RequestPayloadMsg getQueryWorkflowRequestMsg(UUID workflowId) {
        return RequestPayloadMsg.newBuilder()
                .setOrchestratorRequest(OrchestratorRequestMsg.newBuilder()
                        .setQuery(getQueryWorkflowMsg(workflowId))
                        .build())
                .build();
    }

    public static RequestPayloadMsg getAddNodeRequestMsg(String endpoint) {
        return RequestPayloadMsg.newBuilder()
                .setOrchestratorRequest(OrchestratorRequestMsg.newBuilder()
                        .setAddNode(getAddNodeWorkflowMsg(endpoint))
                        .build())
                .build();
    }

    public static RequestPayloadMsg getRemoveNodeRequestMsg(String endpoint) {
        return RequestPayloadMsg.newBuilder()
                .setOrchestratorRequest(OrchestratorRequestMsg.newBuilder()
                        .setRemoveNode(getRemoveNodeWorkflowMsg(endpoint))
                        .build())
                .build();
    }

    public static RequestPayloadMsg getHealNodeRequestMsg(String endpoint, int stripeIndex, boolean isLayout,
                                                          boolean isSequencer, boolean isLogUnit) {
        return RequestPayloadMsg.newBuilder()
                .setOrchestratorRequest(OrchestratorRequestMsg.newBuilder()
                        .setHealNode(getHealNodeWorkflowMsg(endpoint, stripeIndex, isLayout, isSequencer, isLogUnit))
                        .build())
                .build();
    }

    public static RequestPayloadMsg getForceRemoveNodeRequestMsg(String endpoint) {
        return RequestPayloadMsg.newBuilder()
                .setOrchestratorRequest(OrchestratorRequestMsg.newBuilder()
                        .setForceRemoveNode(getForceRemoveNodeWorkflowMsg(endpoint))
                        .build())
                .build();
    }

    public static RequestPayloadMsg getRestoreRedundancyMergeSegmentsRequestMsg(String endpoint) {
        return RequestPayloadMsg.newBuilder()
                .setOrchestratorRequest(OrchestratorRequestMsg.newBuilder()
                        .setRestoreRedundancyMergeSegments(getRestoreRedundancyMergeSegmentsWorkflowMsg(endpoint))
                        .build())
                .build();
    }

    public static ResponsePayloadMsg getQueriedWorkflowResponseMsg(boolean active) {
        return ResponsePayloadMsg.newBuilder()
                .setOrchestratorResponse(OrchestratorResponseMsg.newBuilder()
                        .setQueryResult(getQueriedWorkflowMsg(active))
                        .build())
                .build();
    }

    public static ResponsePayloadMsg getCreatedWorkflowResponseMsg(UUID workflowId) {
        return ResponsePayloadMsg.newBuilder()
                .setOrchestratorResponse(OrchestratorResponseMsg.newBuilder()
                        .setWorkflowCreated(getCreatedWorkflowMsg(workflowId))
                        .build())
                .build();
    }
}
