package org.corfudb.runtime.clients;


import io.netty.channel.ChannelHandlerContext;

import java.lang.invoke.MethodHandles;
import java.util.UUID;

import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.CorfuProtocolWorkflows;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorResponse;

import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.exceptions.NoBootstrapException;

import org.corfudb.protocols.service.CorfuProtocolManagement;
import org.corfudb.protocols.CorfuProtocolCommon;
import org.corfudb.runtime.proto.Workflows.QueriedWorkflowMsg;
import org.corfudb.runtime.proto.Workflows.CreatedWorkflowMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase;
import org.corfudb.runtime.proto.service.Management.OrchestratorResponseMsg;
import org.corfudb.runtime.proto.service.Management.QueryNodeResponseMsg;
import org.corfudb.runtime.proto.service.Management.ReportFailureResponseMsg;
import org.corfudb.runtime.proto.service.Management.HealFailureResponseMsg;
import org.corfudb.runtime.proto.service.Management.BootstrapManagementResponseMsg;
import org.corfudb.runtime.proto.Common.LayoutMsg;
import org.corfudb.runtime.view.Layout;




/**
 * A client to handle the responses from the Management Server.
 * Handles orchestrator responses as well.
 *
 * <p>Created by zlokhandwala on 2/20/18.
 */
public class ManagementHandler implements IClient, IHandler<ManagementClient> {

    @Setter
    @Getter
    IClientRouter router;

    @Override
    public ManagementClient getClient(long epoch, UUID clusterID) {
        return new ManagementClient(router, epoch, clusterID);
    }

    /**
     * The handler and handlers which implement this client.
     */
    @Getter
    public ClientMsgHandler msgHandler = new ClientMsgHandler(this)
            .generateHandlers(MethodHandles.lookup(), this);

    /**
     * For old CorfuMsg, use {@link #msgHandler}
     * The handler and handlers which implement this client.
     */
    @Getter
    public ClientResponseHandler responseHandler = new ClientResponseHandler(this)
            .generateHandlers(MethodHandles.lookup(), this);


    @ClientHandler(type = CorfuMsgType.ORCHESTRATOR_RESPONSE)
    private static Object handleOrchestratorResponse(CorfuPayloadMsg<OrchestratorResponse> msg,
                                                     ChannelHandlerContext ctx, IClientRouter r) {
        return msg.getPayload();
    }

    @ClientHandler(type = CorfuMsgType.MANAGEMENT_NOBOOTSTRAP_ERROR)
    private static Object handleNoBootstrapError(CorfuMsg msg,
                                                 ChannelHandlerContext ctx, IClientRouter r)
            throws Exception {
        throw new NoBootstrapException();
    }

    @ClientHandler(type = CorfuMsgType.MANAGEMENT_ALREADY_BOOTSTRAP_ERROR)
    private static Object handleAlreadyBootstrappedError(CorfuMsg msg,
                                                         ChannelHandlerContext ctx, IClientRouter r)
            throws Exception {
        throw new AlreadyBootstrappedException();
    }

    @ClientHandler(type = CorfuMsgType.NODE_STATE_RESPONSE)
    private static Object handleNodeStateResponse(CorfuPayloadMsg<NodeState> msg,
                                                  ChannelHandlerContext ctx, IClientRouter r) {
        return msg.getPayload();
    }

    @ClientHandler(type = CorfuMsgType.FAILURE_DETECTOR_METRICS_RESPONSE)
    private static Object handleFailureDetectorMetricsResponse(CorfuPayloadMsg<NodeState> msg,
                                                  ChannelHandlerContext ctx, IClientRouter r) {
        return msg.getPayload();
    }

    // Protobuf region

    /**
     * Handle a query node response from the server.
     *
     * @param msg The query node response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return {@link NodeState} sent back from server.
     */
    @ResponseHandler(type = PayloadCase.QUERY_NODE_RESPONSE)
    private static Object handleQueryNodeResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                  IClientRouter r) {
        QueryNodeResponseMsg responseMsg = msg.getPayload().getQueryNodeResponse();

        return CorfuProtocolManagement.getNodeState(responseMsg);
    }

    /**
     * Handle a report failure response from the server.
     *
     * @param msg The report failure response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return True if ACK, false if NACK.
     */
    @ResponseHandler(type = PayloadCase.REPORT_FAILURE_RESPONSE)
    private static Object handleReportFailureResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                      IClientRouter r) {
        ReportFailureResponseMsg responseMsg = msg.getPayload().getReportFailureResponse();
        ReportFailureResponseMsg.Type type = responseMsg.getRespType();

        switch (type) {
            case ACK:   return true;
            case NACK:  return false;
            // TODO INVALID
            default:    throw new UnsupportedOperationException("Response handler not provided");
        }
    }

    /**
     * Handle a heal failure response from the server.
     *
     * @param msg The heal failure response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return True if ACK, false if NACK.
     */
    @ResponseHandler(type = PayloadCase.HEAL_FAILURE_RESPONSE)
    private static Object handleHealFailureResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                    IClientRouter r) {
        HealFailureResponseMsg responseMsg = msg.getPayload().getHealFailureResponse();
        HealFailureResponseMsg.Type type = responseMsg.getRespType();

        switch (type) {
            case ACK:   return true;
            case NACK:  return false;
            // TODO INVALID
            default:    throw new UnsupportedOperationException("Response handler not provided");
        }
    }

    @ResponseHandler(type = PayloadCase.ORCHESTRATOR_RESPONSE)
    private static Object handleOrchestratorResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                     IClientRouter r) {
        OrchestratorResponseMsg responseMsg = msg.getPayload().getOrchestratorResponse();

        if (responseMsg.getPayloadCase().equals(OrchestratorResponseMsg.PayloadCase.QUERY_RESULT)) {
            QueriedWorkflowMsg qMsg = responseMsg.getQueryResult();
            return CorfuProtocolWorkflows.getQueryResponse(qMsg);
        }

        if (responseMsg.getPayloadCase().equals(OrchestratorResponseMsg.PayloadCase.WORKFLOW_CREATED)) {
            CreatedWorkflowMsg cMsg = responseMsg.getWorkflowCreated();
            return CorfuProtocolWorkflows.getCreateWorkflowResponse(cMsg);
        }

        return null;
    }

    /**
     * Handle a management layout response from the server.
     *
     * @param msg The management layout response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return True if ACK, false if NACK.
     */
    @ResponseHandler(type = PayloadCase.BOOTSTRAP_MANAGEMENT_RESPONSE)
    private static Object handleBootstrapManagementResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                            IClientRouter r) {
        BootstrapManagementResponseMsg responseMsg = msg.getPayload().getBootstrapManagementResponse();
        BootstrapManagementResponseMsg.Type type = responseMsg.getRespType();

        switch (type) {
            case ACK:   return true;
            case NACK:  return false;
            // TODO INVALID
            default:    throw new UnsupportedOperationException("Response handler not provided");
        }
    }

    /**
     * Handle a management layout response from the server.
     *
     * @param msg The management layout response message.
     * @param ctx The context the message was sent under.
     * @param r A reference to the router.
     * @return {@link Layout} sent back from server.
     */
    @ResponseHandler(type = PayloadCase.MANAGEMENT_LAYOUT_RESPONSE)
    private static Object handleManagementLayoutResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                         IClientRouter r) {
        LayoutMsg layoutMsg = msg.getPayload().getManagementLayoutResponse().getLayout();

        return CorfuProtocolCommon.getLayout(layoutMsg);
    }

    // End region
}
