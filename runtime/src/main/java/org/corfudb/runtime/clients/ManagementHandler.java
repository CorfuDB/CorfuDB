package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;
import java.lang.invoke.MethodHandles;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase;
import org.corfudb.runtime.proto.service.Management.OrchestratorResponseMsg;

import static org.corfudb.protocols.CorfuProtocolCommon.getLayout;
import static org.corfudb.protocols.CorfuProtocolWorkflows.getCreateWorkflowResponse;
import static org.corfudb.protocols.CorfuProtocolWorkflows.getQueryResponse;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getNodeState;

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
     * The response handlers and error handlers which implement
     * this client.
     */
    @Getter
    public ClientResponseHandler responseHandler = new ClientResponseHandler(this)
            .generateHandlers(MethodHandles.lookup(), this)
            .generateErrorHandlers(MethodHandles.lookup(), this);

    /**
     * Handle an ORCHESTRATOR response from the server.
     *
     * @param msg  An orchestrator response message.
     * @param ctx  The context the message was sent under.
     * @param r    A reference to the router.
     * @return     The orchestrator response.
     */
    @ResponseHandler(type = PayloadCase.ORCHESTRATOR_RESPONSE)
    private static Object handleOrchestratorMsg(ResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        OrchestratorResponseMsg orchestratorResponse = msg.getPayload().getOrchestratorResponse();

        switch (orchestratorResponse.getPayloadCase()) {
            case QUERY_RESULT:
                return getQueryResponse(orchestratorResponse.getQueryResult());
            case WORKFLOW_CREATED:
                return getCreateWorkflowResponse(orchestratorResponse.getWorkflowCreated());
            default:
                throw new UnsupportedOperationException("Unsupported orchestrator response type: " +
                        orchestratorResponse.getPayloadCase().toString());
        }
    }

    /**
     * Handle a REPORT_FAILURE response from the server.
     *
     * @param msg  The report failure response message.
     * @param ctx  The context the message was sent under.
     * @param r    A reference to the router.
     * @return     True if the handling was successful but false otherwise.
     */
    @ResponseHandler(type = PayloadCase.REPORT_FAILURE_RESPONSE)
    private static Object handleReportFailure(ResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return msg.getPayload().getReportFailureResponse().getHandlingSuccessful();
    }

    /**
     * Handle a HEAL_FAILURE response from the server.
     *
     * @param msg  The heal failure response message.
     * @param ctx  The context the message was sent under.
     * @param r    A reference to the router.
     * @return     True if the handling was successful but false otherwise.
     */
    @ResponseHandler(type = PayloadCase.HEAL_FAILURE_RESPONSE)
    private static Object handleHealFailure(ResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return msg.getPayload().getHealFailureResponse().getHandlingSuccessful();
    }

    /**
     * Handle a BOOTSTRAP_MANAGEMENT response from the server.
     *
     * @param msg  The bootstrap management response message.
     * @param ctx  The context the message was sent under.
     * @param r    A reference to the router.
     * @return     True if bootstrap successful but false otherwise.
     */
    @ResponseHandler(type = PayloadCase.BOOTSTRAP_MANAGEMENT_RESPONSE)
    private static Object handleBootstrapManagement(ResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return msg.getPayload().getBootstrapManagementResponse().getBootstrapped();
    }

    /**
     * Handle a MANAGEMENT_LAYOUT response from the server.
     *
     * @param msg  The management layout response message.
     * @param ctx  The context the message was sent under.
     * @param r    A reference to the router.
     * @return     The Layout sent back from server.
     */
    @ResponseHandler(type = PayloadCase.MANAGEMENT_LAYOUT_RESPONSE)
    private static Object handleManagementLayoutMsg(ResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return getLayout(msg.getPayload().getManagementLayoutResponse().getLayout());
    }

    /**
     * Handle a QUERY_NODE response from the server.
     *
     * @param msg  The query node response message.
     * @param ctx  The context the message was sent under.
     * @param r    A reference to the router.
     * @return     {@link NodeState} sent back from server.
     */
    @ResponseHandler(type = PayloadCase.QUERY_NODE_RESPONSE)
    private static Object handleQueryNode(ResponseMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return getNodeState(msg.getPayload().getQueryNodeResponse());
    }
}
