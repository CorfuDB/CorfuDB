package org.corfudb.runtime.clients;


import io.netty.channel.ChannelHandlerContext;

import java.lang.invoke.MethodHandles;

import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.NodeView;
import org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorResponse;

import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.exceptions.NoBootstrapException;

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
    public ManagementClient getClient(long epoch) {
        return new ManagementClient(router, epoch);
    }

    /**
     * The handler and handlers which implement this client.
     */
    @Getter
    public ClientMsgHandler msgHandler = new ClientMsgHandler(this)
            .generateHandlers(MethodHandles.lookup(), this);


    @ClientHandler(type = CorfuMsgType.ORCHESTRATOR_RESPONSE)
    private static Object handleOrchestratorResponse(CorfuPayloadMsg<OrchestratorResponse> msg,
                                                     ChannelHandlerContext ctx, IClientRouter r) {
        return msg.getPayload();
    }

    @ClientHandler(type = CorfuMsgType.HEARTBEAT_RESPONSE)
    private static Object handleHeartbeatResponse(CorfuPayloadMsg<NodeView> msg,
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
}
