package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;

import java.lang.invoke.MethodHandles;

import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorResponse;

import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.exceptions.NoBootstrapException;

/**
 * A client to the Management Server.
 *
 * <p>Failure Detection:
 * This client allows a client to trigger failures handlers with relevant failures.
 *
 * <p>Created by zlokhandwala on 11/4/16.
 */
public class ManagementClient implements IClient {

    @Setter
    @Getter
    IClientRouter router;

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
    private static Object handleHeartbeatResponse(CorfuPayloadMsg<byte[]> msg,
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
