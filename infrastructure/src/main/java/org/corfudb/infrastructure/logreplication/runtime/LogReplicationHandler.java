package org.corfudb.infrastructure.logreplication.runtime;

import com.google.protobuf.TextFormat;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.clients.ClientResponseHandler;
import org.corfudb.runtime.clients.ClientResponseHandler.Handler;
import org.corfudb.runtime.clients.IClient;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.IHandler;
import org.corfudb.runtime.clients.ResponseHandler;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase;

import javax.annotation.Nonnull;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


/**
 * A client to the Log Replication Server
 */
@Slf4j
public class LogReplicationHandler implements IClient, IHandler<LogReplicationClient> {

    @Setter
    @Getter
    private IClientRouter router;

    /**
     * The handler and handlers which implement this client.
     */
    @Getter
    @Setter
    public ClientResponseHandler responseHandler = createResponseHandlers(this, new ConcurrentHashMap<>());

    /**
     * Used for testing and allows for augmenting default member variables.
     *
     * @param client     A client from which the handlers will be extracted
     * @param handlerMap A map implementation which will store handler mappings
     * @return           A new instance of ClientResponseHandler
     */
    public ClientResponseHandler createResponseHandlers(IClient client, Map<PayloadCase, Handler> handlerMap) {
        return new ClientResponseHandler(client, handlerMap)
                .generateHandlers(MethodHandles.lookup(), client)
                .generateErrorHandlers(MethodHandles.lookup(), client);
    }

    /**
     * Handle an ACK from Log Replication server.
     *
     * @param response The ack message
     * @param ctx      The context the message was sent under
     * @param router   A reference to the router
     */
    @ResponseHandler(type = PayloadCase.LR_ENTRY_ACK)
    private static Object handleLogReplicationAck(@Nonnull ResponseMsg response,
                                                  @Nonnull ChannelHandlerContext ctx,
                                                  @Nonnull IClientRouter router) {
        log.debug("Handle log replication ACK");
        return response.getPayload().getLrEntryAck();
    }

    @ResponseHandler(type = PayloadCase.LR_METADATA_RESPONSE)
    private static Object handleLogReplicationMetadata(@Nonnull ResponseMsg response,
                                                       @Nonnull ChannelHandlerContext ctx,
                                                       @Nonnull IClientRouter router) {
        log.debug("Handle log replication Metadata Response");
        return response.getPayload().getLrMetadataResponse();
    }

    @ResponseHandler(type = PayloadCase.LR_LEADERSHIP_RESPONSE)
    private static Object handleLogReplicationQueryLeadershipResponse(@Nonnull ResponseMsg response,
                                                                      @Nonnull ChannelHandlerContext ctx,
                                                                      @Nonnull IClientRouter router) {
        log.trace("Handle log replication query leadership response msg {}", TextFormat.shortDebugString(response));
        return response.getPayload().getLrLeadershipResponse();
    }

    @ResponseHandler(type = PayloadCase.LR_LEADERSHIP_LOSS)
    private static Object handleLogReplicationLeadershipLoss(@Nonnull ResponseMsg response,
                                                             @Nonnull ChannelHandlerContext ctx,
                                                             @Nonnull IClientRouter router) {
        log.debug("Handle log replication leadership loss msg {}", TextFormat.shortDebugString(response));
        return response.getPayload().getLrLeadershipLoss();
    }

    @Override
    public LogReplicationClient getClient(long epoch, UUID clusterID) {
        return new LogReplicationClient(router, epoch);
    }
}
