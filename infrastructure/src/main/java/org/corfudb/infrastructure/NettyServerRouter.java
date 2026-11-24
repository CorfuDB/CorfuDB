package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.TextFormat;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg.PayloadCase;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.view.Layout;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * The netty server router routes incoming messages to registered roles using the
 * handlerMap (in the case of a legacy CorfuMsg) and requestTypeHandlerMap (in
 * the case of a Protobuf RequestMsg).
 * Created by mwei on 12/1/15.
 */
@Slf4j
@ChannelHandler.Sharable
public class NettyServerRouter extends ChannelInboundHandlerAdapter implements IServerRouter {

    /**
     * This map stores the mapping from message types to server handler.
     */
    private final Map<PayloadCase, AbstractServer> requestTypeHandlerMap;

    /**
     * This node's server context.
     */
    private final ServerContext serverContext;
    /**
     * The epoch of this router. This is managed by the base server implementation.
     */
    @Getter
    @Setter
    volatile long serverEpoch;

    /**
     * The {@link AbstractServer}s this {@link NettyServerRouter} routes messages for.
     */
    private final ImmutableList<AbstractServer> servers;

    /**
     * Construct a new {@link NettyServerRouter}.
     *
     * @param servers A list of {@link AbstractServer}s this router will route
     *                messages for.
     */
    public NettyServerRouter(ImmutableList<AbstractServer> servers, ServerContext serverContext) {
        this.serverContext = serverContext;
        this.serverEpoch = serverContext.getServerEpoch();
        this.servers = servers;
        requestTypeHandlerMap = new EnumMap<>(PayloadCase.class);

        servers.forEach(server -> {
            server.getHandlerMethods().getHandledTypes().forEach(handledType ->
                        requestTypeHandlerMap.put(handledType, server));
        });
    }

    /**
     * {@inheritDoc}
     *
     * @deprecated This operation is no longer supported. The router will only route messages for
     * servers provided at construction time.
     */
    @Override
    @Deprecated
    public void addServer(AbstractServer server) {
        throw new UnsupportedOperationException("No longer supported");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AbstractServer> getServers() {
        return servers;
    }

    @Override
    public void setServerContext(ServerContext serverContext) {
        throw new UnsupportedOperationException("The operation is not supported.");
    }

    /**
     * Send a response message through this router.
     *
     * @param response The response message to send.
     * @param ctx      The context of the channel handler.
     */
    public void sendResponse(ResponseMsg response, ChannelHandlerContext ctx) {

        Channel channel = ctx.channel();
        synchronized (channel) {
            long timeout = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100);
            while (!channel.isWritable() && System.nanoTime() < timeout) {
                try {
                    TimeUnit.MILLISECONDS.sleep(1);
                } catch (InterruptedException ie) {
                    break;
                }
            }

            if (!channel.isWritable()) {
                channel.close();
                log.warn("Channel {} not writable for {}. Dropping {}", channel, timeout, response);
                return;
            }
        }

        ctx.writeAndFlush(response, ctx.voidPromise());

        if(log.isTraceEnabled()) {
            log.trace("Sent response: {}", TextFormat.shortDebugString(response));
        }
    }

    @Override
    public Optional<Layout> getCurrentLayout() {
        return Optional.ofNullable(serverContext.getCurrentLayout());
    }

    /**
     * Handle an incoming message read on the channel.
     *
     * @param ctx Channel handler context
     * @param msg The incoming message on that channel.
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        RequestMsg request = ((RequestMsg) msg);
        RequestPayloadMsg payload = request.getPayload();

        AbstractServer handler = requestTypeHandlerMap.get(payload.getPayloadCase());
        if (handler == null) {
            log.warn("channelRead: Received unregistered request {}, dropping", payload.getPayloadCase());
        } else {
            if (validateRequest(request, ctx)) {
                if (log.isTraceEnabled()) {
                    log.trace("channelRead: Request routed to {}: {}",
                            handler.getClass().getSimpleName(), TextFormat.shortDebugString(request));
                }

                try {
                    handler.handleMessage(request, ctx, this);
                } catch (Throwable t) {
                    log.error("channelRead: Handling {} failed due to {}:{}",
                            payload.getPayloadCase(), t.getClass().getSimpleName(), t.getMessage(), t);
                }
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Error in handling inbound message", cause);
        ctx.close();
    }
}
