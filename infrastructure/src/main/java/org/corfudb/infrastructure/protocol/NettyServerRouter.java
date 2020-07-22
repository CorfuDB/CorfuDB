package org.corfudb.infrastructure.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import com.google.common.collect.ImmutableList;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.protocol.proto.CorfuProtocol;
import org.corfudb.common.protocol.proto.CorfuProtocol.MessageType;
import org.corfudb.common.protocol.proto.CorfuProtocol.Request;
import org.corfudb.common.protocol.proto.CorfuProtocol.Header;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.runtime.view.Layout;

import java.util.*;

@Slf4j
@ChannelHandler.Sharable
public class NettyServerRouter extends ChannelInboundHandlerAdapter implements IServerRouter {

    /**
     * This map stores the mapping from message types to server handler.
     */
    private final Map<MessageType, AbstractServer> handlerMap;

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

    /** The {@link AbstractServer}s this {@link NettyServerRouter} routes messages for. */
    private final ImmutableList<AbstractServer> servers;

    public NettyServerRouter(ImmutableList<AbstractServer> servers, ServerContext serverContext) {
        this.serverContext = serverContext;
        this.serverEpoch = serverContext.getServerEpoch();
        this.servers = servers;
        handlerMap = new EnumMap<>(MessageType.class);

        servers.forEach(server -> {
            Set<MessageType> handledTypes = server.getHandler().getHandledTypes();
            handledTypes.forEach(handledType -> handlerMap.put(handledType, server));
        });
    }

    @Override
    public Optional<Layout> getCurrentLayout() {
        return Optional.ofNullable(serverContext.getCurrentLayout());
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
        //TODO(Zach): Method still needed?
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
        //TODO(Zach): Method still needed?
        throw new UnsupportedOperationException("The operation is not supported.");
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("channelActive: Incoming connection established from: {}.",
                ctx.channel().remoteAddress());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf msgBuf = (ByteBuf) msg;

        // Temporary -- Check Corfu msg marker: 0x1 indicates legacy while 0x2 indicates new
        if (msgBuf.getByte(msgBuf.readerIndex()) == 0x1) {
            ctx.fireChannelRead(msgBuf); // Forward legacy corfu msg to next handler
            return;
        }

        msgBuf.readByte(); // Temporary -- Consume 0x2 marker

        ByteBufInputStream msgInputStream = new ByteBufInputStream(msgBuf);

        try {
            Request request = Request.parseFrom(msgInputStream);
            Header header = request.getHeader();

            if (log.isDebugEnabled()) {
                log.debug("Request {} pi {} from {}", header.getType(), ctx.channel().remoteAddress());
            }

            AbstractServer handler = handlerMap.get(header.getType());
            if (handler == null) {
                log.warn("Received unregistered message {}, dropping", header.getType());
            } else {
                //TODO(Zach): Check if request is valid.
                try {
                    handler.handleRequest(request, ctx, this);
                } catch(Throwable t) {
                    log.error("channelRead: Handling {} failed due to {}:{}",
                            header.getType(),t.getClass().getSimpleName(),
                            t.getMessage(), t);
                }
            }

        } catch (Exception e) {
            log.error("Exception during read!", e);
        } finally {
            msgInputStream.close();
            msgBuf.release();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Error in handling inbound message, {}", cause);
        ctx.close();
    }
}
