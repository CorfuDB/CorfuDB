package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.corfudb.common.protocol.API;
import org.corfudb.common.protocol.proto.CorfuProtocol;
import org.corfudb.common.protocol.proto.CorfuProtocol.MessageType;
import org.corfudb.common.protocol.proto.CorfuProtocol.Request;
import org.corfudb.common.protocol.proto.CorfuProtocol.Response;
import org.corfudb.common.protocol.proto.CorfuProtocol.Header;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.runtime.view.Layout;

import java.io.IOException;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Slf4j
@ChannelHandler.Sharable
public class NettyRequestRouter extends ChannelInboundHandlerAdapter implements IRequestRouter {

    /**
     * This map stores the mapping from message types to server handler.
     */
    private final Map<MessageType, AbstractServer> requestTypeHandlerMap;

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

    /** The {@link AbstractServer}s this {@link NettyRequestRouter} routes messages for. */
    private final ImmutableList<AbstractServer> servers;

    /**
     * Construct a new NettyRequestRouter.
     * @param servers A list of {@link AbstractServer}s this router will route requests for.
     * @param serverContext The server context.
     */
    public NettyRequestRouter(ImmutableList<AbstractServer> servers, ServerContext serverContext) {
        this.serverContext = serverContext;
        serverEpoch = serverContext.getServerEpoch();
        this.servers = servers;
        requestTypeHandlerMap = new EnumMap<>(MessageType.class);

        servers.forEach(server -> {
            Set<MessageType> handledTypes = server.getHandlerMethods().getHandledTypes();
            handledTypes.forEach(handledType -> requestTypeHandlerMap.put(handledType, server));
        });
    }

    @Override
    public Optional<Layout> getCurrentLayout() {
        return Optional.ofNullable(serverContext.getCurrentLayout());
    }

    /**
     * Send a response message through this router.
     * @param response The response message to send.
     * @param ctx The context of the channel handler.
     */
    public void sendResponse(Response response, ChannelHandlerContext ctx) {
        ByteBuf outBuf = PooledByteBufAllocator.DEFAULT.buffer();
        ByteBufOutputStream responseOutputStream = new ByteBufOutputStream(outBuf);

        try {
            responseOutputStream.writeByte(API.PROTO_CORFU_MSG_MARK);
            response.writeTo(responseOutputStream);
            ctx.writeAndFlush(outBuf, ctx.voidPromise());
        } catch(IOException e) {
            log.warn("sendResponse[{}]: Exception occurred when sending response {}, caused by {}",
                    response.getHeader().getRequestId(), response.getHeader(), e.getCause(), e);
        } finally {
            IOUtils.closeQuietly(responseOutputStream);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AbstractServer> getServers() {
        return servers;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.info("channelActive: Incoming connection established from: {}.", ctx.channel().remoteAddress());
        ctx.fireChannelActive(); // So that legacy handshake is initiated.
    }
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf msgBuf = (ByteBuf) msg;

        // Temporary -- If message is a legacy message, forward the message.
        byte msgMark = msgBuf.getByte(msgBuf.readerIndex());
        if (msgMark == API.LEGACY_CORFU_MSG_MARK) {
            ctx.fireChannelRead(msgBuf);
            return;
        } else if(msgMark != API.PROTO_CORFU_MSG_MARK) {
            throw new IllegalStateException("Received incorrectly marked message.");
        }

        msgBuf.readByte();
        ByteBufInputStream msgInputStream = new ByteBufInputStream(msgBuf);

        try {
            Request request = Request.parseFrom(msgInputStream);
            Header header = request.getHeader();

            if (log.isDebugEnabled()) {
                log.debug("channelRead: Request {} from {}", header.getType(), ctx.channel().remoteAddress());
            }

            AbstractServer handler = requestTypeHandlerMap.get(header.getType());
            if (handler == null) {
                log.warn("channelRead: Received unregistered request {}, dropping", header.getType());
            } else {
                if(requestIsValid(request, ctx)) {
                    if(log.isTraceEnabled()) {
                        log.trace("channelRead: Request routed to {}: {}", handler.getClass().getSimpleName(), request);
                    }

                    try {
                        handler.handleRequest(request, ctx, this);
                    } catch(Throwable t) {
                        log.error("channelRead: Handling {} failed due to {}:{}",
                                header.getType(), t.getClass().getSimpleName(), t.getMessage(), t);
                    }
                }
            }
        } catch (Exception e) {
            log.error("channelRead: Exception during read!", e);
        } finally {
            msgInputStream.close();
            msgBuf.release();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("exceptionCaught: Error in handling inbound request, {}", cause);
        ctx.close();
    }
}
