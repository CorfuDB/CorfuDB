package org.corfudb.infrastructure.logreplication.transport.sample;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.Messages.CorfuMessageType;
import org.corfudb.runtime.Messages.CorfuMessage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@ChannelHandler.Sharable
public class CorfuNettyServerChannel extends ChannelInboundHandlerAdapter {

    private NettyLogReplicationServerChannelAdapter adapter;

    private Map<Long, ChannelHandlerContext> contextMap;

    private Map<Long, ChannelHandlerContext> contextMapLogEntries;

    public CorfuNettyServerChannel(NettyLogReplicationServerChannelAdapter adapter) {

        this.adapter = adapter;
        this.contextMap = new ConcurrentHashMap<>();
        this.contextMapLogEntries = new ConcurrentHashMap<>();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            // The incoming message should have been transformed to a CorfuMessage earlier in the
            // pipeline.
            CorfuMessage message = (CorfuMessage) msg;

            log.trace("Received message {}", message.getType());

            // Hold ChannelHandlerContexts to send response back

            // Note: log replication entries send a single summarized ACK as response for a batch of entries
            // for this reason, we will hold in a separate map so we can remove all context handlers for requests lower
            // than the one being served and avoid a memory leak.
            Map<Long, ChannelHandlerContext> contexts = message.getType() == CorfuMessageType.LOG_REPLICATION_ENTRY ?
                    contextMapLogEntries : contextMap;
            contexts.put(message.getRequestID(), ctx);

            // Send to the adapter for further processing.
            adapter.receive(message);
        } catch (Exception e) {
            log.error("Exception during read!", e);
        }
    }

    /**
     * Channel event that is triggered when a new connected channel is created.
     *
     * @param ctx channel handler context
     * @throws Exception
     */
    @Override
    public void channelActive(final ChannelHandlerContext ctx) {
        log.info("channelActive: Incoming connection established from: {} Start Read Timeout.",
                ctx.channel().remoteAddress());
    }

    /**
     * Send a netty message through this channel.
     *
     * @param outMsg Outgoing message.
     */
    public synchronized void sendResponse(CorfuMessage outMsg) {
        ChannelHandlerContext ctx = getContext(outMsg);
        if (ctx != null) {
            ctx.writeAndFlush(outMsg, ctx.voidPromise());
            log.trace("Sent response: {}", outMsg);
        } else {
            log.warn("Netty context not found for request id={}. Dropping message type={}", outMsg.getRequestID(),
                    outMsg.getType());
        }
    }

    private ChannelHandlerContext getContext(CorfuMessage message) {

        ChannelHandlerContext context;

        if (message.getType() == CorfuMessageType.LOG_REPLICATION_ENTRY) {
            // Because ACKs are aggregated (summarized) for a batch of messages, remove all
            // contexts lower than this request ID (to prevent memory leak, as those other messages, will
            // never be served)
            context = contextMapLogEntries.remove(message.getRequestID());
            contextMapLogEntries.keySet().removeIf(id -> id <= message.getRequestID());
        } else {
            context = contextMap.remove(message.getRequestID());
        }

        return context;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Error in handling inbound message, {}", cause);
        ctx.close();
    }
}
