package org.corfudb.infrastructure;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.corfudb.infrastructure.wireprotocol.NettyCorfuMsg;
import org.corfudb.infrastructure.wireprotocol.NettyStreamingServerTokenRequestMsg;
import org.corfudb.infrastructure.wireprotocol.NettyStreamingServerTokenResponseMsg;
import org.corfudb.util.SizeBufferPool;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalAndSentinelRetry;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static io.netty.buffer.Unpooled.directBuffer;

/**
 * A streaming sequencer which uses the netty library.
 * Created by mwei on 9/10/15.
 */
@Slf4j
public class NettyStreamingSequencerServer extends AbstractNettyServer {

    /**
     * A simple map of the most recently issued token for any given stream.
     */
    ConcurrentHashMap<UUID, Long> lastIssuedMap;

    /**
     * The current global index.
     */
    AtomicLong globalIndex;

    @Override
    void parseConfiguration(Map<String, Object> configuration)
    {
        serverName = "NettyStreamingSequencerServer";
        reset();
    }

    /** Process an incoming message
     *
     * @param msg   The message to process.
     * @param r     Where to send the response.
     */
    void processMessage(NettyCorfuMsg msg, ChannelHandlerContext ctx)
    {
        switch (msg.getMsgType())
        {
            case TOKEN_REQ: {
                NettyStreamingServerTokenRequestMsg req = (NettyStreamingServerTokenRequestMsg) msg;
                if (req.getNumTokens() == 0)
                {
                    long max = 0L;
                    for (UUID id : req.getStreamIDs()) {
                        Long lastIssued = lastIssuedMap.get(id);
                        max = Math.max(max, lastIssued == null ? Long.MIN_VALUE: lastIssued);
                    }
                    NettyStreamingServerTokenResponseMsg resp = new NettyStreamingServerTokenResponseMsg(max);
                    sendResponse(resp, msg, ctx);
                }
                else {
                    long thisIssue = globalIndex.getAndAdd(req.getNumTokens());
                    for (UUID id : req.getStreamIDs()) {
                        lastIssuedMap.compute(id, (k, v) -> v == null ? thisIssue + req.getNumTokens() :
                                Math.max(thisIssue + req.getNumTokens(), v));
                    }
                    NettyStreamingServerTokenResponseMsg resp = new NettyStreamingServerTokenResponseMsg(thisIssue);
                    sendResponse(resp, msg, ctx);
                }
            }
            break;
            default:
                log.warn("Unknown message type {} passed to handler!", msg.getMsgType());
                throw new RuntimeException("Unsupported message passed to handler!");
        }
    }

    /**
     * Reset the state of the server.
     */
    @Override
    public void reset() {
        globalIndex = new AtomicLong(0);
        lastIssuedMap = new ConcurrentHashMap<>();
    }
}
