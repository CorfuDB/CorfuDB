package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.TokenRequestMsg;
import org.corfudb.protocols.wireprotocol.TokenResponseMsg;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This server implements the sequencer functionality of Corfu.
 *
 * It currently supports a single operation, which is a incoming request:
 *
 * TOKEN_REQ - Request the next token.
 *
 * Created by mwei on 12/8/15.
 */
@Slf4j
public class SequencerServer implements INettyServer {

    /** The options map */
    Map<String,Object> opts;

    @Getter
    long epoch;

    AtomicLong globalIndex;

    /**
     * A simple map of the most recently issued token for any given stream.
     */
    ConcurrentHashMap<UUID, Long> lastIssuedMap;

    public SequencerServer(Map<String,Object> opts)
    {
        this.opts = opts;
        lastIssuedMap = new ConcurrentHashMap<>();
        globalIndex = new AtomicLong();

        try {
           long newIndex = Long.parseLong((String)opts.get("--initial-token"));
            if (newIndex == -1)
            {
                log.warn("Sequencer recovery requested but not implemented, defaulting to 0.");
                globalIndex.set(0);
            }
            else
            {
                globalIndex.set(newIndex);
            }
            log.info("Sequencer initial token set to {}", globalIndex.get());
        }
        catch (Exception ex)
        {
            log.warn("Exception parsing initial token, default to 0.", ex);
        }
    }

    @Override
    public void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx, NettyServerRouter r) {
        switch (msg.getMsgType())
        {
            case TOKEN_REQ: {
                TokenRequestMsg req = (TokenRequestMsg) msg;
                if (req.getNumTokens() == 0)
                {
                    long max = 0L;
                    for (UUID id : req.getStreamIDs()) {
                        Long lastIssued = lastIssuedMap.get(id);
                        max = Math.max(max, lastIssued == null ? Long.MIN_VALUE: lastIssued);
                    }
                    r.sendResponse(ctx, msg,
                            new TokenResponseMsg(max));
                }
                else {
                    long thisIssue = globalIndex.getAndAdd(req.getNumTokens());
                    for (UUID id : req.getStreamIDs()) {
                        lastIssuedMap.compute(id, (k, v) -> v == null ? thisIssue + req.getNumTokens() :
                                Math.max(thisIssue + req.getNumTokens(), v));
                    }
                    r.sendResponse(ctx, msg,
                            new TokenResponseMsg(thisIssue));
                }
            }
            break;
            default:
                log.warn("Unknown message type {} passed to handler!", msg.getMsgType());
                throw new RuntimeException("Unsupported message passed to handler!");
        }
    }

    @Override
    public void reset() {
        globalIndex.set(0L);
    }
}
