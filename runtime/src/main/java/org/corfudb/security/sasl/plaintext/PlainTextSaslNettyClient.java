package org.corfudb.security.sasl.plaintext;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;

import io.netty.channel.ChannelPromise;
import java.util.ArrayDeque;
import java.util.Queue;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;

/**
 * Created by sneginhal on 01/31/2017.
 * This ChannelDuplexHandler is inserted into the CorfuRuntime's
 * Netty pipeline. After TLS is negotiated and we have a secure
 * channel, this handler sends the username/password for authentication
 * as per:
 * https://tools.ietf.org/html/rfc4616
 */
@Slf4j
public class PlainTextSaslNettyClient extends ChannelDuplexHandler {

    private SaslClient saslClient;

    private final String[] mechanisms = {"PLAIN"};

    /** Plaintext client constructor. */
    public PlainTextSaslNettyClient(String username, String password)
            throws SaslException {
        PlainTextCallbackHandler cbh = new PlainTextCallbackHandler(username,
                password);
        saslClient = Sasl.createSaslClient(mechanisms, username,
            "plain", null, null, cbh);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        byte[] response = new byte[0];
        while (!saslClient.isComplete()) {
            try {
                response = saslClient.evaluateChallenge(null);
            } catch (SaslException se) {
                log.error("SaslException {}", se.toString());
                break;
            }
        }

        if (saslClient.isComplete()) {
            ByteBuf buf = ctx.alloc().heapBuffer(response.length);
            ByteBuf encoded = buf.writeBytes(response);
            ctx.writeAndFlush(encoded);
            ctx.pipeline().remove(this);
            super.channelActive(ctx);
        }
    }

}
