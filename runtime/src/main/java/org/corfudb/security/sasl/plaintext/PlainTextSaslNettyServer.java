package org.corfudb.security.sasl.plaintext;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import lombok.extern.slf4j.Slf4j;

/**
 * Created by sneginhal on 01/31/2017.
 * This is the server side of SASL Plain Text authentication.
 * This SimpleChannelInboundHandler is inserted in the CorfuServer's Netty
 * pipeline. Once TLS is negotiated and we have a secure channel, this handler
 * expects the username/password from the remote end as per:
 * https://tools.ietf.org/html/rfc4616
 */
@Slf4j
public class PlainTextSaslNettyServer extends SimpleChannelInboundHandler<ByteBuf> {

    private SaslServer saslServer;

    private final String mechanisms = "PLAIN";

    static {
        PlainTextSaslServerProvider.initialize();
    }

    public PlainTextSaslNettyServer() throws SaslException {
        saslServer = Sasl.createSaslServer(mechanisms, "plain", null, null, null);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf buf)
            throws Exception {

        byte[] msg = new byte[buf.readableBytes()];
        buf.getBytes(0, msg);

        while (!saslServer.isComplete()) {
            try {
                byte[] challenge = saslServer.evaluateResponse(msg);
            } catch (SaslException se) {
                log.error("SaslException {}", se.toString());
                break;
            }
        }

        if (saslServer.isComplete()) {
            log.debug("channelRead0: Sasl handshake successful, uninstalling handler");
            ctx.pipeline().remove(this);
        }
    }
}
