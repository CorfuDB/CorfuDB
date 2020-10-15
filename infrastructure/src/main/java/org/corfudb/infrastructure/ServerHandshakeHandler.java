package org.corfudb.infrastructure;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.AttributeKey;

import java.util.LinkedList;
import java.util.Queue;
import java.util.UUID;

import javax.net.ssl.SSLHandshakeException;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.API;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.HandshakeMsg;
import org.corfudb.protocols.wireprotocol.HandshakeResponse;
import org.corfudb.protocols.wireprotocol.HandshakeState;
import org.corfudb.runtime.protocol.proto.CorfuProtocol;

import static org.corfudb.runtime.protocol.proto.CorfuProtocol.*;

/**
 * The ServerHandshakeHandler waits for the handshake message, validates and sends
 * a response to the client. This reply contains its node id and current version of Corfu.
 *
 * Created by amartinezman on 12/11/17.
 */
@Slf4j
public class ServerHandshakeHandler extends ChannelDuplexHandler {

    private final UUID nodeId;
    private final String corfuVersion;
    private final HandshakeState state;
    private final int timeoutInSeconds;
    private final Queue<CorfuMsg> messages = new LinkedList<>();
    private static final  AttributeKey<UUID> clientIdAttrKey = AttributeKey.valueOf("ClientID");
    private static final String READ_TIMEOUT_HANDLER = "readTimeoutHandler";

    /**
     * Creates a new ServerHandshakeHandler which will handle the handshake--initiated by a client
     * on the server side.
     *
     * @param nodeId Current Server Node Identifier.
     * @param corfuVersion Version of Corfu in Server Node.
     */
    public ServerHandshakeHandler(UUID nodeId, String corfuVersion, String timeoutInSeconds) {
        this.nodeId = nodeId;
        this.corfuVersion = corfuVersion;
        this.timeoutInSeconds = Integer.parseInt(timeoutInSeconds);
        this.state = new HandshakeState();
    }

    /**
     * Read data from the Channel.
     *
     * @param ctx channel handler context
     * @param m object received in inbound buffer
     * @throws Exception
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object m) throws Exception {
        log.warn("ChannelRead: message received - "+ m);
        if (this.state.failed()) {
            return;
        }

        if (this.state.completed()) {
            // If handshake completed successfully, but still a message came through this handler,
            // send on to the next handler in order to avoid message loss.
            super.channelRead(ctx, m);
            return;
        }
        UUID clientId = null;
        UUID serverId = null;

        if (m instanceof CorfuMsg) {
            CorfuPayloadMsg<HandshakeMsg> handshake;
            try {
                handshake = (CorfuPayloadMsg<HandshakeMsg>) m;
                log.info("channelRead: Handshake Message received. Removing {} from pipeline.",
                        READ_TIMEOUT_HANDLER);
                // Remove the handler from the pipeline. Also remove the reference of the context from
                // the handler so that it does not disconnect the channel.
                ctx.pipeline().remove(READ_TIMEOUT_HANDLER).handlerRemoved(ctx);
            } catch (ClassCastException e) {
                log.warn("channelRead: Non-handshake message received by handshake handler." +
                        " Send upstream only if handshake succeeded.");
                if (this.state.completed()) {
                    // Only send upstream if handshake is complete.
                    super.channelRead(ctx, m);
                } else {
                    // Otherwise, drop message.
                    try {
                        log.info("channelRead: Dropping message: {}", ((CorfuMsg) m).getMsgType().name());
                    } catch (Exception ex) {
                        log.error("channelRead: Message received by Server is not a valid " +
                                "CorfuMsg type.");
                    }
                }
                return;
            }

            clientId = handshake.getPayload().getClientId();
            serverId = handshake.getPayload().getServerId();
        } else if (m instanceof Request) {
            Request request = ((Request) m);
            HandshakeRequest handshakeRequest = request.getHandshakeRequest();
            clientId = API.getJavaUUID(handshakeRequest.getClientId());
            serverId = API.getJavaUUID(handshakeRequest.getServerId());
        } else {
            if (this.state.completed()) {
                // Only send upstream if handshake is complete.
                super.channelRead(ctx, m);
            }
            // The message was unregistered, we are dropping it.
            log.error("channelRead: Received unregistered message {}.", m);
        }


        // Validate handshake, but first verify if node identifier is set to default (all 0's)
        // which indicates node id matching is not required.
        if (serverId.equals(UUID.fromString("00000000-0000-0000-0000-000000000000"))) {
            log.info("channelRead: node id matching is not requested by client.");
        } else if (!serverId.equals(this.nodeId)) {
            log.error("channelRead: Invalid handshake: this is {} and client is trying to connect to {}",
                    this.nodeId, serverId);
            this.fireHandshakeFailed(ctx);
            return;
        }

        // Store clientID as a channel attribute.
        ctx.channel().attr(clientIdAttrKey).set(clientId);
        log.info("channelRead: Handshake validated by Server.");
        log.info("channelRead: Sending handshake response: Node Id: {} Corfu Version: {}",
                this.nodeId, this.corfuVersion);


        if (m instanceof CorfuMsg){
            CorfuMsg handshakeResponse = CorfuMsgType.HANDSHAKE_RESPONSE
                    .payloadMsg(new HandshakeResponse(this.nodeId, this.corfuVersion));
            ctx.writeAndFlush(handshakeResponse);
        } else if (m instanceof Request) {
            // Note: we reuse the request header as the ignore_cluster_id and
            // ignore_epoch fields are the same in both cases.
            Response response = API.getHandshakeResponse(((Request) m).getHeader(), this.nodeId, this.corfuVersion);
            ctx.writeAndFlush(response);
        }


        // Flush messages in queue
        log.info("channelRead: There are [{}] messages in queue to be flushed.", this.messages.size());
        while (!messages.isEmpty()) {
            ctx.writeAndFlush(messages.poll());
        }

        // Remove this handler from the pipeline; handshake is completed.
        log.info("channelRead: Removing handshake handler from pipeline.");
        ctx.pipeline().remove(this);
        this.fireHandshakeSucceeded();
    }

    /**
     * Channel event that is triggered when a new connected channel is created.
     *
     * @param ctx channel handler context
     * @throws Exception
     */
    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        log.info("channelActive: Incoming connection established from: {} Start Read Timeout.",
                ctx.channel().remoteAddress());
        ctx.pipeline().addBefore(ctx.name(), READ_TIMEOUT_HANDLER,
                new ReadTimeoutHandler(this.timeoutInSeconds));
    }

    /**
     * Channel event that is triggered when the channel is closed.
     *
     * @param ctx channel handler context
     * @throws Exception
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info("channelInactive: Channel closed.");
        if (!this.state.completed()) {
            this.fireHandshakeFailed(ctx);
        }
    }

    /**
     * Channel event that is triggered when an exception is caught.
     *
     * @param ctx channel handler context
     * @param cause exception cause
     * @throws Exception
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
        log.error("exceptionCaught: Exception caught: {}", cause.getMessage());

        if (cause instanceof ReadTimeoutException) {
            // Read timeout: no inbound traffic detected in a period of time.
            if (this.state.failed()) {
                log.info("exceptionCaught: Handshake timeout checker: already failed.");
                return;
            }

            if (!this.state.completed()) {
                log.error("exceptionCaught: Handshake timeout checker: timed out. Close Connection.");
                this.state.set(true, false);
            } else {
                log.info("exceptionCaught: Handshake timeout " +
                        "checker: discarded (handshake OK)");
            }
        } else if (cause instanceof SSLHandshakeException) {
            // If an SslException is thrown by the inbound SslHandler it will be caught by this
            // upstream handler (in the pipeline). For debugging purposes we log the address of
            // the client that failed to authenticate.

            log.error("Client authentication failed for remote address {}",
                    ctx.channel().remoteAddress());
        } else {
            super.exceptionCaught(ctx, cause);
        }

        if (ctx.channel().isActive()) {
            // Closing the channel will trigger handshake failure.
            ctx.channel().close();
        } else {
            // Channel did not open, fire handshake failure.
            this.fireHandshakeFailed(ctx);
        }
    }

    /**
     * Channel event that is triggered when an outbound handler attempts to write into the channel.
     *
     * @param ctx channel handler context
     * @param msg message written into channel
     * @param promise channel promise
     * @throws Exception
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
            throws Exception {
        if (this.state.failed()) {
            // If handshake failed, discard messages.
            return;
        }

        if (this.state.completed()) {
            log.info("write: Handshake already completed, not appending corfu message to queue");
            super.write(ctx, msg, promise);
        } else {
            this.messages.offer((CorfuMsg) msg);
        }
    }

    /**
     * Signal handshake as failed.
     *
     * @param ctx channel handler context
     */
    private void fireHandshakeFailed(ChannelHandlerContext ctx) {
        this.state.set(true, true);
        ctx.channel().close();
    }

    /**
     * Signal handshake as succeeded.
     */
    private void fireHandshakeSucceeded() {
        this.state.set(false, true);
    }
}
