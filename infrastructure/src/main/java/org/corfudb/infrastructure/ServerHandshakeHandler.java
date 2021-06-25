package org.corfudb.infrastructure;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.AttributeKey;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;
import java.util.UUID;
import javax.net.ssl.SSLHandshakeException;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.HandshakeState;
import org.corfudb.runtime.proto.service.Base.HandshakeRequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;

import static org.corfudb.protocols.CorfuProtocolCommon.DEFAULT_UUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getUUID;
import static org.corfudb.protocols.service.CorfuProtocolBase.getHandshakeResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;

/**
 * The ServerHandshakeHandler waits for the handshake message, validates and sends
 * a response to the client. This reply contains its node id and current version of Corfu.
 *
 * Created by amartinezman on 12/11/17.
 */
@Slf4j
public class ServerHandshakeHandler extends ChannelDuplexHandler {

    private final UUID nodeId;
    private final HandshakeState handshakeState;
    private final int timeoutInSeconds;
    private final long corfuServerVersion;
    private final Set<ResponseMsg> responseMessages = ConcurrentHashMap.newKeySet();
    private static final  AttributeKey<UUID> clientIdAttrKey = AttributeKey.valueOf("ClientID");
    private static final String READ_TIMEOUT_HANDLER = "readTimeoutHandler";

    /**
     * Creates a new ServerHandshakeHandler which will handle the handshake--initiated by a client
     * on the server side.
     *
     * @param nodeId Current Server Node Identifier.
     */
    public ServerHandshakeHandler(UUID nodeId, long corfuServerVersion, int timeoutInSeconds) {
        this.nodeId = nodeId;
        this.corfuServerVersion = corfuServerVersion;
        this.timeoutInSeconds = timeoutInSeconds;
        this.handshakeState = new HandshakeState();
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
        if (this.handshakeState.failed()) {
            log.warn("channelRead: Dropping the message as the handshake was not completed. Message - {}", m);
            return;
        }

        if (!(m instanceof RequestMsg)) {
            log.warn("channelRead: Dropping the message as the received msg is not an instance" +
                    " of RequestMsg. Message - {}", m);
            return;
        }

        RequestMsg requestMsg = (RequestMsg) m;
        long corfuClientVersion = requestMsg.getHeader().getVersion().getCorfuSourceCodeVersion();
        if (corfuClientVersion != corfuServerVersion) {
            log.warn("channelRead: Version mismatch. Corfu client version: {}, Corfu server version: {}",
                    Long.toHexString(corfuClientVersion), Long.toHexString(corfuServerVersion));
        }

        if (!requestMsg.getPayload().hasHandshakeRequest()) {
            log.warn("channelRead: Non-handshake message received by handshake handler. Message - {}", requestMsg);

            if (this.handshakeState.completed()) {
                // If handshake completed successfully, but still a message came through this handler,
                // send on to the next handler in order to avoid message loss.
                log.warn("channelRead: Sending the message to upstream as the handshake was completed. Message - {}", requestMsg);
                super.channelRead(ctx, m);
            } else {
                log.warn("channelRead: Dropping the message as the handshake was not completed. Message - {}", requestMsg);
            }

            return;
        }

        log.debug("channelRead: Handshake message received. Removing {} from pipeline.", READ_TIMEOUT_HANDLER);

        // Remove the handler from the pipeline. Also remove the reference of the context from
        // the handler so that it does not disconnect the channel.
        ctx.pipeline().remove(READ_TIMEOUT_HANDLER).handlerRemoved(ctx);

        HandshakeRequestMsg handshakeRequest = requestMsg.getPayload().getHandshakeRequest();
        UUID clientId = getUUID(handshakeRequest.getClientId());
        UUID serverId = getUUID(handshakeRequest.getServerId());

        // Validate handshake, but first verify if node identifier is set to default (all 0's)
        // which indicates node id matching is not required.
        if (serverId.equals(DEFAULT_UUID)) {
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
        log.debug("channelRead: Sending handshake response: Node Id: {}, Corfu client version:" +
                " {}, Corfu server version: {}", this.nodeId, Long.toHexString(corfuClientVersion),
                Long.toHexString(corfuServerVersion));


        // Note: we reuse the request header as the ignore_cluster_id and
        // ignore_epoch fields are the same in both cases.
        ResponseMsg response = getResponseMsg(getHeaderMsg(requestMsg.getHeader()),
                getHandshakeResponseMsg(this.nodeId));

        ctx.writeAndFlush(response);

        // Flush messages in backlog
        responseMessages.forEach(ctx::writeAndFlush);
        responseMessages.clear();

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
        log.debug("channelInactive: Channel closed.");
        if (!this.handshakeState.completed()) {
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
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("exceptionCaught: Exception caught: {}", cause.getMessage());

        if (cause instanceof ReadTimeoutException) {
            // Read timeout: no inbound traffic detected in a period of time.
            if (this.handshakeState.failed()) {
                log.debug("exceptionCaught: Handshake timeout checker: already failed.");
                return;
            }

            if (!this.handshakeState.completed()) {
                log.error("exceptionCaught: Handshake timeout checker: timed out. Close Connection.");
                this.handshakeState.set(true, false);
            } else {
                log.debug("exceptionCaught: Handshake timeout " +
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
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (this.handshakeState.failed()) {
            // If handshake failed, discard messages.
            return;
        }

        if (this.handshakeState.completed()) {
            log.debug("write: Handshake already completed, not appending corfu message to queue");
            super.write(ctx, msg, promise);
        } else {
            if (msg instanceof ResponseMsg){
                this.responseMessages.add((ResponseMsg) msg);
            } else {
                log.warn("write: Invalid message received through the pipeline by Handshake handler, Dropping it." +
                        " Message - {}", msg);
            }
        }
    }

    /**
     * Signal handshake as failed.
     *
     * @param ctx channel handler context
     */
    private void fireHandshakeFailed(ChannelHandlerContext ctx) {
        this.handshakeState.set(true, true);
        ctx.channel().close();
    }

    /**
     * Signal handshake as succeeded.
     */
    private void fireHandshakeSucceeded() {
        this.handshakeState.set(false, true);
    }
}
