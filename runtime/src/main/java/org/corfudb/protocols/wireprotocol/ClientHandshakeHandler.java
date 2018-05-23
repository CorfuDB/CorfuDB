package org.corfudb.protocols.wireprotocol;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.ReadTimeoutHandler;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.Queue;
import java.util.UUID;

/**
 * The ClientHandshakeHandler initiates the handshake upon socket connection.
 *
 * - Once the client connects to the server, it sends a handshake message that contains:
 *         its own id and the (asserted) server's node id.
 * - The server validates and replies with its node id and current version of Corfu.
 * - If validation is correct on both sides, message exchange is initiated between client-server,
 * otherwise, the handshake times out, and either server or client close the connection.
 *
 * Created by amartinezman on 12/8/17.
 */
@Slf4j
public class ClientHandshakeHandler extends ChannelDuplexHandler {

    private final UUID clientId;
    private final UUID nodeId;
    private final int handshakeTimeout;
    private final HandshakeState handshakeState;
    private final Queue<CorfuMsg> messages = new LinkedList<>();
    private static final String READ_TIMEOUT_HANDLER = "readTimeoutHandler";

    /** Events that the handshaker sends to downstream handlers.
     *
     */
    public enum ClientHandshakeEvent {
        CONNECTED,  /* Connection succeeded. */
        FAILED      /* Handshake failed. */
    }

    /**
     * Creates a new ClientHandshakeHandler which will handle the handshake between the
     * current client and a remote server.
     *
     * @param clientId Current Client Identifier.
     * @param serverId Remote Server Identifier to connect to.
     */
    public ClientHandshakeHandler(@NonNull UUID clientId, UUID serverId, int handshakeTimeout) {
        this.clientId = clientId;
        if (serverId == null) {
            // A null identifier, indicates node ID matching is not required. Send a default
            // (all 0's) UUID to Server, to ignore matching stage during handshake
            this.nodeId = UUID.fromString("00000000-0000-0000-0000-000000000000");
        } else {
            this.nodeId = serverId;
        }
        this.handshakeTimeout = handshakeTimeout;
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
    public void channelRead(ChannelHandlerContext ctx, Object m)
        throws Exception {

        if (this.handshakeState.failed()) {
            // if handshake has already failed, return
            return;
        }

        if (this.handshakeState.completed()) {
            // If handshake completed successfully, but still a message came through this handler,
            // send on to the next handler in order to avoid message loss.
            super.channelRead(ctx, m);
            return;
        }

        CorfuPayloadMsg<HandshakeResponse> handshakeResponse;

        try {
            handshakeResponse = (CorfuPayloadMsg<HandshakeResponse>) m;
            log.info("channelRead: Handshake Response received. Removing {} from pipeline.",
                    READ_TIMEOUT_HANDLER);
            ctx.pipeline().remove(READ_TIMEOUT_HANDLER);
        } catch (ClassCastException e) {
            log.warn("channelRead: Non-handshake message received by handshake handler. " +
                    "Send upstream only if handshake succeeded.");
            if (this.handshakeState.completed()) {
                // Only send upstream if handshake is complete.
                super.channelRead(ctx, m);
            } else {
                // Otherwise, drop message.
                try {
                    CorfuMsg msg = (CorfuMsg) m;
                    log.debug("channelRead: Dropping message: {}", msg.getMsgType().name());
                } catch (Exception ex) {
                    log.error("channelRead: Message received is not a valid CorfuMsg type.");
                }
            }
            return;
        }

        UUID serverId = handshakeResponse.getPayload().getServerId();
        String corfuVersion = handshakeResponse.getPayload().getCorfuVersion();

        // Validate handshake, but first verify if node identifier is set to default (all 0's)
        // which indicates node id matching is not required.
        if (this.nodeId.equals(UUID.fromString("00000000-0000-0000-0000-000000000000"))) {
            log.info("channelRead: node id matching is not requested by client.");
        } else if (!this.nodeId.equals(serverId)) {
            // Validation failed, client opened a socket to server with id
            // 'nodeId', instead server's id is 'serverId'
            log.error("channelRead: Handshake validation failed. Server node id mismatch.");
            log.debug("channelRead: Client opened socket to server [{}] instead, connected to: [{}]",
                    this.nodeId, serverId);
            this.fireHandshakeFailed(ctx);
            return;
        }

        log.info("channelRead: Handshake succeeded. Server Corfu Version: [{}]", corfuVersion);
        log.debug("channelRead: There are [{}] messages in queue to be flushed.", this.messages.size());
        // Flush messages in queue
        while (!messages.isEmpty()) {
            ctx.writeAndFlush(messages.poll());
        }

        // Remove this handler from the pipeline; handshake is completed.
        log.info("channelRead: Removing handshake handler from pipeline.");
        ctx.pipeline().remove(this);
        this.fireHandshakeSucceeded(ctx);
    }

    /**
     * Channel event that is triggered when a new connected channel is created.
     *
     * @param ctx channel handler context
     * @throws Exception
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx)
        throws Exception {
        log.info("channelActive: Outgoing connection established to: {}", ctx.channel().remoteAddress());

        // Write the handshake & add a timeout listener.
        CorfuMsg handshake = CorfuMsgType.HANDSHAKE_INITIATE
            .payloadMsg(new HandshakeMsg(this.clientId, this.nodeId));

        log.info("channelActive: Initiate handshake. Send handshake message.");
        ctx.writeAndFlush(handshake);
        log.debug("channelActive: Add {} to channel pipeline.", READ_TIMEOUT_HANDLER);
        ctx.pipeline().addBefore(ctx.name(), READ_TIMEOUT_HANDLER, new ReadTimeoutHandler(this.handshakeTimeout));
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
    public void exceptionCaught(ChannelHandlerContext ctx,
        java.lang.Throwable cause) throws Exception {
        log.error("exceptionCaught: Exception {} caught.", cause.getClass().getSimpleName(), cause);
        if (cause instanceof ReadTimeoutException) {
            // Handshake has failed or completed. If none is True, handshake timed out.
            if (this.handshakeState.failed()) {
                log.debug("exceptionCaught: Handshake timeout checker: already failed.");
                return;
            }

            if (!this.handshakeState.completed()) {
                // If handshake did not complete nor failed, it timed out.
                // Force failure.
                log.error("exceptionCaught: Handshake timeout checker: timed out." +
                    " Close Connection.");
                this.handshakeState.set(true, false);
                ctx.channel().close();
            } else {
                // Handshake completed successfully,
                log.debug("exceptionCaught: Handshake timeout checker: discarded " +
                    "(handshake OK)");
            }
        }
        if (ctx.channel().isOpen()) {
            ctx.channel().close();
        } else {
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
        if (this.handshakeState.failed()) {
            return;
        }

        // If the handshake hasn't failed but completed meanwhile and
        // messages still passed through this handler, then forward
        // them downwards.
        if (this.handshakeState.completed()) {
            super.write(ctx, msg, promise);
        } else {
            // Otherwise, queue messages in order until the handshake
            // completes.
            this.messages.offer((CorfuMsg) msg);
        }
    }

    /**
     * Signal handshake as failed.
     *
     * @param ctx channel handler context
     */
    private void fireHandshakeFailed(ChannelHandlerContext ctx) {
        this.handshakeState.set(true, true);
        log.error("fireHandshakeFailed: Handshake Failed. Close Channel.");
        // Let downstream handlers know the handshake failed
        ctx.fireUserEventTriggered(ClientHandshakeEvent.FAILED);
        ctx.channel().close();
    }

    /**
     * Signal handshake as succeeded.
     */
    private void fireHandshakeSucceeded(ChannelHandlerContext ctx) {
        this.handshakeState.set(false, true);
        // Let downstream handlers know the handshake succeeded.
        ctx.fireUserEventTriggered(ClientHandshakeEvent.CONNECTED);
    }
}
