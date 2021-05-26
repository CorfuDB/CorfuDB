package org.corfudb.protocols.wireprotocol;

import com.google.protobuf.TextFormat;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.ReadTimeoutHandler;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;
import java.util.UUID;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.runtime.proto.service.Base.HandshakeResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.util.GitRepositoryState;

import static org.corfudb.protocols.CorfuProtocolCommon.DEFAULT_UUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getUUID;
import static org.corfudb.protocols.service.CorfuProtocolBase.getHandshakeRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getRequestMsg;

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
    private final Set<RequestMsg> requestMessages = ConcurrentHashMap.newKeySet();
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
            this.nodeId = DEFAULT_UUID;
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
    public void channelRead(ChannelHandlerContext ctx, Object m) throws Exception {
        if (this.handshakeState.failed()) {
            log.warn("channelRead: Dropping the message as the handshake was not completed. Message - {}", m);
            return;
        }

        if (!(m instanceof ResponseMsg)) {
            log.error("channelRead: Message received is not a ResponseMsg type. Message - {}", m);
            return;
        }

        ResponseMsg response = ((ResponseMsg) m);
        long corfuServerVersion = response.getHeader().getVersion().getCorfuSourceCodeVersion();
        long corfuClientVersion = GitRepositoryState.getCorfuSourceCodeVersion();

        if (corfuServerVersion != corfuClientVersion) {
            log.warn("channelRead: Version mismatch. Corfu client version: {}, Corfu server version: {}",
                    Long.toHexString(corfuClientVersion), Long.toHexString(corfuServerVersion));
        }

        if (!response.getPayload().hasHandshakeResponse()) {
            log.warn("channelRead: Non-Handshake Response received. Message - {}", TextFormat.shortDebugString(response));
            if (this.handshakeState.completed()) {
                // Only send upstream if handshake is complete.
                super.channelRead(ctx, m);
            }

            return;
        }

        log.info("channelRead: Handshake Response received. Removing {} from pipeline.", READ_TIMEOUT_HANDLER);

        // Remove the handler from the pipeline. Also remove the reference of the context from
        // the handler so that it does not disconnect the channel.
        ctx.pipeline().remove(READ_TIMEOUT_HANDLER).handlerRemoved(ctx);
        HandshakeResponseMsg handshakeResponse = response.getPayload().getHandshakeResponse();
        UUID serverId = getUUID(handshakeResponse.getServerId());

        // Validate handshake, but first verify if node identifier is set to default (all 0's)
        // which indicates node id matching is not required.
        if (this.nodeId.equals(DEFAULT_UUID)) {
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

        log.info("channelRead: Handshake succeeded. Corfu Server Version: [{}]", Long.toHexString(corfuServerVersion));

        requestMessages.forEach(ctx::writeAndFlush);
        requestMessages.clear();

        // Remove this handler from the pipeline; handshake is completed.
        log.info("channelRead: Removing handshake handler from pipeline.");
        ctx.pipeline().remove(this);
        this.fireHandshakeSucceeded(ctx);
    }

    /**
     * Channel event that is triggered when a new connected channel is created.
     *
     * @param ctx channel handler context
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.info("channelActive: Outgoing connection established to: {} from id={}",
                ctx.channel().remoteAddress(), ctx.channel().localAddress());

        // Note: Some fields in the header are unused during the handshake process.
        HeaderMsg header = getHeaderMsg(0, CorfuMessage.PriorityLevel.NORMAL, 0,
                DEFAULT_UUID, this.clientId, ClusterIdCheck.CHECK, EpochCheck.IGNORE);
        RequestMsg request = getRequestMsg(header, getHandshakeRequestMsg(this.clientId, this.nodeId));

        ctx.writeAndFlush(request);

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
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (this.handshakeState.failed()) {
            return;
        }

        // If the handshake hasn't failed but completed meanwhile and
        // messages still passed through this handler, then forward
        // them downwards.
        if (this.handshakeState.completed()) {
            log.debug("write: Handshake already completed, not appending corfu message to queue");
            super.write(ctx, msg, promise);
        } else {
            // Otherwise, queue messages in order until the handshake completes.
            if (msg instanceof RequestMsg) {
                this.requestMessages.add((RequestMsg) msg);
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
