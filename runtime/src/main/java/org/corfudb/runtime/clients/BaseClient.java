package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.ExceptionMsg;
import org.corfudb.protocols.wireprotocol.JSONPayloadMsg;
import org.corfudb.protocols.wireprotocol.VersionInfo;
import org.corfudb.runtime.exceptions.ServerNotReadyException;
import org.corfudb.runtime.exceptions.ShutdownException;
import org.corfudb.runtime.exceptions.WrongEpochException;

/**
 * This is a base client which processes basic messages.
 * It mainly handles PINGs, as well as the ACK/NACKs defined by
 * the Corfu protocol.
 *
 * <p>Created by mwei on 12/9/15.
 */
@Slf4j
public class BaseClient implements IClient {

    /**
     * The router to use for the client.
     */
    @Getter
    @Setter
    public IClientRouter router;

    /** Public functions which are exposed to clients. */

    /**
     * Ping the endpoint, synchronously.
     *
     * @return True, if the endpoint was reachable, false otherwise.
     */
    public boolean pingSync() {
        try {
            return ping().get();
        } catch (Exception e) {
            log.error("Ping failed due to exception", e);
            return false;
        }
    }

    /**
     * Sets the epoch on client router and on the target layout server.
     *
     * @param newEpoch New Epoch to be set
     * @return Completable future which returns true on successful epoch set.
     */
    public CompletableFuture<Boolean> setRemoteEpoch(long newEpoch) {
        return router.sendMessageAndGetCompletable(
                new CorfuPayloadMsg<>(CorfuMsgType.SET_EPOCH, newEpoch));
    }

    public CompletableFuture<VersionInfo> getVersionInfo() {
        return router.sendMessageAndGetCompletable(
                new CorfuMsg(CorfuMsgType.VERSION_REQUEST));
    }


    /**
     * Ping the endpoint, asynchronously.
     *
     * @return A completable future which will be completed with True if
     *     the endpoint is reachable, otherwise False or exceptional completion.
     */
    public CompletableFuture<Boolean> ping() {
        return router.sendMessageAndGetCompletable(
                new CorfuMsg(CorfuMsgType.PING));
    }

    /**
     * Reset the endpoint, asynchronously.
     * WARNING: ALL EXISTING DATA ON THIS NODE WILL BE LOST.
     *
     * @return A completable future which will be completed with True if
     *     the endpoint acks, otherwise False or exceptional completion.
     */
    public CompletableFuture<Boolean> reset() {
        return router.sendMessageAndGetCompletable(new CorfuMsg(CorfuMsgType.RESET));
    }

    /**
     * Restart the endpoint, asynchronously.
     *
     * @return A completable future which will be completed with True if
     *     the endpoint acks, otherwise False or exceptional completion.
     */
    public CompletableFuture<Boolean> restart() {
        return router.sendMessageAndGetCompletable(new CorfuMsg(CorfuMsgType.RESTART));
    }

    /**
     * The handler and handlers which implement this client.
     */
    @Getter
    public ClientMsgHandler msgHandler = new ClientMsgHandler(this)
            .generateHandlers(MethodHandles.lookup(), this);

    /**
     * Handle a ping request from the server.
     *
     * @param msg The ping request message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return The return value, null since this is a message from the server.
     */
    @ClientHandler(type = CorfuMsgType.PING)
    private static Object handlePing(CorfuMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        r.sendResponseToServer(ctx, msg, new CorfuMsg(CorfuMsgType.PONG));
        return null;
    }

    /**
     * Handle a pong response from the server.
     *
     * @param msg The ping request message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return Always True, since the ping message was successful.
     */
    @ClientHandler(type = CorfuMsgType.PONG)
    private static Object handlePong(CorfuMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return true;
    }

    /**
     * Handle an ACK response from the server.
     *
     * @param msg The ping request message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return Always True, since the ACK message was successful.
     */
    @ClientHandler(type = CorfuMsgType.ACK)
    private static Object handleAck(CorfuMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return true;
    }

    /**
     * Handle a NACK response from the server.
     *
     * @param msg The ping request message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return Always True, since the ACK message was successful.
     */
    @ClientHandler(type = CorfuMsgType.NACK)
    private static Object handleNack(CorfuMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return false;
    }

    /**
     * Handle a WRONG_EPOCH response from the server.
     *
     * @param msg The wrong epoch message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return none, throw a wrong epoch exception instead.
     */
    @ClientHandler(type = CorfuMsgType.WRONG_EPOCH)
    private static Object handleWrongEpoch(CorfuPayloadMsg<Long> msg, ChannelHandlerContext ctx,
                                           IClientRouter r) {
        throw new WrongEpochException(msg.getPayload());
    }

    /**
     * Handle a Version response from the server.
     *
     * @param msg The version message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return The versioninfo object.
     */
    @ClientHandler(type = CorfuMsgType.VERSION_RESPONSE)
    private static Object handleVersionResponse(JSONPayloadMsg<VersionInfo> msg,
                                                ChannelHandlerContext ctx, IClientRouter r) {
        return msg.getPayload();
    }

    @ClientHandler(type = CorfuMsgType.NOT_READY)
    private static Object handleNotReady(CorfuMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        throw new ServerNotReadyException();
    }

    /** Generic handler for a server exception. */
    @ClientHandler(type = CorfuMsgType.ERROR_SERVER_EXCEPTION)
    private static Object handleServerException(CorfuPayloadMsg<ExceptionMsg> msg,
                                                ChannelHandlerContext ctx, IClientRouter r)
        throws Throwable {
        log.warn("Server threw exception for request {}", msg.getRequestID(),
                msg.getPayload().getThrowable());
        throw msg.getPayload().getThrowable();
    }

    /**
     * Handle a ERROR_SHUTDOWN_EXCEPTION response from the server.
     *
     * @param msg The shutdown exception message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return none, throw a shutdown exception instead.
     */
    @ClientHandler(type = CorfuMsgType.ERROR_SHUTDOWN_EXCEPTION)
    private static Object handleShutdownException(CorfuMsg msg, ChannelHandlerContext ctx,
                                                  IClientRouter r) {
        throw new ShutdownException();
    }
}
