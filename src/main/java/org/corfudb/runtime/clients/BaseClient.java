package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.util.ClientMsgHandler;

import java.util.concurrent.CompletableFuture;

/**
 * This is a base client which processes basic messages.
 * It mainly handles PINGs, as well as the ACK/NACKs defined by
 * the Corfu protocol.
 * <p>
 * Created by mwei on 12/9/15.
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
            log.trace("Ping failed due to exception", e);
            return false;
        }
    }

    public CompletableFuture<Boolean> setRemoteEpoch(long newEpoch) {
        // Set our own epoch to this epoch.
        router.setEpoch(newEpoch);
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
     * the endpoint is reachable, otherwise False or exceptional completion.
     */
    public CompletableFuture<Boolean> ping() {
        return router.sendMessageAndGetCompletable(
                new CorfuMsg(CorfuMsgType.PING));
    }

    /**
     * Reset the endpoint, asynchronously.
     *
     * @return A completable future which will be completed with True if
     * the endpoint acks, otherwise False or exceptional completion.
     */
    public CompletableFuture<Boolean> reset() {
        return router.sendMessageAndGetCompletable(
                new CorfuMsg(CorfuMsgType.RESET));
    }

    /** The handler and handlers which implement this client. */
    @Getter
    public ClientMsgHandler msgHandler = new ClientMsgHandler(this)
            .addHandler(CorfuMsgType.PING, BaseClient::handlePing)
            .addHandler(CorfuMsgType.PONG, BaseClient::handlePong)
            .addHandler(CorfuMsgType.ACK, BaseClient::handleAck)
            .addHandler(CorfuMsgType.NACK, BaseClient::handleNack)
            .addHandler(CorfuMsgType.WRONG_EPOCH, BaseClient::handleWrongEpoch)
            .addHandler(CorfuMsgType.VERSION_RESPONSE, BaseClient::handleVersionResponse);

    /** Handle a ping request from the server.
     *
     * @param msg   The ping request message
     * @param ctx   The context the message was sent under
     * @param r     A reference to the router
     * @return      The return value, null since this is a message from the server.
     */
    private static Object handlePing(CorfuMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        r.sendResponseToServer(ctx, msg, new CorfuMsg(CorfuMsgType.PONG));
        return null;
    }

    /** Handle a pong response from the server.
     *
     * @param msg   The ping request message
     * @param ctx   The context the message was sent under
     * @param r     A reference to the router
     * @return      Always True, since the ping message was successful.
     */
    private static Object handlePong(CorfuMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return true;
    }

    /** Handle an ACK response from the server.
     *
     * @param msg   The ping request message
     * @param ctx   The context the message was sent under
     * @param r     A reference to the router
     * @return      Always True, since the ACK message was successful.
     */
    private static Object handleAck(CorfuMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return true;
    }

    /** Handle a NACK response from the server.
     *
     * @param msg   The ping request message
     * @param ctx   The context the message was sent under
     * @param r     A reference to the router
     * @return      Always True, since the ACK message was successful.
     */
    private static Object handleNack(CorfuMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        return false;
    }

    /** Handle a WRONG_EPOCH response from the server.
     *
     * @param msg   The wrong epoch message
     * @param ctx   The context the message was sent under
     * @param r     A reference to the router
     * @return      none, throw a wrong epoch exception instead.
     */
    private static Object handleWrongEpoch(CorfuPayloadMsg<Long> msg, ChannelHandlerContext ctx, IClientRouter r) {
        throw new WrongEpochException(msg.getPayload());
    }

    /** Handle a Version response from the server.
     *
     * @param msg   The version message
     * @param ctx   The context the message was sent under
     * @param r     A reference to the router
     * @return      The versioninfo object.
     */
    private static VersionInfo handleVersionResponse(JSONPayloadMsg<VersionInfo> msg,
                                                ChannelHandlerContext ctx, IClientRouter r) {
        return msg.getPayload();
    }

}
