package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;

import java.lang.invoke.MethodHandles;

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
 * This is a base client which handles basic Corfu messages such as PING, ACK.
 * This is also responsible for handling unknown server exceptions.
 *
 * <p>Created by zlokhandwala on 2/20/18.
 */
@Slf4j
public class BaseHandler implements IClient {

    /**
     * The router to use for the client.
     */
    @Getter
    @Setter
    public IClientRouter router;

    /** Public functions which are exposed to clients. */

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

    /**
     * Generic handler for a server exception.
     */
    @ClientHandler(type = CorfuMsgType.ERROR_SERVER_EXCEPTION)
    private static Object handleServerException(CorfuPayloadMsg<ExceptionMsg> msg,
                                                ChannelHandlerContext ctx, IClientRouter r)
            throws Throwable {
        log.warn("Server threw exception for request {}", msg.getRequestID(),
                msg.getPayload().getThrowable());
        throw msg.getPayload().getThrowable();
    }
}
