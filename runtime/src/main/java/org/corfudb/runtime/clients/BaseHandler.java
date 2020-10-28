package org.corfudb.runtime.clients;

import com.google.protobuf.ByteString;
import io.netty.channel.ChannelHandlerContext;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.lang.invoke.MethodHandles;
import java.util.UUID;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.CorfuProtocolCommon;
import org.corfudb.protocols.service.CorfuProtocolBase;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.ExceptionMsg;
import org.corfudb.protocols.wireprotocol.JSONPayloadMsg;
import org.corfudb.protocols.wireprotocol.VersionInfo;
import org.corfudb.protocols.wireprotocol.WrongClusterMsg;
import org.corfudb.runtime.exceptions.ServerNotReadyException;
import org.corfudb.runtime.exceptions.WrongClusterException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.exceptions.NoBootstrapException;

import org.corfudb.runtime.proto.ServerErrors.WrongClusterErrorMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase;
import org.corfudb.runtime.proto.ServerErrors.ServerErrorMsg.ErrorCase;
import org.corfudb.runtime.proto.service.Base.VersionResponseMsg;

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
     * For old CorfuMsg, use {@link #msgHandler}
     * The handler and handlers which implement this client.
     */
    @Getter
    public ClientResponseHandler responseHandler = new ClientResponseHandler(this)
            .generateHandlers(MethodHandles.lookup(), this)
            .generateErrorHandlers(MethodHandles.lookup(), this);

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
        CorfuMsg outMsg = new CorfuMsg(CorfuMsgType.PONG);
        outMsg.copyBaseFields(msg);
        ctx.writeAndFlush(outMsg, ctx.voidPromise());
        log.trace("Sent response: {}", outMsg);
        return null;
    }

    /**
     * Handle a pong response from the server.
     * For protobuf, use {@link #handlePingResponse(ResponseMsg,
     * ChannelHandlerContext, IClientRouter)}
     *
     * @param msg The ping request message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return Always True, since the ping message was successful.
     */
    @ClientHandler(type = CorfuMsgType.PONG)
    @Deprecated
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
    @Deprecated
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
    @Deprecated
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
     * For protobuf, use {@link #handleVersionResponse(ResponseMsg, ChannelHandlerContext, IClientRouter)}
     *
     * @param msg The version message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return The versioninfo object.
     */
    @ClientHandler(type = CorfuMsgType.VERSION_RESPONSE)
    @Deprecated
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

    /**
     * Handle a wrong cluster id exception.
     *
     * @param msg Wrong cluster id exception message.
     * @param ctx A context the message was sent under.
     * @param r   A reference to the router.
     * @return None, throw a wrong cluster id exception.
     */
    @ClientHandler(type = CorfuMsgType.WRONG_CLUSTER_ID)
    private static Object handleWrongClusterId(CorfuPayloadMsg<WrongClusterMsg> msg,
                                               ChannelHandlerContext ctx, IClientRouter r) {
        WrongClusterMsg wrongClusterMessage = msg.getPayload();
        throw new WrongClusterException(wrongClusterMessage.getServerClusterId(),
                wrongClusterMessage.getClientClusterId());
    }

    // Protobuf region

    /**
     * Handle a ping response from the server.
     * For old CorfuMsg, use {@link #handlePong(CorfuMsg, ChannelHandlerContext, IClientRouter)}
     *
     * @param msg The ping response message.
     * @param ctx The context the message was sent under.
     * @param r   A reference to the router
     * @return Always True, since the ping message was successful.
     */
    @ResponseHandler(type = PayloadCase.PING_RESPONSE)
    private static Object handlePingResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                             IClientRouter r) {
        log.info("Received PING_RESPONSE from the server - {}", msg);
        return true;
    }

    /**
     * Handle a restart response from the server.
     *
     * @param msg The ping response message.
     * @param ctx The context the message was sent under.
     * @param r   A reference to the router
     * @return Always True, since the restart message was successful.
     */
    @ResponseHandler(type = PayloadCase.RESTART_RESPONSE)
    private static Object handleRestartResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                IClientRouter r) {
        return true;
    }

    /**
     * Handle a reset response from the server.
     *
     * @param msg The ping response message.
     * @param ctx The context the message was sent under.
     * @param r   A reference to the router
     * @return Always True, since the reset message was successful.
     */
    @ResponseHandler(type = PayloadCase.RESET_RESPONSE)
    private static Object handleResetResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                              IClientRouter r) {
        log.info("Received RESET_RESPONSE from the server - {}", msg);
        return true;
    }

    /**
     * Handle a seal response from the server.
     *
     * @param msg The ping response message.
     * @param ctx The context the message was sent under.
     * @param r   A reference to the router
     * @return Always True, since the seal message was successful.
     */
    @ResponseHandler(type = PayloadCase.SEAL_RESPONSE)
    private static Object handleSealResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                             IClientRouter r) {
        return true;
    }

    /**
     * Handle a version response from the server.
     * For old CorfuMsg, use {@link #handleVersionResponse(JSONPayloadMsg, ChannelHandlerContext, IClientRouter)}
     *
     * @param msg The ping response message.
     * @param ctx The context the message was sent under.
     * @param r   A reference to the router
     * @return The VersionInfo object fetched from response msg.
     */
    @ResponseHandler(type = PayloadCase.VERSION_RESPONSE)
    private static Object handleVersionResponse(ResponseMsg msg, ChannelHandlerContext ctx,
                                                IClientRouter r) {
        VersionResponseMsg versionResponseMsg = msg.getPayload().getVersionResponse();

        return CorfuProtocolBase.getVersionInfo(versionResponseMsg);
    }


    /**
     * Handle a UNKNOWN_ERROR response from the server.
     * For old CorfuMsg, use {@link #handleServerException(CorfuPayloadMsg, ChannelHandlerContext, IClientRouter)}
     *
     * @param msg The wrong epoch message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return none, throw a wrong epoch exception instead.
     */
    @ServerErrorsHandler(type = ErrorCase.UNKNOWN_ERROR)
    private static Object handleUnknownError(ResponseMsg msg, ChannelHandlerContext ctx,
                                             IClientRouter r) throws Throwable {
        ByteString bs = msg.getPayload().getServerError().getUnknownError().getThrowable();
        byte[] bytes = new byte[bs.size()];
        bs.copyTo(bytes, 0);
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bis);

        throw (Throwable) ois.readObject();
    }

    /**
     * Handle a WRONG_EPOCH_ERROR response from the server.
     * For old CorfuMsg, use {@link #handleWrongEpoch(CorfuPayloadMsg, ChannelHandlerContext, IClientRouter)}
     *
     * @param msg The wrong epoch message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return none, throw a wrong epoch exception instead.
     */
    @ServerErrorsHandler(type = ErrorCase.WRONG_EPOCH_ERROR)
    private static Object handleWrongEpochError(ResponseMsg msg, ChannelHandlerContext ctx,
                                                IClientRouter r) {
        long correctEpoch = msg.getPayload().getServerError().getWrongEpochError().getCorrectEpoch();
        throw new WrongEpochException(correctEpoch);
    }

    /**
     * Handle a NOT_READY_ERROR response from the server.
     * For old CorfuMsg, use {@link #handleNotReady(CorfuMsg, ChannelHandlerContext, IClientRouter)}
     *
     * @param msg The wrong epoch message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return none, throw a wrong epoch exception instead.
     */
    @ServerErrorsHandler(type = ErrorCase.NOT_READY_ERROR)
    private static Object handleNotReadyError(ResponseMsg msg, ChannelHandlerContext ctx,
                                              IClientRouter r) {
        throw new ServerNotReadyException();
    }

    /**
     * Handle a WRONG_CLUSTER_ERROR response from the server.
     * For old CorfuMsg, use {@link #handleWrongClusterId(CorfuPayloadMsg, ChannelHandlerContext, IClientRouter)}
     *
     * @param msg The wrong epoch message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return none, throw a wrong epoch exception instead.
     */
    @ServerErrorsHandler(type = ErrorCase.WRONG_CLUSTER_ERROR)
    private static Object handleWrongClusterError(ResponseMsg msg, ChannelHandlerContext ctx,
                                                  IClientRouter r) {
        WrongClusterErrorMsg errorMsg = msg.getPayload().getServerError().getWrongClusterError();
        UUID expectedCluster = CorfuProtocolCommon.getUUID(errorMsg.getExpectedClusterId());
        UUID actualCluster = CorfuProtocolCommon.getUUID(errorMsg.getProvidedClusterId());

        throw new WrongClusterException(expectedCluster, actualCluster);
    }

    /**
     * Handle a BOOTSTRAPPED_ERROR response from the server.
     *
     * @param msg The wrong epoch message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return none, throw a wrong epoch exception instead.
     */
    @ServerErrorsHandler(type = ErrorCase.BOOTSTRAPPED_ERROR)
    private static Object handleBootStrappedError(ResponseMsg msg, ChannelHandlerContext ctx,
                                                  IClientRouter r) throws AlreadyBootstrappedException {
        throw new AlreadyBootstrappedException();
    }

    /**
     * Handle a NOT_BOOTSTRAPPED_ERROR response from the server.
     *
     * @param msg The wrong epoch message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return none, throw a wrong epoch exception instead.
     */
    @ServerErrorsHandler(type = ErrorCase.NOT_BOOTSTRAPPED_ERROR)
    private static Object handleNotBootstrappedError(ResponseMsg msg, ChannelHandlerContext ctx,
                                                     IClientRouter r) throws NoBootstrapException {
        throw new NoBootstrapException();
    }

    // End region
}
