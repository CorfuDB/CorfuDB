package org.corfudb.infrastructure;

import com.google.protobuf.TextFormat;
import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.WrongClusterMsg;
import org.corfudb.runtime.proto.RpcCommon.UuidMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.view.Layout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.CorfuProtocolServerErrors.getWrongClusterErrorMsg;
import static org.corfudb.protocols.CorfuProtocolServerErrors.getWrongEpochErrorMsg;
import static org.corfudb.protocols.CorfuProtocolServerErrors.getNotBootstrappedErrorMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;

/**
 * Created by mwei on 12/13/15.
 */
public interface IServerRouter {

    // Lombok annotations are not allowed for the interfaces.
    Logger log = LoggerFactory.getLogger(IServerRouter.class);

    @Deprecated
    void sendResponse(ChannelHandlerContext ctx, CorfuMsg inMsg, CorfuMsg outMsg);

    void sendResponse(ResponseMsg response, ChannelHandlerContext ctx);

    /**
     * Get the current epoch.
     */
    long getServerEpoch();

    /**
     * Set the current epoch.
     */
    void setServerEpoch(long newEpoch);

    /**
     * Get the currently bootstrapped layout.
     */
    Optional<Layout> getCurrentLayout();

    /**
     * Register a server to route messages to.
     * @param server The server to route messages to
     */
    void addServer(AbstractServer server);

    /**
     * Get a list of registered servers.
     */
    List<AbstractServer> getServers();

    /**
     * Set a serverContext for this router.
     * @param serverContext A current server context.
     */
    void setServerContext(ServerContext serverContext);

    /**
     * @deprecated [RM]
     * Send WRONG_EPOCH message.
     *
     * @param msg The incoming message.
     * @param ctx The context of the channel handler.
     */
    @Deprecated
    default void sendWrongEpochMessage(CorfuMsg msg, ChannelHandlerContext ctx) {
        sendResponse(ctx, msg, new CorfuPayloadMsg<>(CorfuMsgType.WRONG_EPOCH, getServerEpoch()));
        log.trace("Incoming message with wrong epoch, got {}, expected {}, message was: {}",
                msg.getEpoch(), getServerEpoch(), msg);
    }

    /**
     * @deprecated [RM]
     * Send LAYOUT_NOBOOTSTRAP message.
     *
     * @param msg The incoming message.
     * @param ctx The context of the channel handler.
     */
    @Deprecated
    default void sendNoBootstrapMessage(CorfuMsg msg, ChannelHandlerContext ctx) {
        sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.LAYOUT_NOBOOTSTRAP));
        log.trace("Received message but not bootstrapped! Message={}", msg);
    }

    /**
     * @deprecated [RM]
     * Send WRONG_CLUSTER_ID message.
     *
     * @param msg              The incoming message.
     * @param ctx              The context of the channel handler.
     * @param currentClusterID The current cluster id.
     */
    @Deprecated
    default void sendWrongClusterIdMessage(CorfuMsg msg, ChannelHandlerContext ctx, UUID currentClusterID) {
        sendResponse(ctx, msg, new CorfuPayloadMsg<>(CorfuMsgType.WRONG_CLUSTER_ID,
                new WrongClusterMsg(currentClusterID, msg.getClusterID())));
        log.trace("Incoming message with a wrong cluster ID, got {}, expected {}, message was {}",
                msg.getClusterID(), currentClusterID, msg);
    }

    /**
     * @deprecated [RM]
     * Validate the epoch of a CorfuMsg, and send a WRONG_EPOCH response if
     * the server is in the wrong epoch. Ignored if the message type is reset (which
     * is valid in any epoch).
     *
     * @param msg The incoming message to validate.
     * @param ctx The context of the channel handler.
     * @return True, if the epoch is correct, but false otherwise.
     */
    @Deprecated
    default boolean epochIsValid(CorfuMsg msg, ChannelHandlerContext ctx) {
        long serverEpoch = getServerEpoch();
        if (msg.getEpoch() != serverEpoch) {
            sendWrongEpochMessage(msg, ctx);
            return false;
        }
        return true;
    }

    /**
     * @deprecated [RM]
     * Validate that the message's cluster ID is equal to the cluster ID of a bootstrapped layout.
     *
     * @param msg           The incoming message.
     * @param ctx           The context of the channel handler.
     * @param currentLayout The layout a server was bootstrapped with.
     * @return True, if the message's cluster ID is equal to this node's cluster ID, otherwise false.
     */
    @Deprecated
    default boolean clusterIdIsValid(CorfuMsg msg, ChannelHandlerContext ctx, Layout currentLayout) {
        UUID currentClusterID = currentLayout.getClusterId();

        boolean clusterIdsMatch = msg.getClusterID()
                .equals(currentClusterID);

        if (!clusterIdsMatch) {
            sendWrongClusterIdMessage(msg, ctx, currentClusterID);
        }
        return clusterIdsMatch;
    }

    /**
     * @deprecated [RM]
     * Validate the incoming message. The message is valid if:
     * 1) The flag ignoreEpoch is set to true or it's set to false, and the epoch is valid for all the messages.
     * 2) Also if the flag ignoreClusterId is set to false,
     *      a. The current layout server should be bootstrapped and
     *      b. the message's cluster ID should be equal to the bootstrapped layout's cluster ID.
     *
     * @param msg The incoming message.
     * @param ctx The context of the channel handler.
     * @return True, if it's a valid message, and false otherwise.
     */
    @Deprecated
    default boolean messageIsValid(CorfuMsg msg, ChannelHandlerContext ctx) {
        if (!msg.getMsgType().ignoreEpoch && !epochIsValid(msg, ctx)) {
            return false;
        }

        if(!msg.getMsgType().ignoreClusterId){
            return getCurrentLayout()
                    .map(layout -> clusterIdIsValid(msg, ctx, layout))
                    .orElseGet(() -> {
                        sendNoBootstrapMessage(msg, ctx);
                        return false;
                    });
        }
        return true;
    }

    /**
     * @deprecated [RM]
     * Send Response through a generic transport layer (not Netty-specific)
     *
     * Currently, this is used for the Log Replication Server which supports any
     * custom defined transport layer, a default implementation is available to
     * avoid empty implementations on CorfuServer.
     *
     * @param inMsg
     * @param outMsg
     */
    @Deprecated
    default void sendResponse(CorfuMsg inMsg, CorfuMsg outMsg) {}

    /**
     * Send a WRONG_EPOCH error response.
     *
     * @param requestHeader The header of the incoming request.
     * @param ctx The context of the channel handler.
     */
    default void sendWrongEpochError(HeaderMsg requestHeader, ChannelHandlerContext ctx) {
        final long correctEpoch = getServerEpoch();

        HeaderMsg responseHeader = getHeaderMsg(requestHeader, false, true);
        ResponseMsg response = getResponseMsg(responseHeader, getWrongEpochErrorMsg(correctEpoch));
        sendResponse(response, ctx);

        if (log.isTraceEnabled()) {
            log.trace("sendWrongEpochError[{}]: Incoming request received with wrong epoch, got {}, expected {}, " +
                            "request was {}", requestHeader.getRequestId(), requestHeader.getEpoch(),
                    correctEpoch, TextFormat.shortDebugString(requestHeader));
        }
    }

    /**
     * Send a NOT_BOOTSTRAPPED error response.
     *
     * @param requestHeader The header of the incoming request.
     * @param ctx The context of the channel handler.
     */
    default void sendNoBootstrapError(HeaderMsg requestHeader, ChannelHandlerContext ctx) {
        HeaderMsg responseHeader = getHeaderMsg(requestHeader, false, true);
        ResponseMsg response = getResponseMsg(responseHeader, getNotBootstrappedErrorMsg());
        sendResponse(response, ctx);

        if (log.isTraceEnabled()) {
            log.trace("sendNoBootstrapError[{}]: Received request but not bootstrapped! Request was {}",
                    requestHeader.getRequestId(), TextFormat.shortDebugString(requestHeader));
        }
    }

    /**
     * Send a WRONG_CLUSTER error response.
     *
     * @param requestHeader The header of the incoming request.
     * @param ctx The context of the channel handler.
     * @param clusterId The current cluster id.
     */
    default void sendWrongClusterError(HeaderMsg requestHeader, ChannelHandlerContext ctx, UuidMsg clusterId) {
        HeaderMsg responseHeader = getHeaderMsg(requestHeader, false, true);
        ResponseMsg response = getResponseMsg(responseHeader,
                getWrongClusterErrorMsg(clusterId, requestHeader.getClusterId()));

        sendResponse(response, ctx);

        log.trace("sendWrongClusterError[{}]: Incoming request with a wrong cluster id, got {}, expected {}, " +
                "request was: {}", requestHeader.getRequestId(),
                TextFormat.shortDebugString(requestHeader.getClusterId()),
                TextFormat.shortDebugString(clusterId), TextFormat.shortDebugString(requestHeader));
    }

    /**
     * Validate the epoch of an incoming request.
     *
     * @param requestHeader The header of the incoming request to validate.
     * @return True, if the epoch is correct, but false otherwise.
     */
    default boolean epochIsValid(HeaderMsg requestHeader) {
        final long serverEpoch = getServerEpoch();
        return requestHeader.getEpoch() == serverEpoch;
    }

    /**
     * Validate that the cluster ID of the incoming request is equal to the
     * cluster ID of a bootstrapped layout.
     *
     * @param requestHeader The header of the incoming request to validate.
     * @param currentClusterID The cluster ID of the layout the server was bootstrapped with.
     * @return True, if the cluster ID of the request matches the cluster ID of the layout, but false otherwise.
     */
    default boolean clusterIdIsValid(HeaderMsg requestHeader, UuidMsg currentClusterID) {
        return requestHeader.getClusterId().equals(currentClusterID);
    }

    /**
     * Validate the incoming request and send an error response if the request is invalid.
     * The request is valid if:
     *    1) The flag ignoreEpoch is set to true, or it's set to false and the epoch is valid.
     *    2) Also, if the flag ignoreClusterId is set to false,
     *           a. The current layout server should be bootstrapped and
     *           b. the request's cluster ID should be equal to the bootstrapped layout's cluster ID.
     *
     * @param req The incoming request.
     * @param ctx The context of the channel handler.
     * @return True if the request is valid, and false otherwise.
     */
    default boolean validateRequest(RequestMsg req, ChannelHandlerContext ctx) {
        HeaderMsg requestHeader = req.getHeader();

        if (!requestHeader.getIgnoreEpoch() && !epochIsValid(requestHeader)) {
            sendWrongEpochError(requestHeader, ctx);
            return false;
        }

        if (requestHeader.getIgnoreClusterId()) {
            return true;
        }

        return getCurrentLayout()
                .map(layout -> {
                    final UuidMsg currentClusterID = getUuidMsg(layout.getClusterId());
                    if (!clusterIdIsValid(requestHeader, currentClusterID)) {
                        sendWrongClusterError(requestHeader, ctx, currentClusterID);
                        return false;
                    }

                    return true;
                }).orElseGet(() -> {
                    sendNoBootstrapError(requestHeader, ctx);
                    return false;
                });
    }
}
