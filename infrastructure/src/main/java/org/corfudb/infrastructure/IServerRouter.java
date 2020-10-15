package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import org.corfudb.protocols.API;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.WrongClusterMsg;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.protocol.proto.CorfuProtocol;
import org.corfudb.runtime.view.Layout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Created by mwei on 12/13/15.
 */
public interface IServerRouter {

    // Lombok annotations are not allowed for the interfaces.
    Logger log = LoggerFactory.getLogger(IServerRouter.class);

    // Remove this after Protobuf for RPC Completion
    void sendResponse(ChannelHandlerContext ctx, CorfuMsg inMsg, CorfuMsg outMsg);

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
     * Remove this after Protobuf for RPC Completion
     *
     * Send WRONG_EPOCH message.
     *
     * @param msg The incoming message.
     * @param ctx The context of the channel handler.
     */
    default void sendWrongEpochMessage(CorfuMsg msg, ChannelHandlerContext ctx) {
        sendResponse(ctx, msg, new CorfuPayloadMsg<>(CorfuMsgType.WRONG_EPOCH,
                getServerEpoch()));
        log.trace("Incoming message with wrong epoch, got {}, expected {}, message was: {}",
                msg.getEpoch(), getServerEpoch(), msg);
    }

    /**
     * Remove this after Protobuf for RPC Completion
     *
     * Send LAYOUT_NOBOOTSTRAP message.
     *
     * @param msg The incoming message.
     * @param ctx The context of the channel handler.
     */
    default void sendNoBootstrapMessage(CorfuMsg msg, ChannelHandlerContext ctx) {
        sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.LAYOUT_NOBOOTSTRAP));
        log.trace("Received message but not bootstrapped! Message={}", msg);
    }

    /**
     * Remove this after Protobuf for RPC Completion
     *
     * Send WRONG_CLUSTER_ID message.
     *
     * @param msg              The incoming message.
     * @param ctx              The context of the channel handler.
     * @param currentClusterID The current cluster id.
     */
    default void sendWrongClusterIdMessage(CorfuMsg msg, ChannelHandlerContext ctx, UUID currentClusterID) {
        sendResponse(ctx, msg, new CorfuPayloadMsg<>(CorfuMsgType.WRONG_CLUSTER_ID,
                new WrongClusterMsg(currentClusterID, msg.getClusterID())));
        log.trace("Incoming message with a wrong cluster ID, got {}, expected {}, message was {}",
                msg.getClusterID(), currentClusterID, msg);
    }

    /**
     * Remove this after Protobuf for RPC Completion
     *
     * Validate the epoch of a CorfuMsg, and send a WRONG_EPOCH response if
     * the server is in the wrong epoch. Ignored if the message type is reset (which
     * is valid in any epoch).
     *
     * @param msg The incoming message to validate.
     * @param ctx The context of the channel handler.
     * @return True, if the epoch is correct, but false otherwise.
     */
    default boolean epochIsValid(CorfuMsg msg, ChannelHandlerContext ctx) {
        long serverEpoch = getServerEpoch();
        if (msg.getEpoch() != serverEpoch) {
            sendWrongEpochMessage(msg, ctx);
            return false;
        }
        return true;
    }

    /**
     * Remove this after Protobuf for RPC Completion
     *
     * Validate that the message's cluster ID is equal to the cluster ID of a bootstrapped layout.
     *
     * @param msg           The incoming message.
     * @param ctx           The context of the channel handler.
     * @param currentLayout The layout a server was bootstrapped with.
     * @return True, if the message's cluster ID is equal to this node's cluster ID, otherwise false.
     */
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
     * Remove this after Protobuf for RPC Completion
     *
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
     * Remove this after Protobuf for RPC Completion
     *
     * Send Response through a generic transport layer (not Netty-specific)
     *
     * Currently, this is used for the Log Replication Server which supports any
     * custom defined transport layer, a default implementation is available to
     * avoid empty implementations on CorfuServer.
     *
     * @param inMsg
     * @param outMsg
     */
    default void sendResponse(CorfuMsg inMsg, CorfuMsg outMsg) {}

    /**
     * Send a WRONG_EPOCH error response.
     * @param requestHeader The header of the incoming request.
     * @param ctx The context of the channel handler.
     * @param correctEpoch The current epoch.
     */
    default void sendWrongEpochError(CorfuProtocol.Header requestHeader, ChannelHandlerContext ctx, long correctEpoch) {
        CorfuProtocol.Header responseHeader = API.generateResponseHeader(requestHeader, false, true);
        CorfuProtocol.Response response = API.getErrorResponseNoPayload(responseHeader, API.getWrongEpochServerError(correctEpoch));
        sendResponse(response, ctx);

        log.trace("sendWrongEpochError[{}]: Incoming request received with wrong epoch, got {}, expected {}, " +
                "request was {}", requestHeader.getRequestId(), requestHeader.getEpoch(), correctEpoch, requestHeader);
    }

    default void sendWrongEpochError(CorfuProtocol.Header requestHeader, ChannelHandlerContext ctx) {
        sendWrongEpochError(requestHeader, ctx, getServerEpoch());
    }

    /**
     * Send a NOT_BOOTSTRAPPED error response.
     * @param requestHeader The header of the incoming request.
     * @param ctx The context of the channel handler.
     */
    default void sendNoBootstrapError(CorfuProtocol.Header requestHeader, ChannelHandlerContext ctx) {
        CorfuProtocol.Header responseHeader = API.generateResponseHeader(requestHeader, false, true);
        CorfuProtocol.Response response = API.getErrorResponseNoPayload(responseHeader, API.getNotBootstrappedServerError());
        sendResponse(response, ctx);

        log.trace("sendNoBootstrapError[{}]: Received request but not bootstrapped! Request was {}",
                requestHeader.getRequestId(), requestHeader);
    }

    /**
     * Send a WRONG_CLUSTER error response.
     * @param requestHeader The header of the incoming request.
     * @param ctx The context of the channel handler.
     * @param clusterId The current cluster id.
     */
    default void sendWrongClusterError(CorfuProtocol.Header requestHeader, ChannelHandlerContext ctx, CorfuProtocol.UUID clusterId) {
        CorfuProtocol.Header responseHeader = API.generateResponseHeader(requestHeader, false, true);
        CorfuProtocol.ServerError wrongClusterError = API.getWrongClusterServerError(clusterId, requestHeader.getClusterId());
        CorfuProtocol.Response response = API.getErrorResponseNoPayload(responseHeader, wrongClusterError);
        sendResponse(response, ctx);

        log.trace("sendWrongClusterError[{}]: Incoming request with a wrong cluster id, got {}, expected {}, " +
                "request was: {}", requestHeader.getRequestId(), requestHeader.getClusterId(), clusterId, requestHeader);
    }

    /**
     * Validate the epoch of an incoming request and send a WRONG_EPOCH error response
     * if the server is in the wrong epoch.
     * @param requestHeader The header of the incoming request to validate.
     * @param ctx The context of the channel handler.
     * @return True, if the epoch is correct, but false otherwise.
     */
    default boolean epochIsValid(CorfuProtocol.Header requestHeader, ChannelHandlerContext ctx) {
        final long serverEpoch = getServerEpoch();
        if(requestHeader.getEpoch() != serverEpoch) {
            sendWrongEpochError(requestHeader, ctx);
            return false;
        }

        return true;
    }

    /**
     * Validate that the cluster ID of the incoming request is equal to the cluster ID of a
     * bootstrapped layout. If this is not the case, send a WRONG_CLUSTER error response.
     * @param requestHeader The header of the incoming request to validate.
     * @param ctx The context of the channel handler.
     * @param layout The layout a server was bootstrapped with.
     * @return True, if the cluster ID of the request matches the cluster ID of the layout, but false otherwise.
     */
    default boolean clusterIdIsValid(CorfuProtocol.Header requestHeader, ChannelHandlerContext ctx, Layout layout) {
        CorfuProtocol.UUID currentClusterID = API.getProtoUUID(layout.getClusterId());
        boolean match = requestHeader.getClusterId().equals(currentClusterID);

        if(!match) {
            sendWrongClusterError(requestHeader, ctx, currentClusterID);
        }

        return match;
    }

    /**
     * Validate the incoming request. The request is valid if:
     *    1) If the request message has the appropriate payload given the request type.
     *    2) The flag ignoreEpoch is set to true, or it's set to false and the epoch is valid.
     *    3) Also, if the flag ignoreClusterId is set to false,
     *           a. The current layout server should be bootstrapped and
     *           b. the request's cluster ID should be equal to the bootstrapped layout's cluster ID.
     *
     * @param req The incoming request.
     * @param ctx The context of the channel handler.
     * @return True if the request is valid, and false otherwise.
     */
    default boolean requestIsValid(CorfuProtocol.Request req, ChannelHandlerContext ctx) {
        CorfuProtocol.Header requestHeader = req.getHeader();

        if(!API.validateRequestPayloadType(req)) {
            // TODO(Zach): What error response to send here, if any?
            return false;
        }

        if(!requestHeader.getIgnoreEpoch() && epochIsValid(requestHeader, ctx)) { return false; }
        if(!requestHeader.getIgnoreClusterId()) {
            return getCurrentLayout()
                    .map(layout -> clusterIdIsValid(requestHeader, ctx, layout))
                    .orElseGet(() -> {
                        sendNoBootstrapError(requestHeader, ctx);
                        return false;
                    });
        }
        return true;
    }

    /**
     * Send Response through a generic transport layer (not Netty-specific)
     *
     * Currently, this is used for the Log Replication Server which supports any
     * custom defined transport layer, a default implementation is available to
     * avoid empty implementations on CorfuServer.
     *
     * @param response
     * @param ctx
     */
    void sendResponse(CorfuProtocol.Response response, ChannelHandlerContext ctx);
    void sendResponse(CorfuMessage.ResponseMsg response, ChannelHandlerContext ctx);

}
