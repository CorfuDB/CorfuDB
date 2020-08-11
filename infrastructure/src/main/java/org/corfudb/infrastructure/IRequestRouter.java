package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import java.util.Optional;
import org.corfudb.common.protocol.API;
import org.corfudb.common.protocol.proto.CorfuProtocol.UUID;
import org.corfudb.common.protocol.proto.CorfuProtocol.Header;
import org.corfudb.common.protocol.proto.CorfuProtocol.Request;
import org.corfudb.common.protocol.proto.CorfuProtocol.Response;
import org.corfudb.common.protocol.proto.CorfuProtocol.ServerError;
import org.corfudb.runtime.view.Layout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface IRequestRouter {

    Logger log = LoggerFactory.getLogger(IRequestRouter.class);

    void sendResponse(Response response, ChannelHandlerContext ctx);

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
     * Get a list of registered servers.
     */
    List<AbstractServer> getServers();

    /**
     * Send a WRONG_EPOCH error response.
     * @param requestHeader The header of the incoming request.
     * @param ctx The context of the channel handler.
     */
    default void sendWrongEpochError(Header requestHeader, ChannelHandlerContext ctx) {
        Header responseHeader = API.generateResponseHeader(requestHeader, false, true);
        long serverEpoch = getServerEpoch();

        ServerError wrongEpochError = API.getWrongEpochServerError("WRONG_EPOCH error "
                + "triggered by " + requestHeader.toString(), serverEpoch);

        Response response = API.getErrorResponseNoPayload(responseHeader, wrongEpochError);
        sendResponse(response, ctx);

        log.trace("sendWrongEpochError[{}]: Incoming request received with wrong epoch, got {}, expected {}, " +
                "request was {}", requestHeader.getRequestId(), requestHeader.getEpoch(), serverEpoch, requestHeader);
    }

    /**
     * Send a NOT_BOOTSTRAPPED error response.
     * @param requestHeader The header of the incoming request.
     * @param ctx The context of the channel handler.
     */
    default void sendNoBootstrapError(Header requestHeader, ChannelHandlerContext ctx) {
        Header responseHeader = API.generateResponseHeader(requestHeader, false, true);
        ServerError notBootstrappedError = API.getNotBootstrappedServerError("NOT_BOOTSTRAPPED "
                + " error triggered by " + requestHeader.toString());

        Response response = API.getErrorResponseNoPayload(responseHeader, notBootstrappedError);
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
    default void sendWrongClusterError(Header requestHeader, ChannelHandlerContext ctx, UUID clusterId) {
        Header responseHeader = API.generateResponseHeader(requestHeader, false, true);
        ServerError wrongClusterError = API.getWrongClusterServerError("WRONG_CLUSTER error triggered by "
                + requestHeader.toString(), clusterId, requestHeader.getClusterId());

        Response response = API.getErrorResponseNoPayload(responseHeader, wrongClusterError);
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
    default boolean epochIsValid(Header requestHeader, ChannelHandlerContext ctx) {
        long serverEpoch = getServerEpoch();
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
    default boolean clusterIdIsValid(Header requestHeader, ChannelHandlerContext ctx, Layout layout) {
        UUID currentClusterID = API.getUUID(layout.getClusterId());
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
    default boolean requestIsValid(Request req, ChannelHandlerContext ctx) {
        Header requestHeader = req.getHeader();

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
}
