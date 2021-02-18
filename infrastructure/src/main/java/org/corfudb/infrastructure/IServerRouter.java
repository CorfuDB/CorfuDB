package org.corfudb.infrastructure;

import com.google.protobuf.TextFormat;
import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import java.util.Optional;

import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
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
     * Send a WRONG_EPOCH error response.
     *
     * @param requestHeader The header of the incoming request.
     * @param ctx The context of the channel handler.
     */
    default void sendWrongEpochError(HeaderMsg requestHeader, ChannelHandlerContext ctx) {
        final long correctEpoch = getServerEpoch();

        HeaderMsg responseHeader = getHeaderMsg(requestHeader, ClusterIdCheck.CHECK, EpochCheck.IGNORE);
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
        HeaderMsg responseHeader = getHeaderMsg(requestHeader, ClusterIdCheck.CHECK, EpochCheck.IGNORE);
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
        HeaderMsg responseHeader = getHeaderMsg(requestHeader, ClusterIdCheck.CHECK, EpochCheck.IGNORE);
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
