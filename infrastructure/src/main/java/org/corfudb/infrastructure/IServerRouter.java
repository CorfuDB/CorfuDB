package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.WrongClusterMsg;
import org.corfudb.runtime.view.Layout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Created by mwei on 12/13/15.
 */
public interface IServerRouter {

    // Lombok annotations are not allowed for the interfaces.
    Logger log = LoggerFactory.getLogger(IServerRouter.class);

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
}
