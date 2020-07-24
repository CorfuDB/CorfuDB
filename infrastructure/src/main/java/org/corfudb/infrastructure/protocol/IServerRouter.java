package org.corfudb.infrastructure.protocol;

import io.netty.channel.ChannelHandlerContext;
import org.corfudb.common.protocol.API;
import org.corfudb.common.protocol.proto.CorfuProtocol;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.runtime.view.Layout;
import org.corfudb.common.protocol.proto.CorfuProtocol.Header;
import org.corfudb.common.protocol.proto.CorfuProtocol.Request;
import org.corfudb.common.protocol.proto.CorfuProtocol.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface IServerRouter {

    Logger log = LoggerFactory.getLogger(IServerRouter.class);

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

    default boolean epochIsValid(Request req, ChannelHandlerContext ctx) {
        long serverEpoch = getServerEpoch();
        if(req.getHeader().getEpoch() != serverEpoch) {
            //TODO(Zach): send WrongEpoch error
            return false;
        }

        return true;
    }

    default boolean clusterIdIsValid(Request req, ChannelHandlerContext ctx, Layout layout) {
        UUID currentClusterID = layout.getClusterId();
        boolean match = req.getHeader().getClusterId().equals(API.getUUID(currentClusterID));

        if(!match) {
            //TODO(Zach): send WrongClusterId error
        }

        return match;
    }

    default boolean requestIsValid(Request req, ChannelHandlerContext ctx) {
        if(!req.getHeader().getIgnoreEpoch() && epochIsValid(req, ctx)) return false;
        if(!req.getHeader().getIgnoreClusterId()) {
            return getCurrentLayout()
                    .map(layout -> clusterIdIsValid(req, ctx, layout))
                    .orElseGet(() -> {
                        //TODO(Zach): send NotBootstrapped error
                        return false;
                    });
        }
        return true;
    }

    //TODO(Zach): default implementations of sendWrongEpochMessage, sendNoBootstrapMessage, sendWrongClusterIdMessage.
}
