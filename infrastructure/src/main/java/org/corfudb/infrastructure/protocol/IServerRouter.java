package org.corfudb.infrastructure.protocol;

import org.corfudb.infrastructure.ServerContext;
import org.corfudb.runtime.view.Layout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

public interface IServerRouter {

    Logger log = LoggerFactory.getLogger(IServerRouter.class);

    //TODO(Zach): void sendResponse(ChannelHandlerContext ctx, ...)

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

    //TODO(Zach): default implementations of sendWrongEpochMessage, sendNoBootstrapMessage,
    //TODO: sendWrongClusterIdMessage, epochIsValid, clusterIdIsValid, messageIsValid.
}
