package org.corfudb.infrastructure.logreplication.runtime;

import com.google.common.collect.ImmutableList;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSession;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.transport.server.IServerChannelAdapter;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.view.Layout;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;


/**
 * This class is shared by all the connection-endpoint routers.
 * Since there can be only 1 server per endpoint, this class creates the server, sets the server in the routers which is per session,
 * indirectly starts the server and listens to incoming msgs.
 */
@Slf4j
public class LogReplicationServerRouter implements IServerRouter {

    @Getter
    private final IServerChannelAdapter serverAdapter;

    /**
     * This map stores the mapping from message type to netty server handler.
     */
    private final Map<CorfuMessage.RequestPayloadMsg.PayloadCase, AbstractServer> handlerMap;

    /**
     * The epoch of this router. This is managed by the base server implementation.
     */
    @Getter
    @Setter
    private volatile long serverEpoch;

    /** The {@link AbstractServer}s this {@link LogReplicationSinkServerRouter} routes messages for. */
    final List<AbstractServer> servers;

    /**
     * Construct a new {@link LogReplicationServerRouter}
     *
     * @param serverMap A list of {@link AbstractServer}s this router will route messages for.
     * @param serverContext
     * @param sessionToSourceServer A map of session and the corresponding source-server router
     * @param sessionToSinkServer A map of session and the corresponding sink-server router
     */
    public LogReplicationServerRouter(Map<Class, AbstractServer> serverMap, ServerContext serverContext,
                                      Map<ReplicationSession, LogReplicationSourceServerRouter> sessionToSourceServer,
                                      Map<ReplicationSession, LogReplicationSinkServerRouter> sessionToSinkServer) {
        this.servers = ImmutableList.copyOf(serverMap.values());
        this.handlerMap = new EnumMap<>(CorfuMessage.RequestPayloadMsg.PayloadCase.class);

        serverMap.values().forEach(server -> {
            try {
                server.getHandlerMethods().getHandledTypes().forEach(x -> handlerMap.put(x, server));
            } catch (UnsupportedOperationException ex) {
                log.trace("No registered CorfuMsg handler for server {}", server, ex);
            }
        });

        serverAdapter = getAdapter(serverContext, sessionToSourceServer, sessionToSinkServer);

        // set the server adapter in every server router.
        updateAndSetServerAdapterForRouters(sessionToSourceServer, sessionToSinkServer);
    }

    public void updateAndSetServerAdapterForRouters(Map<ReplicationSession, LogReplicationSourceServerRouter> sessionToSourceServer,
                                                    Map<ReplicationSession, LogReplicationSinkServerRouter> sessionToSinkServer) {
        serverAdapter.updateRouters(sessionToSourceServer, sessionToSinkServer);
        // set the server adapter in every server router.
        if(!sessionToSourceServer.isEmpty()) {
            sessionToSourceServer.values().stream().filter(router -> router.serverChannelAdapter == null).forEach(router ->
                router.setAdapter(serverAdapter));
        }
        if (!sessionToSinkServer.isEmpty()) {
            sessionToSinkServer.values().stream().filter(router -> router.getServerAdapter() == null).forEach(router ->
                router.setAdapter(serverAdapter));
        }
    }

    private IServerChannelAdapter getAdapter(ServerContext serverContext,
                                             Map<ReplicationSession, LogReplicationSourceServerRouter> sessionToSourceServer,
                                             Map<ReplicationSession, LogReplicationSinkServerRouter> sessionToSinkServer) {

        LogReplicationPluginConfig config = new LogReplicationPluginConfig(serverContext.getPluginConfigFilePath());
        File jar = new File(config.getTransportAdapterJARPath());

        try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
            Class adapter = Class.forName(config.getTransportServerClassCanonicalName(), true, child);
            return (IServerChannelAdapter) adapter.getDeclaredConstructor(ServerContext.class, Map.class, Map.class)
                    .newInstance(serverContext, sessionToSourceServer, sessionToSinkServer);
        } catch (Exception e) {
            log.error("Fatal error: Failed to create serverAdapter", e);
            throw new UnrecoverableCorfuError(e);
        }
    }

    // ============ IServerRouter Methods =============

    @Override
    public void sendResponse(CorfuMessage.ResponseMsg response, ChannelHandlerContext ctx) {
        log.trace("Ready to send response {}", response.getPayload().getPayloadCase());
        try {
            serverAdapter.send(response);
            log.trace("Sent response: {}", response);
        } catch (IllegalArgumentException e) {
            log.warn("Illegal response type. Ignoring message.", e);
        }
    }

    @Override
    public Optional<Layout> getCurrentLayout() {
        return Optional.empty();
    }

    @Override
    public List<AbstractServer> getServers() {
        return servers;
    }

    @Override
    public void setServerContext(ServerContext serverContext) {

    }

    /**
     * {@inheritDoc}
     *
     * @deprecated This operation is no longer supported. The router will only route messages for
     * servers provided at construction time.
     */
    @Override
    @Deprecated
    public void addServer(AbstractServer server) {
        throw new UnsupportedOperationException("No longer supported");
    }

    // ================================================
}
