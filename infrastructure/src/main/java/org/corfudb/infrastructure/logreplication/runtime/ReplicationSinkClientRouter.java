package org.corfudb.infrastructure.logreplication.runtime;

import com.google.common.collect.ImmutableList;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.BaseServer;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSession;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.transport.server.IServerChannelAdapter;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.view.Layout;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

//==================================================================
/**
 *   NOT READY FOR REVIEW. PLACEHOLDER
 */
//==================================================================
@Slf4j
public class ReplicationSinkClientRouter extends ReplicationRouter implements IServerRouter {

    @Getter
    private final IServerChannelAdapter serverAdapter;

    /**
     * This map stores the mapping from message type to netty server handler.
     */
    private final Map<RequestPayloadMsg.PayloadCase, AbstractServer> handlerMap;

    /**
     * The epoch of this router. This is managed by the base server implementation.
     */
    @Getter
    @Setter
    private volatile long serverEpoch;

    /** The {@link AbstractServer}s this {@link ReplicationSinkRouter} routes messages for. */
    final List<AbstractServer> servers;

    /** Construct a new {@link ReplicationSinkRouter}.
     *
     * @param servers   A list of {@link AbstractServer}s this router will route
     *                  messages for.
     * @param remoteCluster the remote source cluster
     * @param localClusterId local cluster ID
     * @param  pluginFilePath file path to fetch plugin information
     * @param session replication session between current and remote cluster
     */
    public ReplicationSinkClientRouter(List<AbstractServer> servers, ClusterDescriptor remoteCluster,
                                 String localClusterId, String pluginFilePath, ReplicationSession session) {
        super(remoteCluster, localClusterId, pluginFilePath, session, false);
        this.serverEpoch = ((BaseServer) servers.get(0)).serverContext.getServerEpoch();
        this.servers = ImmutableList.copyOf(servers);
        this.handlerMap = new EnumMap<>(RequestPayloadMsg.PayloadCase.class);

        servers.forEach(server -> {
            try {
                server.getHandlerMethods().getHandledTypes().forEach(x -> handlerMap.put(x, server));
            } catch (UnsupportedOperationException ex) {
                log.trace("No registered CorfuMsg handler for server {}", server, ex);
            }
        });

        this.serverAdapter = getAdapter(((BaseServer) servers.get(0)).serverContext);
    }


    private IServerChannelAdapter getAdapter(ServerContext serverContext) {

        LogReplicationPluginConfig config = new LogReplicationPluginConfig(serverContext.getPluginConfigFilePath());
        File jar = new File(config.getTransportAdapterJARPath());

        try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
            Class adapter = Class.forName(config.getTransportServerClassCanonicalName(), true, child);
            return (IServerChannelAdapter) adapter.getDeclaredConstructor(
                    ServerContext.class, ReplicationSinkClientRouter.class).newInstance(serverContext, this);
        } catch (Exception e) {
            log.error("Fatal error: Failed to create serverAdapter", e);
            throw new UnrecoverableCorfuError(e);
        }
    }

    /**
     * Connection Up Callback.
     *
     * @param nodeId id of the remote node to which connection was established.
     */
    @Override
    public synchronized void onConnectionUp(String nodeId) {
        log.info("Connection established to remote node {}", nodeId);
        //no op
    }

    /**
     * Connection Down Callback.
     *
     * @param nodeId id of the remote node to which connection came down.
     */
    @Override
    public synchronized void onConnectionDown(String nodeId) {
        log.info("Connection lost to remote node {} on cluster {}", nodeId, this.remoteClusterDescriptor.getClusterId());
        // Attempt to reconnect to this endpoint
        channelAdapter.connectAsync(nodeId);
    }

    /**
     * Channel Adapter On Error Callback
     */
    public synchronized void onError(Throwable t) {
        log.info("Error on Connection {}", t.getMessage());
    }


    // ============ IServerRouter Methods =============

    @Override
    public void sendResponse(ResponseMsg response, ChannelHandlerContext ctx) {
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

    // ================================================

    /**
     * Receive messages from the 'custom' serverAdapter implementation. This message will be forwarded
     * for processing.
     *
     * @param message
     */
    public void receive(RequestMsg message) {
        log.trace("Received message {}", message.getPayload().getPayloadCase());

        AbstractServer handler = handlerMap.get(message.getPayload().getPayloadCase());
        if (handler == null) {
            // The message was unregistered, we are dropping it.
            log.warn("Received unregistered message {}, dropping", message);
        } else {
            if (validateEpoch(message.getHeader())) {
                // Route the message to the handler.
                if (log.isTraceEnabled()) {
                    log.trace("Message routed to {}: {}", handler.getClass().getSimpleName(), message);
                }

                try {
                    handler.handleMessage(message, null,  this);
                } catch (Throwable t) {
                    log.error("channelRead: Handling {} failed due to {}:{}",
                            message.getPayload().getPayloadCase(),
                            t.getClass().getSimpleName(),
                            t.getMessage(),
                            t);
                }
            }
        }
    }

    public void receive(ResponseMsg message) {
        log.info("Received ResponseMsg {} ", message);
    }

    public void completeAllExceptionally(Exception e) {
        log.info("Error in connecting....have the reconnection logic in sink");
    }

    /**
     * Validate the epoch of a CorfuMsg, and send a WRONG_EPOCH response if
     * the server is in the wrong epoch. Ignored if the message type is reset (which
     * is valid in any epoch).
     *
     * @param header The incoming header to validate.
     * @return True, if the epoch is correct, but false otherwise.
     */
    private boolean validateEpoch(HeaderMsg header) {
        long serverEpoch = getServerEpoch();
        if (!header.getIgnoreEpoch() && header.getEpoch()!= serverEpoch) {
            log.trace("Incoming message with wrong epoch, got {}, expected {}",
                    header.getEpoch(), serverEpoch);
            sendWrongEpochError(header, null);
            return false;
        }
        return true;
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
}
