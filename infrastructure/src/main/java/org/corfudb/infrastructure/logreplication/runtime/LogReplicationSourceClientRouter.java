package org.corfudb.infrastructure.logreplication.runtime;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationManager;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent.LogReplicationRuntimeEventType;
import org.corfudb.infrastructure.logreplication.transport.client.IClientChannelAdapter;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.proto.service.CorfuMessage;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Optional;

/**
 * Interacts with the custom transport layer and the Log replication SOURCE components.
 *
 * Being the connection initiator, this class implements the connection related handles.
 * The messages are forwarded to/from Log replication Source components via LogReplicationSourceBaseRouter.
 */
@Slf4j
public class LogReplicationSourceClientRouter extends LogReplicationSourceBaseRouter implements LogReplicationClientRouter {

    private final String pluginFilePath;


    /**
     * Log Replication Client Constructor
     *
     * @param remoteCluster the remote source cluster
     * @param parameters runtime parameters (including connection settings)
     * @param replicationManager replicationManager to start FSM
     * @param session replication session between current and remote cluster
     */
    public LogReplicationSourceClientRouter(ClusterDescriptor remoteCluster,
                                            LogReplicationRuntimeParameters parameters, CorfuReplicationManager replicationManager,
                                            LogReplicationSession session) {
        super(remoteCluster, parameters, replicationManager, session, true);
        pluginFilePath = parameters.getPluginFilePath();
    }


    /**
     * Receive Corfu Message from the Channel Adapter for further processing
     *
     * @param msg received corfu message
     */
    public void receive(CorfuMessage.ResponseMsg msg) {
        super.receive(msg);
    }

    public void receive(CorfuMessage.RequestMsg msg) {
        // no-op when Source is the connection starter
        throw new UnsupportedOperationException();
    }

    /**
     * When an error occurs in the Channel Adapter, trigger
     * exceptional completeness of all pending requests.
     *
     * @param e exception
     */
    public void completeAllExceptionally(Exception e) {
        // Exceptionally complete all requests that were waiting for a completion.
        outstandingRequests.forEach((reqId, reqCompletableFuture) -> {
            reqCompletableFuture.completeExceptionally(e);
            // And also remove them.
            outstandingRequests.remove(reqId);
        });
    }


    /**
     * Connect to remote cluster through the specified channel adapter
     */
    public void connect() {
        LogReplicationPluginConfig config = new LogReplicationPluginConfig(this.pluginFilePath);
        File jar = new File(config.getTransportAdapterJARPath());

        try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
            // Instantiate Channel Adapter (external implementation of the channel / transport)
            Class adapterType = Class.forName(config.getTransportClientClassCanonicalName(), true, child);
            this.clientChannelAdapter = (IClientChannelAdapter) adapterType.getDeclaredConstructor(String.class, ClusterDescriptor.class, LogReplicationSourceClientRouter.class, LogReplicationSinkClientRouter.class)
                    .newInstance(this.localClusterId, remoteClusterDescriptor, this, null);
            log.info("Connect asynchronously to remote cluster {} and session {} ", remoteClusterDescriptor.getClusterId(), this.session);
            // When connection is established to the remote leader node, the remoteLeaderConnectionFuture will be completed.
            this.clientChannelAdapter.connectAsync(session);
        } catch (Exception e) {
            log.error("Fatal error: Failed to initialize transport adapter {}", config.getTransportClientClassCanonicalName(), e);
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
        this.startReplication(nodeId);
    }

    /**
     * Connection Down Callback.
     *
     * @param nodeId id of the remote node to which connection came down.
     */
    @Override
    public synchronized void onConnectionDown(String nodeId) {
        log.info("Connection lost to remote node {} on cluster {}", nodeId, this.session.getSinkClusterId());
        runtimeFSM.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEventType.ON_CONNECTION_DOWN,
                nodeId));
        // Attempt to reconnect to this endpoint
        this.clientChannelAdapter.connectAsync(nodeId, session);
    }

    /**
     * Channel Adapter On Error Callback
     */
    public synchronized void onError(Throwable t) {
        runtimeFSM.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEventType.ERROR, t));
    }

    /**
     * Cluster Change Callback.
     *
     * @param clusterDescriptor remote cluster descriptor
     */
    public synchronized void onClusterChange(ClusterDescriptor clusterDescriptor) {
        this.clientChannelAdapter.clusterChangeNotification(clusterDescriptor);
    }

    public Optional<String> getRemoteLeaderNodeId() {
        return runtimeFSM.getRemoteLeaderNodeId();
    }

    public void resetRemoteLeader() {
        if (this.clientChannelAdapter != null) {
            log.debug("Reset remote leader from channel adapter.");
            this.clientChannelAdapter.resetRemoteLeader();
        }
    }

}
