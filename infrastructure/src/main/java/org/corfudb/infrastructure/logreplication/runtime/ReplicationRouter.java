package org.corfudb.infrastructure.logreplication.runtime;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationManager;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSession;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent;
import org.corfudb.infrastructure.logreplication.transport.client.IClientChannelAdapter;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.proto.service.CorfuMessage;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;

@Slf4j
public abstract class ReplicationRouter {

    /**
     * Adapter to the channel implementation
     */
    IClientChannelAdapter channelAdapter;

    /**
     * Remote Cluster/Site Full Descriptor
     */
    final ClusterDescriptor remoteClusterDescriptor;

    final String pluginFilePath;


    String localClusterId;

    ReplicationSession session;

    boolean isSource;


    public ReplicationRouter(ClusterDescriptor remoteCluster, String localClusterId, String pluginFilePath, ReplicationSession session, boolean isSource) {
        this.remoteClusterDescriptor = remoteCluster;
        this.pluginFilePath = pluginFilePath;
        this.localClusterId = localClusterId;
        // this will be required when the sink is the connection initiator
        this.session = session;
        this.isSource = isSource;
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
            channelAdapter = (IClientChannelAdapter) adapterType.getDeclaredConstructor(String.class, ClusterDescriptor.class, ReplicationSourceRouter.class, ReplicationSinkClientRouter.class)
                    .newInstance(this.localClusterId, remoteClusterDescriptor, isSource ? this : null, isSource ? null : this);
            log.info("Connect asynchronously to remote cluster {} and session {} ", remoteClusterDescriptor.getClusterId(), this.session);
            // When connection is established to the remote leader node, the remoteLeaderConnectionFuture will be completed.
            channelAdapter.connectAsync();
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
    public abstract void onConnectionUp(String nodeId);

    /**
     * Connection Down Callback.
     *
     * @param nodeId id of the remote node to which connection came down.
     */
    public abstract void onConnectionDown(String nodeId);

    public  abstract void  onError(Throwable t);

    public abstract void receive(CorfuMessage.ResponseMsg msg);

    public abstract void receive(CorfuMessage.RequestMsg msg);

    public abstract void completeAllExceptionally(Exception e);

}
