package org.corfudb.infrastructure.logreplication.runtime;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationManager;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSession;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent.LogReplicationRuntimeEventType;
import org.corfudb.infrastructure.logreplication.transport.client.ChannelAdapterException;
import org.corfudb.infrastructure.logreplication.transport.client.IClientChannelAdapter;
import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.runtime.clients.IClient;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.proto.RpcCommon;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase;
import org.corfudb.util.CFUtils;

import javax.annotation.Nonnull;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getDefaultProtocolVersionMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getRequestMsg;

/**
 * This Client Router is used when a custom (client-defined) transport layer is specified for
 * Log Replication Server communication.
 */
@Slf4j
public class LogReplicationSourceClientRouter extends LogReplicationSourceRouterHelper implements IClientRouter, LogReplicationClientRouter {


    /**
     * Log Replication Client Constructor
     *
     * @param remoteCluster the remote source cluster
     * @param localClusterId local cluster ID
     * @param parameters runtime parameters (including connection settings)
     * @param session replication session between current and remote cluster
     */
    public LogReplicationSourceClientRouter(ClusterDescriptor remoteCluster, String localClusterId,
                                            LogReplicationRuntimeParameters parameters, CorfuReplicationManager replicationManager,
                                            ReplicationSession session) {
        super(remoteCluster, localClusterId, parameters, replicationManager, session, true);
    }


    /**
     * Receive Corfu Message from the Channel Adapter for further processing
     *
     * @param msg received corfu message
     */
    public void receive(CorfuMessage.ResponseMsg msg) {
        try {
            // If it is a Leadership Loss Message re-trigger leadership discovery
            if (msg.getPayload().getPayloadCase() == PayloadCase.LR_LEADERSHIP_LOSS) {
                String nodeId = msg.getPayload().getLrLeadershipLoss().getNodeId();
                this.runtimeFSM.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEventType.REMOTE_LEADER_LOSS, nodeId));
                return;
            }

            // We get the handler for this message from the map
            IClient handler = handlerMap.get(msg.getPayload().getPayloadCase());

            if (handler == null) {
                // The message was unregistered, we are dropping it.
                log.warn("Received unregistered message {}, dropping", msg);
            } else {
                // Route the message to the handler.
                if (log.isTraceEnabled()) {
                    log.trace("Message routed to {}: {}",
                            handler.getClass().getSimpleName(), msg);
                }
                handler.handleMessage(msg, null);
            }
        } catch (Exception e) {
            log.error("Exception caught while receiving message of type {}",
                    msg.getPayload().getPayloadCase(), e);
        }
    }

    public void receive(CorfuMessage.RequestMsg msg) {
        log.info("recevied request msg {}", msg.getPayload());
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
     * Verify Message is of valid Log Replication type.
     */
    private boolean isValidMessage(RequestPayloadMsg message) {
        return message.getPayloadCase().equals(RequestPayloadMsg.PayloadCase.LR_ENTRY) ||
                message.getPayloadCase().equals(RequestPayloadMsg.PayloadCase.LR_METADATA_REQUEST) ||
                message.getPayloadCase().equals(RequestPayloadMsg.PayloadCase.LR_LEADERSHIP_QUERY);
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
            this.clientChannelAdapter.connectAsync();
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
        this.startReplication(nodeId);
    }

    /**
     * Connection Down Callback.
     *
     * @param nodeId id of the remote node to which connection came down.
     */
    @Override
    public synchronized void onConnectionDown(String nodeId) {
        log.info("Connection lost to remote node {} on cluster {}", nodeId, this.session.getRemoteClusterId());
        runtimeFSM.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEventType.ON_CONNECTION_DOWN,
                nodeId));
        // Attempt to reconnect to this endpoint
        this.clientChannelAdapter.connectAsync(nodeId);
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
