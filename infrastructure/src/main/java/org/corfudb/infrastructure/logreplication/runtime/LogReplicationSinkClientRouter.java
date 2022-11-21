package org.corfudb.infrastructure.logreplication.runtime;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.BaseServer;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationManager;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationServer;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSession;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent;
import org.corfudb.infrastructure.logreplication.runtime.sinkFsm.SinkVerifyRemoteLeader;
import org.corfudb.infrastructure.logreplication.transport.client.ChannelAdapterException;
import org.corfudb.infrastructure.logreplication.transport.client.IClientChannelAdapter;
import org.corfudb.infrastructure.logreplication.transport.server.IServerChannelAdapter;
import org.corfudb.protocols.service.CorfuProtocolMessage;
import org.corfudb.runtime.clients.IClient;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.proto.RpcCommon;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;

import javax.annotation.Nonnull;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
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

//==================================================================
/**
 *   NOT READY FOR REVIEW. PLACEHOLDER
 */
//==================================================================
@Slf4j
public class LogReplicationSinkClientRouter extends LogReplicationSinkServerRouter implements IClientRouter, LogReplicationClientRouter {

    public static String REMOTE_LEADER = "REMOTE_LEADER";

    /**
     * The clients registered to this router.
     */
    public final List<IClient> clientList;

    /**
     * Whether or not this router is shutdown.
     */
    public volatile boolean shutdown;

    /**
     * A {@link CompletableFuture} which is completed when a connection,
     * including a successful handshake completes and messages can be sent
     * to the remote node.
     */
    @Getter
    private volatile CompletableFuture<Void> remoteLeaderConnectionFuture;

    /**
     * The current request ID.
     */
    @Getter
    @SuppressWarnings("checkstyle:abbreviation")
    public AtomicLong requestID;

    /**
     * Sync call response timeout (milliseconds).
     */
    @Getter
    @Setter
    public long timeoutResponse;

    /**
     * The outstanding requests on this router.
     */
    public final Map<Long, CompletableFuture> outstandingRequests;

    /**
     * Remote cluster's clusterDescriptor
     */
    private final ClusterDescriptor remoteClusterDescriptor;

    private String localClusterId;

//    LogReplicationSinkServerRouter sinkRouter;

    /**
     * Adapter to the channel implementation
     */
    @Getter
    IClientChannelAdapter channelAdapter;

    final String pluginFilePath;

    ReplicationSession session;

    private final SinkVerifyRemoteLeader verifyLeadership;



    /**
     * Log Replication Client Constructor
     *
     * @param remoteCluster the remote source cluster
     * @param localClusterId local cluster ID
     * @param session replication session between current and remote cluster
     */

    public LogReplicationSinkClientRouter(ClusterDescriptor remoteCluster, String localClusterId,
                                            String pluginFilePath, long timeoutResponse, ReplicationSession session,
                                          Map<Class, AbstractServer> serverMap) {
        super(serverMap, true);
        this.timeoutResponse = timeoutResponse;
        this.remoteClusterDescriptor = remoteCluster;
        this.localClusterId = localClusterId;

        this.clientList = new ArrayList<>();
        this.requestID = new AtomicLong();
        this.outstandingRequests = new ConcurrentHashMap<>();
        this.remoteLeaderConnectionFuture = new CompletableFuture<>();

        this.pluginFilePath = pluginFilePath;
        this.localClusterId = localClusterId;
        // this will be required when the sink is the connection initiator
        this.session = session;

        this.verifyLeadership = new SinkVerifyRemoteLeader(session, this);
    }

    // ------------------- IClientRouter Interface ----------------------

    @Override
    public IClientRouter addClient(IClient client) {
        return null;
    }


    /**
     * Send a request message and get a completable future to be fulfilled by the reply.
     *
     * @param payload
     * @param <T> The type of completable to return.
     * @return A completable future which will be fulfilled by the reply,
     * or a timeout in the case there is no response.
     */
    @Override
    public <T> CompletableFuture<T> sendRequestAndGetCompletable(
            @Nonnull RequestPayloadMsg payload,
            @Nonnull String nodeId) {

        CorfuMessage.HeaderMsg.Builder header = CorfuMessage.HeaderMsg.newBuilder()
                .setVersion(getDefaultProtocolVersionMsg())
                .setIgnoreClusterId(true)
                .setIgnoreEpoch(true);

        // Get the next request ID.
        final long requestId = requestID.getAndIncrement();

        // Generate a future and put it in the completion table.
        final CompletableFuture<T> cf = new CompletableFuture<>();
        outstandingRequests.put(requestId, cf);
        try {
//            LogReplicationRuntimeParameters parameters =  this.runtimeFSM.getSourceManager().getParameters();
//            header.setClientId(getUuidMsg(parameters.getClientId()));
            header.setRequestId(requestId);
            header.setClusterId(getUuidMsg(UUID.fromString(this.localClusterId)));

            log.info("Send message to {}, type={}", nodeId, payload.getPayloadCase());
            getChannelAdapter().send(nodeId, getRequestMsg(header.build(), payload));
        } catch (NetworkException ne) {
            log.error("Caught Network Exception while trying to send message to remote node {}", nodeId);
            verifyLeadership.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.ON_CONNECTION_DOWN,
                    nodeId));
            throw ne;
        } catch (Exception e) {
            log.info("sendRequestAndGetCompletable removing reqId outstandingRequests {}", requestId);
            outstandingRequests.remove(requestId);
            log.error("sendMessageAndGetCompletable: Remove request {} to {} due to exception! Message:{}",
                    requestId, remoteClusterDescriptor.getClusterId(), payload.getPayloadCase(), e);
            cf.completeExceptionally(e);
            return cf;
        }

        // Generate a timeout future, which will complete exceptionally if the main future is not completed.
        final CompletableFuture<T> cfTimeout =
                CFUtils.within(cf, Duration.ofMillis(timeoutResponse));
        cfTimeout.exceptionally(e -> {
            if (e.getCause() instanceof TimeoutException) {
                outstandingRequests.remove(requestId);
                log.debug("sendMessageAndGetCompletable: Remove request {} to {} due to timeout! Message:{}",
                        requestId, remoteClusterDescriptor.getClusterId(), payload.getPayloadCase());
            }
            return null;
        });

        return cfTimeout;
    }

    /**
     * Send a request message and get a completable future to be fulfilled by the reply.
     *
     * @param payload
     * @param epoch
     * @param clusterId
     * @param priority
     * @param ignoreClusterId
     * @param ignoreEpoch
     * @param <T> The type of completable to return.
     * @return A completable future which will be fulfilled by the reply,
     * or a timeout in the case there is no response.
     */
    @Override
    public <T> CompletableFuture<T> sendRequestAndGetCompletable(RequestPayloadMsg payload, long epoch, RpcCommon.UuidMsg clusterId,
                                                                 org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel priority,
                                                                 CorfuProtocolMessage.ClusterIdCheck ignoreClusterId, CorfuProtocolMessage.EpochCheck ignoreEpoch) {
        // This is an empty stub. This method is not being used anywhere in the LR framework.
        return null;
    }

    /**
     * Send a one way message, without adding a completable future.
     *
     * @param payload
     * @param epoch
     * @param clusterId
     * @param priority
     * @param ignoreClusterId
     * @param ignoreEpoch
     */
    @Override
    public void sendRequest(RequestPayloadMsg payload, long epoch, RpcCommon.UuidMsg clusterId,
                            org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel priority,
                            CorfuProtocolMessage.ClusterIdCheck ignoreClusterId, CorfuProtocolMessage.EpochCheck ignoreEpoch) {
        // This is an empty stub. This method is not being used anywhere in the LR framework.
    }

    @Override
    public <T> void completeRequest(long requestID, T completion) {
        // only for leadership request
        log.trace("Complete request: {}...outstandingRequests {}", requestID, outstandingRequests);
        CompletableFuture<T> cf;
        if ((cf = (CompletableFuture<T>) outstandingRequests.remove(requestID)) != null) {
            cf.complete(completion);
        } else {
            log.warn("Attempted to complete request {}, but request not outstanding!", requestID);
        }
    }

    @Override
    public void completeExceptionally(long requestID, Throwable cause) {
        // only for leadership request
        CompletableFuture cf;
        if ((cf = outstandingRequests.remove(requestID)) != null) {
            cf.completeExceptionally(cause);
            log.debug("completeExceptionally: Remove request {} to {} due to {}.", requestID,
                    remoteClusterDescriptor.getClusterId(), cause.getClass().getSimpleName(), cause);
        } else {
            log.warn("Attempted to exceptionally complete request {}, but request not outstanding!",
                    requestID);
        }
    }

    @Override
    public void stop() {
        log.debug("stop: Shutting down router for {}", session);
        shutdown = true;
        channelAdapter.stop();
        remoteLeaderConnectionFuture = new CompletableFuture<>();
        remoteLeaderConnectionFuture.completeExceptionally(new NetworkException("Router stopped", remoteClusterDescriptor.getClusterId()));
    }

    @Override
    public Integer getPort() {
        // For logging purposes return one port (as this abstraction does not make sense for a Log Replication
        // Client Router) as it is a router to an entire cluster/site.
        return Integer.valueOf(remoteClusterDescriptor.getNodesDescriptors().iterator().next().getPort());
    }

    @Override
    public String getHost() {
        String host = "";
        // For logging purposes return all remote cluster nodes host in a concatenated form
        remoteClusterDescriptor.getNodesDescriptors().forEach(node -> host.concat(node.getHost() + ":"));
        return host;
    }

    @Override
    public void setTimeoutConnect(long timeoutConnect) {
    }

    @Override
    public void setTimeoutRetry(long timeoutRetry) {
    }

    @Override
    public void setTimeoutResponse(long timeoutResponse) {
        this.timeoutResponse = timeoutResponse;
    }


    // ---------------------------------------------------------------------------

    /**
     * Receive Corfu Message from the Channel Adapter for further processing
     *
     * @param msg received corfu message
     */
    public void receive(CorfuMessage.ResponseMsg msg) {
//        try {
//            // If it is a Leadership Loss Message re-trigger leadership discovery
//            if (msg.getPayload().getPayloadCase() == CorfuMessage.ResponsePayloadMsg.PayloadCase.LR_LEADERSHIP_LOSS) {
//                String nodeId = msg.getPayload().getLrLeadershipLoss().getNodeId();
//                runtimeFSM.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.REMOTE_LEADER_LOSS, nodeId));
//                return;
//            }
//
//            // We get the handler for this message from the map
//            IClient handler = handlerMap.get(msg.getPayload().getPayloadCase());
//
//            if (handler == null) {
//                // The message was unregistered, we are dropping it.
//                log.warn("Received unregistered message {}, dropping", msg);
//            } else {
//                // Route the message to the handler.
//                if (log.isTraceEnabled()) {
//                    log.trace("Message routed to {}: {}",
//                            handler.getClass().getSimpleName(), msg);
//                }
//                handler.handleMessage(msg, null);
//            }
//        } catch (Exception e) {
//            log.error("Exception caught while receiving message of type {}",
//                    msg.getPayload().getPayloadCase(), e);
//        }
    }

    public void receive(CorfuMessage.RequestMsg message) {
        super.receive(message);
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
            channelAdapter = (IClientChannelAdapter) adapterType.getDeclaredConstructor(String.class, ClusterDescriptor.class, LogReplicationSourceClientRouter.class, LogReplicationSinkClientRouter.class)
                    .newInstance(this.localClusterId, remoteClusterDescriptor, null, this);
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
    @Override
    public synchronized void onConnectionUp(String nodeId) {
        log.info("Connection established to remote node {}.", nodeId);
        this.verifyLeadership.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.ON_CONNECTION_UP, nodeId));
    }

    /**
     * Connection Down Callback.
     *
     * @param nodeId id of the remote node to which connection came down.
     */
    @Override
    public synchronized void onConnectionDown(String nodeId) {
        log.info("Connection lost to remote node {} on cluster {}", nodeId, this.session.getRemoteClusterId());
        // Attempt to reconnect to this endpoint
        channelAdapter.connectAsync(nodeId);
    }

    /**
     * Channel Adapter On Error Callback
     */
    public synchronized void onError(Throwable t) {
        // no op
        log.error("Unrecoverable state due to error {}", t.getMessage());
    }

    /**
     * Cluster Change Callback.
     *
     * @param clusterDescriptor remote cluster descriptor
     */

    // Shama: Now sink needs to find the leader.
    public synchronized void onClusterChange(ClusterDescriptor clusterDescriptor) {
        channelAdapter.clusterChangeNotification(clusterDescriptor);
    }

    public Optional<String> getRemoteLeaderNodeId() {
//        return runtimeFSM.getRemoteLeaderNodeId();
        return null;
    }

    public void resetRemoteLeader() {
        if (channelAdapter != null) {
            log.debug("Reset remote leader from channel adapter.");
            channelAdapter.resetRemoteLeader();
        }
    }
}
