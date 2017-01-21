package org.corfudb.infrastructure;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.ChannelHandlerContext;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.format.Types.NetworkMetrics;
import org.corfudb.format.Types.NodeView;
import org.corfudb.format.Types.ServerMetrics;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.FailureDetectorMsg;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.view.Layout;

import java.lang.invoke.MethodHandles;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.RejectedExecutionException;

/**
 * Instantiates and performs failure detection and handling asynchronously.
 * <p>
 * Failure Detector:
 * Executes detection policy (eg. PeriodicPollingPolicy).
 * It then checks for status of the nodes. If the result map is not empty,
 * there are failed nodes to be addressed. This then triggers the failure
 * handler which then responds to these failures based on a policy.
 * <p>
 * Created by zlokhandwala on 9/28/16.
 */
@Slf4j
public class ManagementServer extends AbstractServer {

    /**
     * The options map.
     */
    private final Map<String, Object> opts;
    private final ServerContext serverContext;

    private static final String PREFIX_LAYOUT = "M_LAYOUT";
    private static final String KEY_LAYOUT = "M_CURRENT";

    private static final String metricsPrefix = "corfu.server.management-server.";

    private CorfuRuntime corfuRuntime;

    //  Latest layout received from bootstrap or the runtime.
    private volatile Layout latestLayout;

    //  Bootstrap endpoint to seed the Management Server
    private String bootstrapEndpoint;

    //  Policy to be used to detect failures.
    private IFailureDetectorPolicy failureDetectorPolicy;
    //  Interval in executing the failure detection policy. (milliseconds)
    @Getter
    private final long policyExecuteInterval = 1000;
    //  Executor service to schedule failure detection.
    @Getter
    private final ScheduledExecutorService failureDetectorService;
    //  Failure Detector service future
    private Future failureDetectorFuture = null;

    //  Policy to be used to handle failures.
    private IFailureHandlerPolicy failureHandlerPolicy;
    //  A flag to determine whether the cluster is setup and
    //  the server is ready to start handling the detected failures.
    private boolean startFailureHandler = false;

    /** CLUSTER METRICS COLLECTION   */
    //  Service to poll local node metrics.
    private final ScheduledExecutorService localMetricsPollingService;
    //  Local copy of the local node's server metrics.
    private volatile ServerMetrics serverMetrics = ServerMetrics.getDefaultInstance();
    //  Locally collected nodeMetrics
    private final long localMetricsPollInterval = 1000;
    //  Local copy of the nodeView sent in response to the heartbeat.
    private volatile NodeView nodeView = NodeView.getDefaultInstance();
    //  Report of the latest heartbeat poll
    private volatile HeartbeatPollReport heartbeatPollReport;

    public ManagementServer(ServerContext serverContext) {

        this.opts = serverContext.getServerConfig();
        this.serverContext = serverContext;

        bootstrapEndpoint = (opts.get("--management-server")!=null) ? opts.get("--management-server").toString() : null;

        if((Boolean) opts.get("--single")) {
            String localAddress = opts.get("--address") + ":" + opts.get("<port>");

            Layout singleLayout = new Layout(
                    Collections.singletonList(localAddress),
                    Collections.singletonList(localAddress),
                    Collections.singletonList(new Layout.LayoutSegment(
                            Layout.ReplicationMode.CHAIN_REPLICATION,
                            0L,
                            -1L,
                            Collections.singletonList(
                                    new Layout.LayoutStripe(
                                            Collections.singletonList(localAddress)
                                    )
                            )
                    )),
                    0L
            );

            safeUpdateLayout(singleLayout);
        } else {
            safeUpdateLayout(getCurrentLayout());
        }

        this.failureDetectorPolicy = serverContext.getFailureDetectorPolicy();
        this.failureHandlerPolicy = serverContext.getFailureHandlerPolicy();
        this.failureDetectorService = Executors.newScheduledThreadPool(
                2,
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("FaultDetector-%d-" + getLocalEndpoint())
                        .build());

        // Initiating periodic task to poll for failures.
        try {
            failureDetectorService.scheduleAtFixedRate(
                    this::failureDetectorScheduler,
                    0,
                    policyExecuteInterval,
                    TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException err) {
            log.error("Error scheduling failure detection task, {}", err);
        }

        // Collecting local metrics - task scheduler
        this.localMetricsPollingService = Executors.newScheduledThreadPool(
                1,
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("LocalMetricsCollector-%d-" + getLocalEndpoint())
                        .build());

        // Initiating local metrics polling task.
        try {
            localMetricsPollingService.scheduleAtFixedRate(
                    this::updateLocalMetrics,
                    0,
                    localMetricsPollInterval,
                    TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException err) {
            log.error("Error scheduling local metrics polling task, {}", err);
        }
    }

    /**
     * Handler for the base server
     */
    @Getter
    private CorfuMsgHandler handler = new CorfuMsgHandler().generateHandlers(MethodHandles.lookup(), this);

    /**
     * Thread safe updating of layout only if new layout has higher epoch value.
     *
     * @param layout New Layout
     */
    private synchronized void safeUpdateLayout(Layout layout) {
        // Cannot update with a null layout.
        if (layout == null) return;

        // Update only if new layout has a higher epoch than the existing layout.
        if (latestLayout == null || layout.getEpoch() > latestLayout.getEpoch()) {
            latestLayout = layout;
            // Persisting this new updated layout
            setCurrentLayout(latestLayout);
        }
    }

    /**
     * Sets the latest layout in the persistent datastore.
     *
     * @param layout Layout to be persisted
     */
    private void setCurrentLayout(Layout layout) {
        serverContext.getDataStore().put(Layout.class, PREFIX_LAYOUT, KEY_LAYOUT, layout);
    }

    /**
     * Fetches the latest layout from the persistent datastore.
     *
     * @return The last persisted layout
     */
    private Layout getCurrentLayout() {
        return serverContext.getDataStore().get(Layout.class, PREFIX_LAYOUT, KEY_LAYOUT);
    }

    boolean checkBootstrap(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        if (latestLayout == null && bootstrapEndpoint == null) {
            log.warn("Received message but not bootstrapped! Message={}", msg);
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.MANAGEMENT_NOBOOTSTRAP_ERROR));
            return false;
        }
        return true;
    }

    /**
     * Bootstraps the management server.
     * The msg contains the layout to be bootstrapped.
     *
     * @param msg
     * @param ctx
     * @param r
     */
    @ServerHandler(type = CorfuMsgType.MANAGEMENT_BOOTSTRAP_REQUEST, opTimer = metricsPrefix + "bootstrap-request")
    public synchronized void handleManagementBootstrap(CorfuPayloadMsg<Layout> msg, ChannelHandlerContext ctx, IServerRouter r,
                                                       boolean isMetricsEnabled) {
        if (latestLayout != null) {
            // We are already bootstrapped, bootstrap again is not allowed.
            log.warn("Got a request to bootstrap a server which is already bootstrapped, rejecting!");
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.MANAGEMENT_ALREADY_BOOTSTRAP_ERROR));
        }
        else {
            log.info("Received Bootstrap Layout : {}", msg.getPayload());
            safeUpdateLayout(msg.getPayload());
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
        }
    }

    /**
     * Trigger to start the failure handler.
     *
     * @param msg
     * @param ctx
     * @param r
     */
    @ServerHandler(type = CorfuMsgType.MANAGEMENT_START_FAILURE_HANDLER, opTimer = metricsPrefix + "start-failure-handler")
    public synchronized void initiateFailureHandler(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r,
                                                    boolean isMetricsEnabled) {
        if (isShutdown()) {
            log.warn("Management Server received {} but is shutdown.", msg.getMsgType().toString());
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.NACK));
            return;
        }
        // This server has not been bootstrapped yet, ignore all requests.
        if (!checkBootstrap(msg, ctx, r)) { return; }

        if (!startFailureHandler) {
            startFailureHandler = true;
            log.info("Initiated Failure Handler.");
        } else {
            log.info("Failure Handler already initiated.");
        }
        r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
    }

    /**
     * Triggers the failure handler.
     * The msg contains the failed/defected nodes.
     *
     * @param msg
     * @param ctx
     * @param r
     */
    @ServerHandler(type = CorfuMsgType.MANAGEMENT_FAILURE_DETECTED, opTimer = metricsPrefix + "failure-detected")
    public synchronized void handleFailureDetectedMsg(CorfuPayloadMsg<FailureDetectorMsg> msg, ChannelHandlerContext ctx, IServerRouter r,
                                                      boolean isMetricsEnabled) {
        if (isShutdown()) {
            log.warn("Management Server received {} but is shutdown.", msg.getMsgType().toString());
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.NACK));
            return;
        }
        // This server has not been bootstrapped yet, ignore all requests.
        if (!checkBootstrap(msg, ctx, r)) { return; }

        log.info("Received Failures : {}", msg.getPayload().getNodes());
        FailureHandlerDispatcher failureHandlerDispatcher = new FailureHandlerDispatcher();
        try {
            failureHandlerDispatcher.dispatchHandler(failureHandlerPolicy, (Layout) latestLayout.clone(), getCorfuRuntime(), msg.getPayload().getNodes());
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
        } catch (CloneNotSupportedException e) {
            log.error("Failure Handler could not clone layout: {}", e);
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.NACK));
        }
    }

    /**
     * Handles the heartbeat request.
     * It accumulates the metrics required to build
     * and send the response.
     * The response comprises of the local nodeMetrics and
     * this node's view of the cluster (NodeView).
     *
     * @param msg
     * @param ctx
     * @param r
     */
    @ServerHandler(type = CorfuMsgType.HEARTBEAT_REQUEST, opTimer = metricsPrefix + "heartbeat-request")
    public void handleHearbeatRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r,
                                      boolean isMetricsEnabled) {
        // Responds with the in memory status of the local metrics.
        r.sendResponse(ctx, msg, new CorfuPayloadMsg<>(CorfuMsgType.HEARTBEAT_RESPONSE, nodeView.toByteArray()));
    }

    /**
     * Returns a connected instance of the CorfuRuntime.
     *
     * @return A connected instance of runtime.
     */
    public synchronized CorfuRuntime getCorfuRuntime() {

        if (corfuRuntime == null) {
            corfuRuntime = new CorfuRuntime();
            if ((Boolean) opts.get("--enable-tls")) {
                corfuRuntime.enableTls((String) opts.get("--keystore"),
                    (String) opts.get("--keystore-password-file"),
                    (String) opts.get("--truststore"),
                    (String) opts.get("--truststore-password-file"));
                if ((Boolean) opts.get("--enable-sasl-plain-text-auth")) {
                    corfuRuntime.enableSaslPlainText(
                        (String) opts.get("--sasl-plain-text-username-file"),
                        (String) opts.get("--sasl-plain-text-password-file"));
                }
            }
            // Runtime can be set up either using the layout or the bootstrapEndpoint address.
            if (latestLayout != null)
                latestLayout.getLayoutServers().forEach(ls -> corfuRuntime.addLayoutServer(ls));
            else
                corfuRuntime.addLayoutServer(bootstrapEndpoint);
            corfuRuntime.connect();
            log.info("Corfu Runtime connected successfully");
        }
        return corfuRuntime;
    }

    /**
     * Gets the address of this endpoint.
     *
     * @return localEndpoint address
     */
    private String getLocalEndpoint() {
        return this.opts.get("--address") + ":" + this.opts.get("<port>");
    }

    /**
     * Task to collect local node metrics.
     * This task collects the following:
     *  Boolean Status of layout, sequencer and logunit servers.
     *  Logunit Metrics:
     *      Size of writeQueue for pending write requests.
     * These metrics are then composed into NodeMetrics protobuf model
     * and stored locally.
     */
    private void updateLocalMetrics() {
        // Initializing the status of componenets
        // No need to poll unless node is bootstrapped.
        if (latestLayout == null) {
            return;
        }
        IClientRouter localRouter = getCorfuRuntime().getRouter(getLocalEndpoint());

        // Build and replace existing locally stored nodeMetrics
        serverMetrics = ServerMetrics.newBuilder()
                .setEndpoint(getLocalEndpoint())
                .build();

        NodeView.Builder nodeViewBuilder = NodeView.newBuilder()
                .setEndpoint(getLocalEndpoint())
                .setServerMetrics(serverMetrics);
        if (heartbeatPollReport != null) {
            Map<String, NetworkMetrics> networkMetricsMap = heartbeatPollReport.getNetworkMetricsMap();
            networkMetricsMap.forEach(nodeViewBuilder::putNetworkMetricsMap);
        }
        nodeView = nodeViewBuilder.build();
    }

    /**
     * Schedules the failure detector task only if the previous task is completed.
     */
    private void failureDetectorScheduler() {
        if (latestLayout == null && bootstrapEndpoint == null) {
            log.warn("Management Server waiting to be bootstrapped");
            return;
        }
        if (failureDetectorFuture == null || failureDetectorFuture.isDone()) {
            failureDetectorFuture = failureDetectorService.submit(this::failureDetectorTask);
        } else {
            log.debug("Cannot initiate new polling task. Polling in progress.");
        }
    }

    /**
     * This contains the complete failure detection and handling mechanism.
     * <p>
     * It first checks whether the current node is bootstrapped.
     * If not, it continues checking in intervals of 1 second.
     * If yes, it sets up the corfuRuntime and continues execution
     * of the policy.
     * <p>
     * It executes the policy which detects and reports failures.
     * Once detected, it triggers the trigger handler which takes care of
     * dispatching the appropriate handler.
     * <p>
     * Currently executing the periodicPollPolicy.
     * It executes the the polling at an interval of every 1 second.
     * After every poll it checks for any failures detected.
     */
    private void failureDetectorTask() {

        CorfuRuntime corfuRuntime = getCorfuRuntime();
        corfuRuntime.invalidateLayout();

        // Fetch the latest layout view through the runtime.
        safeUpdateLayout(corfuRuntime.getLayoutView().getLayout());

        // Execute the failure detection policy once.
        failureDetectorPolicy.executePolicy(latestLayout, corfuRuntime);

        // Get the server status from the policy and check for failures.
        PollReport pollReport = failureDetectorPolicy.getServerStatus();

        // Analyze the poll report and trigger failure handler if needed.
        analyzePollReportAndTriggerHandler(pollReport);

    }

    /**
     * Analyzes the poll report and triggers the failure handler if status change
     * of node detected.
     * <p>
     * @param pollReport Poll report obtained from failure detection policy.
     */
    private void analyzePollReportAndTriggerHandler(PollReport pollReport) {

        // Check if handler has been initiated.
        if (!startFailureHandler) {
            log.warn("Failure Handler not yet initiated: {}", pollReport.toString());
            return;
        }

        final ManagementClient localManagementClient = corfuRuntime.getRouter(getLocalEndpoint()).getClient(ManagementClient.class);

        try {
            if (!pollReport.getIsFailurePresent()) {
                // CASE 1:
                // No Failures detected by polling policy.
                // We can check if we have unresponsive servers marked in the layout and
                // un-mark them as they respond to polling now.
                if (!latestLayout.getUnresponsiveServers().isEmpty()) {
                    log.info("Received response from unresponsive server");
                    localManagementClient.handleFailure(pollReport.getFailingNodes()).get();
                    return;
                }
                log.debug("No failures present.");

            } else if (!pollReport.getFailingNodes().isEmpty() && !latestLayout.getUnresponsiveServers().isEmpty()) {
                // CASE 2:
                // Failures detected - unresponsive servers.
                // We check if these servers are the same set of servers which are marked as
                // unresponsive in the layout. If yes take no action. Else trigger handler.
                log.info("Failures detected. Failed nodes : {}", pollReport.toString());
                // Check if this failure has already been recognized.
                for (String failedServer : pollReport.getFailingNodes()) {
                    if (!latestLayout.getUnresponsiveServers().contains(failedServer)) {
                        localManagementClient.handleFailure(pollReport.getFailingNodes()).get();
                        return;
                    }
                }
                log.debug("Failure already taken care of.");

            } else {
                // CASE 3:
                // Failures detected but not marked in the layout or
                // some servers have been partially sealed to new epoch or stuck on
                // the previous epoch.
                localManagementClient.handleFailure(pollReport.getFailingNodes()).get();
                // TODO: Only re-sealing needed if server stuck on a previous epoch.
            }

        } catch (Exception e) {
            log.error("Exception invoking failure handler : {}", e);
        }
    }

    /**
     * Management Server shutdown:
     * Shuts down the fault detector service.
     */
    public void shutdown() {
        super.shutdown();
        // Shutting the fault detector.
        failureDetectorService.shutdownNow();
        // Shutting down local metrics polling.
        localMetricsPollingService.shutdownNow();

        // Shut down the Corfu Runtime.
        if (corfuRuntime != null) {
            corfuRuntime.shutdown();
        }

        try {
            failureDetectorService.awaitTermination(ServerContext.SHUTDOWN_TIMER.getSeconds(), TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            log.debug("failureDetectorService awaitTermination interrupted : {}", ie);
        }
        log.info("Management Server shutting down.");
    }
}
