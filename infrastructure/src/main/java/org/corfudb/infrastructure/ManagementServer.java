package org.corfudb.infrastructure;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.channel.ChannelHandlerContext;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.format.Types.NodeMetrics;
import org.corfudb.infrastructure.management.IFailureDetectorPolicy;
import org.corfudb.infrastructure.management.PollReport;
import org.corfudb.infrastructure.management.ReconfigurationEventHandler;
import org.corfudb.infrastructure.orchestrator.Orchestrator;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.FailureDetectorMsg;
import org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.view.IFailureHandlerPolicy;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.QuorumFuturesFactory;

import javax.annotation.Nonnull;

/**
 * Instantiates and performs failure detection and handling asynchronously.
 *
 * <p>Failure Detector:
 * Executes detection policy (eg. PeriodicPollingPolicy).
 * It then checks for status of the nodes. If the result map is not empty,
 * there are failed nodes to be addressed. This then triggers the failure
 * handler which then responds to these failures based on a policy.
 *
 * <p>Created by zlokhandwala on 9/28/16.
 */
@Slf4j
public class ManagementServer extends AbstractServer {

    /**
     * The options map.
     */
    private final Map<String, Object> opts;
    private final ServerContext serverContext;

//    private static final String PREFIX_MANAGEMENT = "MANAGEMENT";
//    private static final String KEY_LAYOUT = "LAYOUT";

    private CorfuRuntime corfuRuntime;
    /**
     * Policy to be used to detect failures.
     */
    private IFailureDetectorPolicy failureDetectorPolicy;
    /**
     * Policy to be used to handle failures.
     */
    private IFailureHandlerPolicy failureHandlerPolicy;
    /**
     * Latest layout received from bootstrap or the runtime.
     */
    private volatile Layout latestLayout;
    /**
     * Bootstrap endpoint to seed the Management Server.
     */
    private String bootstrapEndpoint;
    /**
     * To determine whether the cluster is setup and the server is ready to
     * start handling the detected failures.
     */
    private boolean startFailureHandler = false;
    /**
     * Failure Handler Dispatcher to launch configuration changes or recovery.
     */
    private ReconfigurationEventHandler reconfigurationEventHandler;
    /**
     * Interval in executing the failure detection policy.
     * In milliseconds.
     */
    @Getter
    private final long policyExecuteInterval = 1000;
    /**
     * To schedule failure detection.
     */
    @Getter
    private final ScheduledExecutorService failureDetectorService;
    /**
     * Future for periodic failure detection task.
     */
    private Future failureDetectorFuture = null;
    private boolean recovered = false;

    @Getter
    private volatile CompletableFuture<Boolean> sequencerBootstrappedFuture;

    private final Orchestrator orchestrator;

    /**
     * Returns new ManagementServer.
     *
     * @param serverContext context object providing parameters and objects
     */
    public ManagementServer(ServerContext serverContext) {

        this.opts = serverContext.getServerConfig();
        this.serverContext = serverContext;

        bootstrapEndpoint = (opts.get("--management-server") != null)
                ? opts.get("--management-server").toString() : null;
        sequencerBootstrappedFuture = new CompletableFuture<>();


        safeUpdateLayout(serverContext.getManagementLayout());
        // If no state was preserved, there is no layout to recover.
        if (latestLayout == null) {
            recovered = true;
        }

        serverContext.installSingleNodeLayoutIfAbsent();
        safeUpdateLayout(serverContext.getCurrentLayout());

        this.failureDetectorPolicy = serverContext.getFailureDetectorPolicy();
        this.failureHandlerPolicy = serverContext.getFailureHandlerPolicy();
        this.reconfigurationEventHandler = new ReconfigurationEventHandler();
        this.failureDetectorService = Executors.newScheduledThreadPool(
                2,
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat(serverContext.getThreadPrefix() + "FaultDetector-%d")
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

        orchestrator = new Orchestrator(this::getCorfuRuntime, serverContext);
    }

    private void bootstrapPrimarySequencerServer() {
        try {
            String primarySequencer = latestLayout.getSequencers().get(0);
            boolean bootstrapResult = getCorfuRuntime().getRouter(primarySequencer)
                    .getClient(SequencerClient.class)
                    .bootstrap(0L, Collections.emptyMap(), latestLayout.getEpoch())
                    .get();
            sequencerBootstrappedFuture.complete(bootstrapResult);
            if (!bootstrapResult) {
                log.warn("Sequencer already bootstrapped.");
            } else {
                log.info("Bootstrapped sequencer server at epoch:{}", latestLayout.getEpoch());
            }
            return;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Bootstrapping sequencer failed. Retrying: {}", e);
        }
        getCorfuRuntime().invalidateLayout();
    }

    private boolean recover() {
            boolean recoveryResult = reconfigurationEventHandler
                    .recoverCluster(new Layout(latestLayout), getCorfuRuntime());
            safeUpdateLayout(corfuRuntime.getLayoutView().getLayout());
            return recoveryResult;

    }

    /**
     * Handler for this server.
     */
    @Getter
    private final CorfuMsgHandler handler =
        CorfuMsgHandler.generateHandler(MethodHandles.lookup(), this);

    /**
     * Thread safe updating of layout only if new layout has higher epoch value.
     *
     * @param layout New Layout
     */
    private synchronized void safeUpdateLayout(Layout layout) {
        // Cannot update with a null layout.
        if (layout == null) {
            log.warn("safeUpdateLayout: Attempted to update with null layout");
            return;
        }

        // Update only if new layout has a higher epoch than the existing layout.
        if (latestLayout == null || layout.getEpoch() > latestLayout.getEpoch()) {
            latestLayout = layout;
            log.info("safeUpdateLayout: Updating to new layout at epoch {}",
                    latestLayout.getEpoch());
            // Persisting this new updated layout
            serverContext.setManagementLayout(latestLayout);
        } else {
            log.debug("safeUpdateLayout: Ignoring layout because new epoch {} <= old epoch {}",
                    layout.getEpoch(), latestLayout.getEpoch());
        }
    }

    private boolean checkBootstrap(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        if (latestLayout == null && bootstrapEndpoint == null) {
            log.warn("Received message but not bootstrapped! Message={}", msg);
            return false;
        }
        return true;
    }

    /**
     * Forward an orchestrator request to the orchestrator service.
     *
     * @param msg corfu message containing ORCHESTRATOR_REQUEST
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    @ServerHandler(type = CorfuMsgType.ORCHESTRATOR_REQUEST)
    public synchronized void handleOrchestratorMsg(@Nonnull CorfuPayloadMsg<OrchestratorMsg> msg,
                                                   @Nonnull ChannelHandlerContext ctx,
                                                   @Nonnull IServerRouter r) {
        log.debug("Received an orchestrator message {}", msg);
        orchestrator.handle(msg, ctx, r);
    }

    /**
     * Bootstraps the management server.
     * The msg contains the layout to be bootstrapped.
     *
     * @param msg corfu message containing MANAGEMENT_BOOTSTRAP_REQUEST
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    @ServerHandler(type = CorfuMsgType.MANAGEMENT_BOOTSTRAP_REQUEST)
    public synchronized void handleManagementBootstrap(CorfuPayloadMsg<Layout> msg,
                                                       ChannelHandlerContext ctx, IServerRouter r) {
        if (latestLayout != null) {
            // We are already bootstrapped, bootstrap again is not allowed.
            log.warn("Got a request to bootstrap a server which is already bootstrapped, "
                    + "rejecting!");
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.MANAGEMENT_ALREADY_BOOTSTRAP_ERROR));
        } else {
            log.info("Received Bootstrap Layout : {}", msg.getPayload());
            safeUpdateLayout(msg.getPayload());
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
        }
    }

    /**
     * Trigger to start the failure handler.
     *
     * @param msg corfu message containing MANAGEMENT_START_FAILURE_HANDLER
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    @ServerHandler(type = CorfuMsgType.MANAGEMENT_START_FAILURE_HANDLER)
    public synchronized void initiateFailureHandler(CorfuMsg msg, ChannelHandlerContext ctx,
                                                    IServerRouter r) {

        // This server has not been bootstrapped yet, ignore all requests.
        if (!checkBootstrap(msg, ctx, r)) {
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.MANAGEMENT_NOBOOTSTRAP_ERROR));
            return;
        }

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
     * @param msg corfu message containing MANAGEMENT_FAILURE_DETECTED
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    @ServerHandler(type = CorfuMsgType.MANAGEMENT_FAILURE_DETECTED)
    public synchronized void handleFailureDetectedMsg(CorfuPayloadMsg<FailureDetectorMsg> msg,
                                                      ChannelHandlerContext ctx, IServerRouter r) {

        // This server has not been bootstrapped yet, ignore all requests.
        if (!checkBootstrap(msg, ctx, r)) {
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.MANAGEMENT_NOBOOTSTRAP_ERROR));
            return;
        }

        log.info("handleFailureDetectedMsg: Received Failures : {}",
                msg.getPayload().getFailedNodes());
            boolean result = reconfigurationEventHandler.handleFailure(
                    failureHandlerPolicy,
                    new Layout(latestLayout),
                    getCorfuRuntime(),
                    msg.getPayload().getFailedNodes(),
                    msg.getPayload().getHealedNodes());
            if (result) {
                r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
            } else {
                log.error("handleFailureDetectedMsg: failure handling unsuccessful.");
                r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.NACK));
            }

    }

    /**
     * Handles the heartbeat request.
     * It accumulates the metrics required to build
     * and send the response(NodeMetrics).
     *
     * @param msg corfu message containing HEARTBEAT_REQUEST
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    @ServerHandler(type = CorfuMsgType.HEARTBEAT_REQUEST)
    public void handleHeartbeatRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        // Currently builds a default instance of the model.
        // TODO: Collect metrics from Layout, Sequencer and LogUnit Servers.
        NodeMetrics nodeMetrics = NodeMetrics.getDefaultInstance();
        r.sendResponse(ctx, msg, new CorfuPayloadMsg<>(CorfuMsgType.HEARTBEAT_RESPONSE,
                nodeMetrics.toByteArray()));
    }

    /**
     * Returns a connected instance of the CorfuRuntime.
     *
     * @return A connected instance of runtime.
     */
    public synchronized CorfuRuntime getCorfuRuntime() {

        if (corfuRuntime == null) {
            CorfuRuntimeParameters params = serverContext.getDefaultRuntimeParameters();
            corfuRuntime = CorfuRuntime.fromParameters(params);
            // Runtime can be set up either using the layout or the bootstrapEndpoint address.
            if (latestLayout != null) {
                latestLayout.getLayoutServers().forEach(ls -> corfuRuntime.addLayoutServer(ls));
            } else {
                corfuRuntime.addLayoutServer(bootstrapEndpoint);
            }
            corfuRuntime.connect();
            log.info("getCorfuRuntime: Corfu Runtime connected successfully");
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
     * Schedules the failure detector task only if the previous task is completed.
     */
    private synchronized void failureDetectorScheduler() {
        if (latestLayout == null && bootstrapEndpoint == null) {
            log.warn("Management Server waiting to be bootstrapped");
            return;
        }
        // Recover if flag is false
        if (!recovered) {
            recovered = recover();
            if (!recovered) {
                log.error("failureDetectorScheduler: Recovery failed. Retrying.");
                return;
            }
            // If recovery succeeds, reconfiguration was successful.
            sequencerBootstrappedFuture.complete(true);
        } else if (!sequencerBootstrappedFuture.isDone()) {
            // This should be invoked only once in case of a clean startup (not recovery).
            bootstrapPrimarySequencerServer();
        }

        if (failureDetectorFuture == null || failureDetectorFuture.isDone()) {
            failureDetectorFuture = failureDetectorService.submit(this::failureDetectorTask);
        } else {
            log.debug("Cannot initiate new polling task. Polling in progress.");
        }
    }

    /**
     * This contains the complete failure detection and handling mechanism.
     *
     * <p>It first checks whether the current node is bootstrapped.
     * If not, it continues checking in intervals of 1 second.
     * If yes, it sets up the corfuRuntime and continues execution
     * of the policy.
     *
     * <p>It executes the policy which detects and reports failures.
     * Once detected, it triggers the trigger handler which takes care of
     * dispatching the appropriate handler.
     *
     * <p>Currently executing the periodicPollPolicy.
     * It executes the the polling at an interval of every 1 second.
     * After every poll it checks for any failures detected.
     */
    private void failureDetectorTask() {

        CorfuRuntime corfuRuntime = getCorfuRuntime();
        corfuRuntime.invalidateLayout();

        safeUpdateLayout(corfuRuntime.getLayoutView().getLayout());

        // Execute the failure detection policy once.
        failureDetectorPolicy.executePolicy(latestLayout, corfuRuntime);

        // Get the server status from the policy and check for failures.
        PollReport pollReport = failureDetectorPolicy.getServerStatus();

        // Corrects out of phase epoch issues if present in the report. This method performs
        // re-sealing of all nodes if required and catchup of a layout server to the current state.
        correctOutOfPhaseEpochs(pollReport);

        // Analyze the poll report and trigger failure handler if needed.
        analyzePollReportAndTriggerHandler(pollReport);

    }

    /**
     * We can check if we have unresponsive servers marked in the layout and un-mark them as they
     * respond to polling now.
     *
     * @param pollReport Report from the polling task
     * @return Set of nodes which have healed relative to the latest local copy of the layout.
     */
    private Set<String> getHealedNodes(PollReport pollReport) {
        return Sets.difference(
                new HashSet<>(latestLayout.getUnresponsiveServers()),
                pollReport.getFailingNodes());
    }

    /**
     * We check if these servers are the same set of servers which are marked as unresponsive in
     * the layout.
     * Check if this new detected failure has already been recognized.
     *
     * @param pollReport Report from the polling task
     * @return Set of nodes which have failed, relative to the latest local copy of the layout.
     */
    private Set<String> getNewFailures(PollReport pollReport) {
        return Sets.difference(
                pollReport.getFailingNodes(),
                new HashSet<>(latestLayout.getUnresponsiveServers()));
    }

    /**
     * All Layout servers have been sealed but there is no client to take this forward and fill the
     * slot by proposing a new layout.
     * In this case we can pass an empty set to propose the same layout again and fill the layout
     * slot to un-block the data plane operations.
     *
     * @param pollReport Report from the polling task
     * @return True if latest layout slot is vacant. Else False.
     */
    private boolean checkIfCurrentLayoutSlotUnFilled(PollReport pollReport) {
        return pollReport.getOutOfPhaseEpochNodes().keySet()
                .containsAll(latestLayout.getLayoutServers());
    }

    /**
     * Analyzes the poll report and triggers the failure handler if status change
     * of node detected.
     *
     * @param pollReport Poll report obtained from failure detection policy.
     */
    private void analyzePollReportAndTriggerHandler(PollReport pollReport) {

        // Check if handler has been initiated.
        if (!startFailureHandler) {
            log.debug("Failure Handler not yet initiated: {}", pollReport.toString());
            return;
        }

        // We check for 2 conditions here: If the node is a part of the current layout or has it
        // been marked as unresponsive. If either is true, it should not attempt to change layout.
        if (!latestLayout.getAllServers().contains(getLocalEndpoint())
                || latestLayout.getUnresponsiveServers().contains(getLocalEndpoint())) {
            log.warn("This Server is not a part of the active layout. Aborting failure handling.");
            return;
        }

        final ManagementClient localManagementClient = corfuRuntime.getRouter(getLocalEndpoint())
                .getClient(ManagementClient.class);

        try {
            Set<String> failedNodes = new HashSet<>();
            Set<String> healedNodes = new HashSet<>();

            healedNodes.addAll(getHealedNodes(pollReport));
            failedNodes.addAll(getNewFailures(pollReport));

            // These conditions are mutually exclusive. If there is a failure or healing to be
            // handled, we don't need to explicitly fix the unfilled layout slot. Else we do.
            if (failedNodes.isEmpty() && healedNodes.isEmpty()) {
                if (checkIfCurrentLayoutSlotUnFilled(pollReport)) {
                    log.info("Current layout slot is empty. Filling slot with current layout.");
                    localManagementClient
                            .handleFailure(Collections.emptySet(), Collections.emptySet()).get();
                }
                return;
            }

            log.info("Detected changes in node responsiveness: Failed:{}, Healed:{}, pollReport:{}",
                    failedNodes, healedNodes, pollReport);
            localManagementClient.handleFailure(failedNodes, healedNodes).get();

        } catch (Exception e) {
            log.error("Exception invoking failure handler : {}", e);
        }
    }

    /**
     * Corrects out of phase epochs by resealing the servers.
     * This would also need to update trailing layout servers.
     *
     * @param pollReport Poll Report from running the failure detection policy.
     */
    private void correctOutOfPhaseEpochs(PollReport pollReport) {

        // Check if handler has been initiated.
        if (!startFailureHandler) {
            log.debug("Failure Handler not yet initiated: {}", pollReport.toString());
            return;
        }

        // If node has been removed. Then it should not attempt to change layout.
        if (!latestLayout.getAllServers().contains(getLocalEndpoint())
                || latestLayout.getUnresponsiveServers().contains(getLocalEndpoint())) {
            log.warn("This Server is not a part of the active layout. Aborting failure handling.");
            return;
        }

        final Map<String, Long> outOfPhaseEpochNodes = pollReport.getOutOfPhaseEpochNodes();
        if (outOfPhaseEpochNodes.isEmpty()) {
            return;
        }

        try {

            // Query all layout servers to get quorum Layout.
            Map<String, CompletableFuture<Layout>> layoutCompletableFutureMap = new HashMap<>();
            for (String layoutServer : latestLayout.getLayoutServers()) {
                layoutCompletableFutureMap.put(
                        layoutServer, getCorfuRuntime().getRouter(layoutServer)
                                .getClient(LayoutClient.class).getLayout());
            }

            // Retrieve the correct layout from quorum of members to reseal servers.
            // If we are unable to reach a consensus from a quorum we get an exception and
            // abort the epoch correction phase.
            Layout quorumLayout = fetchQuorumLayout(layoutCompletableFutureMap.values()
                    .toArray(new CompletableFuture[layoutCompletableFutureMap.size()]));

            // Update local layout copy.
            safeUpdateLayout(quorumLayout);

            // We clone the layout to not pollute the original latestLayout.
            Layout sealLayout = new Layout(latestLayout);
            sealLayout.setRuntime(getCorfuRuntime());

            // In case of a partial seal, a set of servers can be sealed with a higher epoch.
            // We should be able to detect this and bring the rest of the servers to this epoch.
            long maxOutOfPhaseEpoch = Collections.max(outOfPhaseEpochNodes.values());
            if (maxOutOfPhaseEpoch > latestLayout.getEpoch()) {
                sealLayout.setEpoch(maxOutOfPhaseEpoch);
            }

            // Re-seal all servers with the latestLayout epoch.
            // This has no effect on up-to-date servers. Only the trailing servers are caught up.
            sealLayout.moveServersToEpoch();

            // Check if any layout server has a stale layout.
            // If yes patch it (commit) with the latestLayout (received from quorum).
            updateTrailingLayoutServers(layoutCompletableFutureMap);

        } catch (QuorumUnreachableException e) {
            log.error("Error in correcting server epochs: {}", e);
        }
    }

    /**
     * Fetches the updated layout from quorum of layout servers.
     *
     * @return quorum agreed layout.
     * @throws QuorumUnreachableException If unable to receive consensus on layout.
     */
    private Layout fetchQuorumLayout(CompletableFuture<Layout>[] completableFutures)
            throws QuorumUnreachableException {

        QuorumFuturesFactory.CompositeFuture<Layout> quorumFuture = QuorumFuturesFactory
                .getQuorumFuture(
                        Comparator.comparing(Layout::asJSONString),
                        completableFutures);
        try {
            return quorumFuture.get();
        } catch (ExecutionException | InterruptedException e) {
            if (e.getCause() instanceof QuorumUnreachableException) {
                throw (QuorumUnreachableException) e.getCause();
            }

            int reachableServers = (int) Arrays.stream(completableFutures)
                    .filter(booleanCompletableFuture -> !booleanCompletableFuture
                            .isCompletedExceptionally()).count();
            throw new QuorumUnreachableException(reachableServers, completableFutures.length);
        }
    }

    /**
     * Finds all trailing layout servers and patches them with the latestLayout
     * retrieved by quorum.
     *
     * @param layoutCompletableFutureMap Map of layout server endpoints to their layout requests.
     */
    private void updateTrailingLayoutServers(
            Map<String, CompletableFuture<Layout>> layoutCompletableFutureMap) {

        // Patch trailing layout servers with latestLayout.
        layoutCompletableFutureMap.keySet().forEach(layoutServer -> {
            Layout layout = null;
            try {
                layout = layoutCompletableFutureMap.get(layoutServer).get();
            } catch (InterruptedException | ExecutionException e) {
                // Expected wrong epoch exception if layout server fell behind and has stale
                // layout and server epoch.
                log.warn("updateTrailingLayoutServers: layout fetch failed: {}", e);
            }

            // Do nothing if this layout server is updated with the latestLayout.
            if (layout != null && layout.equals(latestLayout)) {
                return;
            }
            try {
                // Committing this layout directly to the trailing layout servers.
                // This is safe because this layout is acquired by a quorum fetch which confirms
                // that there was a consensus on this layout and has been committed to a quorum.
                boolean result = getCorfuRuntime().getRouter(layoutServer)
                        .getClient(LayoutClient.class)
                        .committed(latestLayout.getEpoch(), latestLayout).get();
                if (result) {
                    log.debug("Layout Server: {} successfully patched with latest layout : {}",
                            layoutServer, latestLayout);
                } else {
                    log.debug("Layout Server: {} patch with latest layout failed : {}",
                            layoutServer, latestLayout);
                }
            } catch (InterruptedException | ExecutionException e) {
                log.error("Updating layout servers failed due to : {}", e);
            }
        });
    }

    /**
     * Management Server shutdown:
     * Shuts down the fault detector service.
     */
    public void shutdown() {
        super.shutdown();
        // Shutting the fault detector.
        failureDetectorService.shutdownNow();

        // Shut down the Corfu Runtime.
        if (corfuRuntime != null) {
            corfuRuntime.shutdown();
        }

        try {
            failureDetectorService.awaitTermination(ServerContext.SHUTDOWN_TIMER.getSeconds(),
                    TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            log.debug("failureDetectorService awaitTermination interrupted : {}", ie);
        }
        log.info("Management Server shutting down.");
    }
}
