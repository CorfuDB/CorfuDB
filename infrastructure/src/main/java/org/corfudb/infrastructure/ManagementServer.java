package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.TextFormat;
import io.netty.channel.ChannelHandlerContext;
import javax.annotation.Nonnull;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.management.ClusterStateContext;
import org.corfudb.infrastructure.management.FailureDetector;
import org.corfudb.infrastructure.management.ReconfigurationEventHandler;
import org.corfudb.infrastructure.orchestrator.Orchestrator;
import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.UnreachableClusterException;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg.PayloadCase;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.Management.HealFailureRequestMsg;
import org.corfudb.runtime.proto.service.Management.ReportFailureRequestMsg;
import org.corfudb.runtime.view.IReconfigurationHandlerPolicy;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.concurrent.SingletonResource;

import static org.corfudb.protocols.CorfuProtocolCommon.getLayout;
import static org.corfudb.protocols.CorfuProtocolServerErrors.getBootstrappedErrorMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getBootstrapManagementResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getHealFailureResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getManagementLayoutResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getQueryNodeResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getReportFailureResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;


/**
 * Handles reconfiguration and workflow requests to the Management Server.
 * Spawns the following services:
 * - Management Agent.
 * - Orchestrator.
 *
 * <p>Created by zlokhandwala on 9/28/16.
 */
@Slf4j
public class ManagementServer extends AbstractServer {

    private final ServerContext serverContext;

    /**
     * A {@link SingletonResource} which provides a {@link CorfuRuntime}.
     */
    private final SingletonResource<CorfuRuntime> corfuRuntime;

    /**
     * Policy to be used to handle failures/healing.
     */
    private final IReconfigurationHandlerPolicy failureHandlerPolicy;

    private final ClusterStateContext clusterContext;

    @Getter
    private final ManagementAgent managementAgent;

    private final Orchestrator orchestrator;

    /**
     * RequestHandlerMethods for the Management server
     */
    @Getter
    private final RequestHandlerMethods handlerMethods =
            RequestHandlerMethods.generateHandler(MethodHandles.lookup(), this);

    /**
     * System down handler to break out of live-locks if the runtime cannot reach the cluster for a
     * certain amount of time. This handler can be invoked at anytime if the Runtime is stuck and
     * cannot make progress on an RPC call after trying for more than
     * SYSTEM_DOWN_HANDLER_TRIGGER_LIMIT number of retries.
     */
    private final Runnable runtimeSystemDownHandler = () -> {
        log.warn("ManagementServer: Runtime stalled. Invoking systemDownHandler after {} "
                + "unsuccessful tries.", SYSTEM_DOWN_HANDLER_TRIGGER_LIMIT);
        throw new UnreachableClusterException("Runtime stalled. Invoking systemDownHandler after "
                + SYSTEM_DOWN_HANDLER_TRIGGER_LIMIT + " unsuccessful tries.");
    };

    /**
     * The number of tries to be made to execute any RPC request before the runtime gives up and
     * invokes the systemDownHandler.
     * This is set to 60  based on the fact that the sleep duration between RPC retries is
     * defaulted to 1 second in the Runtime parameters. This gives the Runtime a total of 1 minute
     * to make progress. Else the ongoing task is aborted.
     */
    private static final int SYSTEM_DOWN_HANDLER_TRIGGER_LIMIT = 60;

    private final ExecutorService executor;

    private final Lock healingLock = new ReentrantLock();

    @Override
    protected void processRequest(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
        if (req.getPayload().getPayloadCase().equals(PayloadCase.QUERY_NODE_REQUEST)) {
            // Execute this type of request on the IO thread
            getHandlerMethods().handle(req, ctx, r);
        } else {
            executor.submit(() -> getHandlerMethods().handle(req, ctx, r));
        }
    }

    /**
     * Management Server shutdown:
     * Shuts down the fault detector service.
     */
    @Override
    public void shutdown() {
        super.shutdown();
        executor.shutdown();
        orchestrator.shutdown();
        managementAgent.shutdown();

        // Shut down the Corfu Runtime.
        corfuRuntime.cleanup(CorfuRuntime::shutdown);
    }

    /**
     * Returns new ManagementServer.
     *
     * @param serverContext      context object providing parameters and objects
     * @param serverInitializer  a ManagementServerInitializer object
     */
    public ManagementServer(ServerContext serverContext, ManagementServerInitializer serverInitializer) {
        this.serverContext = serverContext;

        corfuRuntime = SingletonResource.withInitial(this::getNewCorfuRuntime);

        executor = serverContext.getExecutorService(serverContext.getConfiguration().getNumManagementServerThreads(), "management-");
        failureHandlerPolicy = serverContext.getFailureHandlerPolicy();

        clusterContext = serverInitializer.buildDefaultClusterContext(serverContext);
        managementAgent = serverInitializer.buildManagementAgent(corfuRuntime, serverContext, clusterContext);
        orchestrator = serverInitializer.buildOrchestrator(corfuRuntime, serverContext);
    }

    /**
     * Returns a connected instance of the CorfuRuntime.
     *
     * @return A connected instance of runtime.
     */
    private CorfuRuntime getNewCorfuRuntime() {
        final CorfuRuntime.CorfuRuntimeParameters params =
                serverContext.getManagementRuntimeParameters();
        params.setSystemDownHandlerTriggerLimit(SYSTEM_DOWN_HANDLER_TRIGGER_LIMIT);
        final CorfuRuntime runtime = CorfuRuntime.fromParameters(params);
        final Layout managementLayout = serverContext.copyManagementLayout();
        // Runtime can be set up either using the layout or the bootstrapEndpoint address.
        if (managementLayout != null) {
            managementLayout.getLayoutServers().forEach(runtime::addLayoutServer);
        }
        runtime.connect();
        log.info("getCorfuRuntime: Corfu Runtime connected successfully");
        params.setSystemDownHandler(runtimeSystemDownHandler);
        return runtime;
    }

    private boolean isBootstrapped(RequestMsg req) {
        if (serverContext.getManagementLayout() == null) {
            log.warn("Received message but not bootstrapped!"
                    + "Message={}", TextFormat.shortDebugString(req.getHeader()));
            return false;
        }

        return true;
    }

    /**
     * Forward an orchestrator request to the orchestrator service.
     *
     * @param req  a message containing an ORCHESTRATOR request
     * @param ctx  the netty ChannelHandlerContext
     * @param r    the server router
     */
    @RequestHandler(type = PayloadCase.ORCHESTRATOR_REQUEST)
    public synchronized void handleOrchestratorMsg(@Nonnull RequestMsg req,
                                                   @Nonnull ChannelHandlerContext ctx,
                                                   @Nonnull IServerRouter r) {
        log.debug("handleOrchestratorMsg: message:{}",
                TextFormat.shortDebugString(req.getPayload().getOrchestratorRequest()));
        orchestrator.handle(req, ctx, r);
    }

    /**
     * Bootstraps the management server.
     * The msg contains the Layout to be bootstrapped with.
     *
     * @param req  a message containing a BOOTSTRAP_MANAGEMENT request
     * @param ctx  the netty ChannelHandlerContext
     * @param r    the server router
     */
    @RequestHandler(type = PayloadCase.BOOTSTRAP_MANAGEMENT_REQUEST)
    public synchronized void handleBootstrapManagement(@Nonnull RequestMsg req,
                                                       @Nonnull ChannelHandlerContext ctx,
                                                       @Nonnull IServerRouter r) {
        final Layout layout = getLayout(req.getPayload().getBootstrapManagementRequest().getLayout());
        HeaderMsg responseHeader;
        ResponseMsg response;

        if (layout == null || layout.getClusterId() == null) {
            log.error("handleBootstrapManagement[{}]: Incomplete layout {}", req.getHeader().getRequestId(), layout);
            responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.CHECK);
            response = getResponseMsg(responseHeader, getBootstrapManagementResponseMsg(false));
        } else {
            final Layout managementLayout = serverContext.getManagementLayout();
            responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);

            if (managementLayout != null) {
                log.warn("handleBootstrapManagement[{}]: Already bootstrapped with {}, "
                        + "rejecting {}", req.getHeader().getRequestId(), managementLayout, layout);
                response = getResponseMsg(responseHeader, getBootstrappedErrorMsg());
            } else {
                log.info("handleBootstrapManagement[{}]: Received bootstrap " +
                        "layout {}", req.getHeader().getRequestId(), layout);
                serverContext.saveManagementLayout(layout);
                response = getResponseMsg(responseHeader, getBootstrapManagementResponseMsg(true));
            }
        }

        r.sendResponse(response, ctx);
    }

    /**
     * Triggers the failure handler.
     * The msg contains the failed/defected nodes.
     *
     * @param req  a message containing a REPORT_FAILURE request
     * @param ctx  the netty ChannelHandlerContext
     * @param r    the server router
     */
    @RequestHandler(type = PayloadCase.REPORT_FAILURE_REQUEST)
    public void handleReportFailure(@Nonnull RequestMsg req,
                                    @Nonnull ChannelHandlerContext ctx,
                                    @Nonnull IServerRouter r) {
        // If the server isn't bootstrapped yet, ignore the request
        if (!isBootstrapped(req)) {
            r.sendNoBootstrapError(req.getHeader(), ctx);
            return;
        }

        final ReportFailureRequestMsg payload = req.getPayload().getReportFailureRequest();
        log.info("handleReportFailure[{}]: request:{}",
                req.getHeader().getRequestId(), TextFormat.shortDebugString(payload));

        final Layout layout = serverContext.copyManagementLayout();
        HeaderMsg responseHeader;
        ResponseMsg response;

        // If this message is stamped with an older epoch which indicates the polling was
        // conducted in an old epoch. This message cannot be considered and is discarded.
        if (layout.getEpoch() != payload.getDetectorEpoch()) {
            log.error("handleReportFailure[{}]: Discarding stale detector message received. detectorEpoch:{} " +
                    "latestLayoutEpoch:{}", req.getHeader().getRequestId(), payload.getDetectorEpoch(), layout.getEpoch());

            responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.CHECK);
            response = getResponseMsg(responseHeader, getReportFailureResponseMsg(false));
            r.sendResponse(response, ctx);
            return;
        }

        // Collecting the failed nodes in the message that are amongst the responsive nodes in the layout
        final Set<String> allActiveServers = layout.getAllActiveServers();
        final Set<String> responsiveFailedNodes = payload.getFailedNodeList()
                .stream()
                .filter(allActiveServers::contains)
                .collect(Collectors.toSet());

        // If it is not an out of phase and there is no need to update the layout, return without
        // any reconfiguration
        if (!payload.getFailedNodeList().isEmpty() && responsiveFailedNodes.isEmpty()) {
            log.warn("handleReportFailure[{}]: No action taken as none of the failed nodes are responsive. " +
                            "failedNodes:{} responsiveLayoutNodes:{}, latestLayoutEpoch:{}",
                    req.getHeader().getRequestId(), payload.getFailedNodeList(), allActiveServers, layout.getEpoch());

            responseHeader = getHeaderMsg(req.getHeader());
            response = getResponseMsg(responseHeader, getReportFailureResponseMsg(true));
            r.sendResponse(response, ctx);
            return;
        }

        boolean result = ReconfigurationEventHandler.handleFailure(
                failureHandlerPolicy,
                layout,
                managementAgent.getCorfuRuntime(),
                responsiveFailedNodes);

        if (result) {
            responseHeader = getHeaderMsg(req.getHeader());
            response = getResponseMsg(responseHeader, getReportFailureResponseMsg(true));
        } else {
            log.error("handleReportFailure[{}]: Failure handling unsuccessful.", req.getHeader().getRequestId());
            responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.CHECK);
            response = getResponseMsg(responseHeader, getReportFailureResponseMsg(false));
        }

        r.sendResponse(response, ctx);
    }

    /**
     * Triggers the healing handler.
     * The msg contains the healed nodes.
     *
     * @param req  a message containing a HEAL_FAILURE request
     * @param ctx  the netty ChannelHandlerContext
     * @param r    the server router
     */
    @RequestHandler(type = PayloadCase.HEAL_FAILURE_REQUEST)
    public void handleHealFailure(@Nonnull RequestMsg req,
                                  @Nonnull ChannelHandlerContext ctx,
                                  @Nonnull IServerRouter r) {
        // If the server isn't bootstrapped yet, ignore the request
        if (!isBootstrapped(req)) {
            r.sendNoBootstrapError(req.getHeader(), ctx);
            return;
        }

        final HealFailureRequestMsg payload = req.getPayload().getHealFailureRequest();
        log.info("handleHealFailure[{}]: request:{}",
                req.getHeader().getRequestId(), TextFormat.shortDebugString(payload));

        final Layout layout = serverContext.copyManagementLayout();
        HeaderMsg responseHeader;
        ResponseMsg response;

        // If this message is stamped with an older epoch which indicates the polling was
        // conducted in an old epoch. This message cannot be considered and is discarded.
        if (payload.getDetectorEpoch() != layout.getEpoch()) {
            log.error("handleHealFailure[{}]: Discarding request received... detectorEpoch:{} latestLayoutEpoch:{}",
                    req.getHeader().getRequestId(), payload.getDetectorEpoch(), layout.getEpoch());

            responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.CHECK);
            response = getResponseMsg(responseHeader, getHealFailureResponseMsg(false));
            r.sendResponse(response, ctx);
            return;
        }

        final List<String> unresponsiveServers = layout.getUnresponsiveServers();
        final Set<String> unresponsiveHealedNodes = payload.getHealedNodeList()
                .stream()
                .filter(unresponsiveServers::contains)
                .collect(Collectors.toSet());

        if (!payload.getHealedNodeList().isEmpty() && unresponsiveHealedNodes.isEmpty()) {
            log.warn("handleHealFailure[{}]: No action taken as none of the healed nodes are unresponsive. " +
                            "healedNodes:{} unresponsiveLayoutNodes:{}, latestLayoutEpoch:{}",
                    req.getHeader().getRequestId(), payload.getHealedNodeList(), unresponsiveServers, layout.getEpoch());

            responseHeader = getHeaderMsg(req.getHeader());
            response = getResponseMsg(responseHeader, getHealFailureResponseMsg(true));
            r.sendResponse(response, ctx);
            return;
        }

        boolean result;
        if (healingLock.tryLock()) {
            try {
                log.info("handleHealFailure[{}]: Acquired healing lock. Performing healing for nodes: {}",
                        req.getHeader().getRequestId(), unresponsiveHealedNodes);

                final Duration retryWorkflowQueryTimeout = Duration.ofSeconds(1L);
                result = ReconfigurationEventHandler.handleHealing(
                        managementAgent.getCorfuRuntime(),
                        unresponsiveHealedNodes,
                        retryWorkflowQueryTimeout);
            } finally {
                healingLock.unlock();
            }
        } else {
            log.info("handleHealFailure[{}]: healing handling " +
                    "already in progress... Skipping.", req.getHeader().getRequestId());

            responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.CHECK);
            response = getResponseMsg(responseHeader, getHealFailureResponseMsg(false));
            r.sendResponse(response, ctx);
            return;
        }

        if (result) {
            responseHeader = getHeaderMsg(req.getHeader());
            response = getResponseMsg(responseHeader, getHealFailureResponseMsg(true));
        } else {
            log.error("handleHealFailure[{}]: healing handling unsuccessful.", req.getHeader().getRequestId());
            responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.CHECK);
            response = getResponseMsg(responseHeader, getHealFailureResponseMsg(false));
        }

        r.sendResponse(response, ctx);
    }

    /**
     * Returns current {@link NodeState} provided by failure detector.
     * The detector periodically collects current cluster state and saves it in {@link ClusterStateContext}.
     * Servers periodically inspect cluster and ask each other of the connectivity/node state
     * (connection status between current node and all the others).
     * The node provides its current node state.
     *
     * Default NodeState has been providing unless the node is not bootstrapped.
     * Failure detector updates ClusterNodeState by current state then current NodeState can be provided to other nodes.
     *
     * @param req  a message containing a QUERY_NODE request
     * @param ctx  the netty ChannelHandlerContext
     * @param r    the server router
     */
    @RequestHandler(type = PayloadCase.QUERY_NODE_REQUEST)
    public void handleQueryNode(@Nonnull RequestMsg req,
                                @Nonnull ChannelHandlerContext ctx,
                                @Nonnull IServerRouter r) {
        NodeState nodeState = clusterContext.getClusterView()
                .getNode(serverContext.getLocalEndpoint())
                .orElseGet(this::buildDefaultNodeState);

        HeaderMsg responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
        ResponseMsg response = getResponseMsg(responseHeader, getQueryNodeResponseMsg(nodeState));
        r.sendResponse(response, ctx);
    }

    /**
     * Build default {@link NodeState} nodes state to provide current connectivity status.
     *
     * @return node state
     */
    private NodeState buildDefaultNodeState() {
        log.info("Management server: {}, not ready yet, return default NodeState, current cluster view: {}",
                serverContext.getLocalEndpoint(), clusterContext.getClusterView());

        //Node state is connected by default.
        //We believe two servers are connected if another servers is able to send command QUERY_NODE_REQUEST
        // and get a response. If we are able to provide NodeState we believe that the state is CONNECTED.
        return NodeState.getNotReadyNodeState(serverContext.getLocalEndpoint());
    }

    /**
     * Handles the Management Layout request.
     *
     * @param req  a message containing a MANAGEMENT_LAYOUT request
     * @param ctx  the netty ChannelHandlerContext
     * @param r    the server router
     */
    @RequestHandler(type = PayloadCase.MANAGEMENT_LAYOUT_REQUEST)
    public void handleManagementLayoutMsg(@Nonnull RequestMsg req,
                                          @Nonnull ChannelHandlerContext ctx,
                                          @Nonnull IServerRouter r) {
        // This server has not been bootstrapped yet, ignore all requests.
        if (!isBootstrapped(req)) {
            r.sendNoBootstrapError(req.getHeader(), ctx);
            return;
        }

        ResponseMsg response = getResponseMsg(getHeaderMsg(req.getHeader()),
                getManagementLayoutResponseMsg(serverContext.getManagementLayout()));

        r.sendResponse(response, ctx);
    }

    /**
     * Utility class used by the ManagementServer.
     */
    public static class ManagementServerInitializer {
        ClusterStateContext buildDefaultClusterContext(@Nonnull ServerContext serverContext) {
            return ClusterStateContext.builder()
                    .clusterView(new AtomicReference<>(ClusterState.builder()
                            .localEndpoint(serverContext.getLocalEndpoint())
                            .nodes(ImmutableMap.of())
                            .unresponsiveNodes(ImmutableList.of())
                            .build()))
                    .build();
        }

        Orchestrator buildOrchestrator(@Nonnull SingletonResource<CorfuRuntime> corfuRuntime,
                                       @Nonnull ServerContext serverContext) {
            return new Orchestrator(corfuRuntime, serverContext, new Orchestrator.WorkflowFactory());
        }

        ManagementAgent buildManagementAgent(@Nonnull SingletonResource<CorfuRuntime> corfuRuntime,
                                             @Nonnull ServerContext serverContext,
                                             @Nonnull ClusterStateContext clusterContext) {
            return new ManagementAgent(corfuRuntime, serverContext, clusterContext,
                    new FailureDetector(serverContext.getLocalEndpoint()), serverContext.getManagementLayout());
        }
    }
}
