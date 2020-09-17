package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.management.ClusterStateContext;
import org.corfudb.infrastructure.management.FailureDetector;
import org.corfudb.infrastructure.management.ReconfigurationEventHandler;
import org.corfudb.infrastructure.orchestrator.Orchestrator;
import org.corfudb.protocols.API;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.DetectorMsg;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.failuredetector.FailureDetectorMetrics;
import org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.UnreachableClusterException;
import org.corfudb.runtime.protocol.proto.CorfuProtocol;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.BootstrapManagementResponse;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.Header;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.HealFailureResponse;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.ReportFailureResponse;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.Request;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.Response;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.ServerError;
import org.corfudb.runtime.view.IReconfigurationHandlerPolicy;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.concurrent.SingletonResource;

import javax.annotation.Nonnull;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;


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
    private final SingletonResource<CorfuRuntime> corfuRuntime =
            SingletonResource.withInitial(this::getNewCorfuRuntime);

    /**
     * Policy to be used to handle failures/healing.
     */
    private final IReconfigurationHandlerPolicy failureHandlerPolicy;

    private final ClusterStateContext clusterContext;

    @Getter
    private final ManagementAgent managementAgent;

    private final Orchestrator orchestrator;

    /**
     * HandlerMethod for this server.
     * [RM] Remove this after Protobuf for RPC Completion
     */
    @Getter
    private final HandlerMethods handler = HandlerMethods.generateHandler(MethodHandles.lookup(), this);

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

    // [RM] Remove this after Protobuf for RPC Completion
    @Override
    public boolean isServerReadyToHandleMsg(CorfuMsg msg) {
        return getState() == ServerState.READY;
    }

    @Override
    public boolean isServerReadyToHandleReq(Header requestHeader) {
        return getState() == ServerState.READY;
    }

    // [RM] Remove this after Protobuf for RPC Completion
    @Override
    protected void processRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        if (msg.getMsgType() == CorfuMsgType.NODE_STATE_REQUEST) {
            // Execute this request on the io thread
            getHandler().handle(msg, ctx, r);
        } else {
            executor.submit(() -> getHandler().handle(msg, ctx, r));
        }
    }

    @Override
    protected void processRequest(Request req, ChannelHandlerContext ctx, IRequestRouter r) {
        if(req.getHeader().getType().equals(CorfuProtocol.MessageType.QUERY_NODE)) {
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
     * @param serverContext context object providing parameters and objects
     */
    public ManagementServer(ServerContext serverContext) {
        this.serverContext = serverContext;

        this.executor = Executors.newFixedThreadPool(serverContext.getManagementServerThreadCount(),
                new ServerThreadFactory("management-", new ServerThreadFactory.ExceptionHandler()));

        this.failureHandlerPolicy = serverContext.getFailureHandlerPolicy();


        FailureDetector failureDetector = new FailureDetector(serverContext.getLocalEndpoint());

        // Creating a management agent.
        ClusterState defaultView = ClusterState.builder()
                .localEndpoint(serverContext.getLocalEndpoint())
                .nodes(ImmutableMap.of())
                .unresponsiveNodes(ImmutableList.of())
                .build();
        clusterContext = ClusterStateContext.builder()
                .clusterView(new AtomicReference<>(defaultView))
                .build();

        Layout managementLayout = serverContext.copyManagementLayout();
        managementAgent = new ManagementAgent(
                corfuRuntime, serverContext, clusterContext, failureDetector, managementLayout
        );

        orchestrator = new Orchestrator(corfuRuntime, serverContext);
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

    // [RM] Remove this after Protobuf for RPC Completion
    private boolean isBootstrapped(CorfuMsg msg) {
        if (serverContext.getManagementLayout() == null) {
            log.warn("Received message but not bootstrapped! Message={}", msg);
            return false;
        }
        return true;
    }

    private boolean isBootstrapped(Request req, ChannelHandlerContext ctx, IRequestRouter r) {
        if(serverContext.getManagementLayout() == null) {
            r.sendNoBootstrapError(req.getHeader(), ctx);
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

    @RequestHandler(type = CorfuProtocol.MessageType.ORCHESTRATOR)
    public synchronized void handleOrchestratorMsg(Request req, ChannelHandlerContext ctx, IRequestRouter r) {
        log.debug("handleOrchestratorMsg: Received an orchestrator request {}", req);
        orchestrator.handle(req, ctx, r);
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
        if (serverContext.getManagementLayout() != null) {
            // We are already bootstrapped, bootstrap again is not allowed.
            log.warn("handleManagementBootstrap: Got a request to bootstrap a server " +
                    "with {} which is already bootstrapped, rejecting!", msg.getPayload());
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.MANAGEMENT_ALREADY_BOOTSTRAP_ERROR));
        } else {
            Layout layout = msg.getPayload();
            log.info("handleManagementBootstrap: received bootstrap layout : {}", layout);
            if(layout.getClusterId() == null){
                log.warn("handleManagementBootstrap: clusterId for the layout {} is not present.",
                        layout.getClusterId());
                r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.NACK));
            }
            else{
                serverContext.saveManagementLayout(layout);
                r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
            }
        }
    }

    @RequestHandler(type = CorfuProtocol.MessageType.BOOTSTRAP_MANAGEMENT)
    public synchronized  void handleManagementBootstrap(Request req, ChannelHandlerContext ctx, IRequestRouter r) {
        final Layout layout = API.fromProtobufLayout(req.getBootstrapManagementRequest().getLayout());
        Header responseHeader;
        Response response;

        if(serverContext.getManagementLayout() != null) {
            log.warn("handleManagementBootstrap[{}]: Got a request to bootstrap a server, with layout " +
                    "{}, which is already bootstrapped... Rejecting!", req.getHeader().getRequestId(), layout);

            responseHeader = API.generateResponseHeader(req.getHeader(), false, true);
            response = API.getErrorResponseNoPayload(responseHeader, API.getBootstrappedServerError());
        } else {
            log.info("handleManagementBootstrap[{}]: Received bootstrap " +
                    "layout {}", req.getHeader().getRequestId(), layout);

            if(layout.getClusterId() == null) {
                log.warn("handleManagementBootstrap[{}]: clusterId " +
                        "for the layout is not present", req.getHeader().getRequestId());

                responseHeader = API.generateResponseHeader(req.getHeader(), false, false);
                response = API.getBootstrapManagementResponse(responseHeader, BootstrapManagementResponse.Type.NACK);

            } else {
                serverContext.saveManagementLayout(layout);

                responseHeader = API.generateResponseHeader(req.getHeader(), false, true);
                response = API.getBootstrapManagementResponse(responseHeader, BootstrapManagementResponse.Type.ACK);
            }
        }

        r.sendResponse(response, ctx);
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
    public void handleFailureDetectedMsg(CorfuPayloadMsg<DetectorMsg> msg,
                                         ChannelHandlerContext ctx, IServerRouter r) {

        // This server has not been bootstrapped yet, ignore all requests.
        if (!isBootstrapped(msg)) {
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.MANAGEMENT_NOBOOTSTRAP_ERROR));
            return;
        }

        log.info("handleFailureDetectedMsg: Received DetectorMsg : {}", msg.getPayload());

        DetectorMsg detectorMsg = msg.getPayload();
        Layout layout = serverContext.copyManagementLayout();

        // If this message is stamped with an older epoch which indicates the polling was
        // conducted in an old epoch. This message cannot be considered and is discarded.
        if (!detectorMsg.getDetectorEpoch().equals(layout.getEpoch())) {
            log.error("handleFailureDetectedMsg: Discarding stale detector message received. "
                            + "detectorEpoch:{} latestLayout epoch:{}",
                    detectorMsg.getDetectorEpoch(), layout.getEpoch());
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.NACK));
            return;
        }

        // Collecting the failed nodes in the message that are amongst the responsive nodes in the
        // layout
        final Set<String> allActiveServers = layout.getAllActiveServers();
        final Set<String> responsiveFailedNodes = detectorMsg.getFailedNodes()
                .stream()
                .filter(allActiveServers::contains)
                .collect(Collectors.toSet());

        // If it is not an out of phase and there is no need to update the layout, return without
        // any reconfiguration
        if (!detectorMsg.getFailedNodes().isEmpty() && responsiveFailedNodes.isEmpty()) {
            log.warn("handleFailureDetectedMsg: No action is taken as none of the failed nodes " +
                            "are responsive. failedNodes:{} responsive layout nodes: {}, " +
                            "latestLayout epoch:{} ",
                    detectorMsg.getFailedNodes(), allActiveServers, layout.getEpoch());
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
            return;
        }

        boolean result = ReconfigurationEventHandler.handleFailure(
                failureHandlerPolicy,
                layout,
                managementAgent.getCorfuRuntime(),
                responsiveFailedNodes);

        if (result) {
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
        } else {
            log.error("handleFailureDetectedMsg: failure handling unsuccessful.");
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.NACK));
        }
    }

    @RequestHandler(type = CorfuProtocol.MessageType.REPORT_FAILURE)
    public void handleReportFailure(Request req, ChannelHandlerContext ctx, IRequestRouter r) {
        // If the server isn't bootstrapped yet, ignore the request
        if(!isBootstrapped(req, ctx, r)) return;

        log.info("handleReportFailure[{}]: Received report failure request {}", req.getHeader().getRequestId(), req);
        Layout layout = serverContext.copyManagementLayout();
        Header responseHeader;
        Response response;

        final Set<String> allActiveServers = layout.getAllActiveServers();
        final Set<String> responsiveFailedNodes = req.getReportFailureRequest()
                .getFailedNodesList()
                .stream()
                .filter(allActiveServers::contains)
                .collect(Collectors.toSet());

        if(!req.getReportFailureRequest().getFailedNodesList().isEmpty() && responsiveFailedNodes.isEmpty()) {
            log.warn("handleReportFailure[{}]: No action taken as none of the failed nodes are responsive. " +
                    "failedNodes:{} responsiveLayoutNodes:{}, latestLayoutEpoch:{}", req.getHeader().getRequestId(),
                    req.getReportFailureRequest().getFailedNodesList(), allActiveServers, layout.getEpoch());

            responseHeader = API.generateResponseHeader(req.getHeader(), false, true);
            response = API.getReportFailureResponse(responseHeader, ReportFailureResponse.Type.ACK);
            r.sendResponse(response, ctx);
            return;
        }

        boolean result = ReconfigurationEventHandler.handleFailure(
                failureHandlerPolicy,
                layout,
                managementAgent.getCorfuRuntime(),
                responsiveFailedNodes);

        if(result) {
            responseHeader = API.generateResponseHeader(req.getHeader(), false, true);
            response = API.getReportFailureResponse(responseHeader, ReportFailureResponse.Type.ACK);
        } else {
            log.error("handleReportFailure[{}]: failure handling unsuccessful.", req.getHeader().getRequestId());
            responseHeader = API.generateResponseHeader(req.getHeader(), false, false);
            response = API.getReportFailureResponse(responseHeader, ReportFailureResponse.Type.NACK);
        }

        r.sendResponse(response, ctx);
    }

    /**
     * Triggers the healing handler.
     * The msg contains the healed nodes.
     *
     * @param msg corfu message containing MANAGEMENT_HEALING_DETECTED
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    @ServerHandler(type = CorfuMsgType.MANAGEMENT_HEALING_DETECTED)
    public void handleHealingDetectedMsg(CorfuPayloadMsg<DetectorMsg> msg,
                                         ChannelHandlerContext ctx, IServerRouter r) {

        // This server has not been bootstrapped yet, ignore all requests.
        if (!isBootstrapped(msg)) {
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.MANAGEMENT_NOBOOTSTRAP_ERROR));
            return;
        }

        log.info("handleHealingDetectedMsg: Received DetectorMsg : {}", msg.getPayload());

        DetectorMsg detectorMsg = msg.getPayload();
        Layout layout = serverContext.copyManagementLayout();

        // If this message is stamped with an older epoch which indicates the polling was
        // conducted in an old epoch. This message cannot be considered and is discarded.
        if (!detectorMsg.getDetectorEpoch().equals(layout.getEpoch())) {
            log.error("handleHealingDetectedMsg: Discarding stale detector message received. "
                            + "detectorEpoch:{} latestLayout epoch:{}",
                    detectorMsg.getDetectorEpoch(), layout.getEpoch());
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.NACK));
            return;
        }

        // Collecting the healed nodes in the message that are amongst the unresponsive nodes in the
        // layout
        final List<String> unresponsiveServers = layout.getUnresponsiveServers();
        final Set<String> unresponsiveHealedNodes = detectorMsg.getHealedNodes()
                .stream()
                .filter(unresponsiveServers::contains)
                .collect(Collectors.toSet());

        // If it is not an out of phase and there is no need to update the layout, return without
        // any reconfiguration
        if (!detectorMsg.getHealedNodes().isEmpty() && unresponsiveHealedNodes.isEmpty()) {
            log.warn("handleHealingDetectedMsg: No action is taken as none of the healedNodes are" +
                            " unresponsive. healedNodes:{}, unresponsive layout nodes: {}, " +
                            "latestLayout epoch:{} ",
                    detectorMsg.getHealedNodes(), unresponsiveServers, layout.getEpoch());
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
            return;
        }

        boolean result = false;
        if (healingLock.tryLock()) {
            try {
                log.info("handleHealingDetectedMsg: acquired healing lock. Performing healing for nodes: {}",
                        unresponsiveHealedNodes);
                final Duration retryWorkflowQueryTimeout = Duration.ofSeconds(1L);
                result = ReconfigurationEventHandler.handleHealing(
                        managementAgent.getCorfuRuntime(),
                        unresponsiveHealedNodes,
                        retryWorkflowQueryTimeout);
            } finally {
                healingLock.unlock();
            }
        } else {
            log.info("handleHealingDetectedMsg: healing handling already in progress. Skipping.");
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.NACK));
        }

        if (result) {
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
        } else {
            log.error("handleHealingDetectedMsg: healing handling unsuccessful.");
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.NACK));
        }
    }

    @RequestHandler(type = CorfuProtocol.MessageType.HEAL_FAILURE)
    public void handleHealFailure(Request req, ChannelHandlerContext ctx, IRequestRouter r) {
        // If the server isn't bootstrapped yet, ignore the request
        if(!isBootstrapped(req, ctx, r)) return;

        log.info("handleHealFailure[{}]: Received heal failure request {}", req.getHeader().getRequestId(), req);
        Layout layout = serverContext.copyManagementLayout();
        Header responseHeader;
        Response response;

        // If this message is stamped with an older epoch which indicates the polling was
        // conducted in an old epoch. This message cannot be considered and is discarded.
        if(req.getHealFailureRequest().getDetectorEpoch() != layout.getEpoch()) {
            log.error("handleHealFailure[{}]: Discarding request received... detectorEpoch:{} latestLayoutEpoch:{}",
                    req.getHeader().getRequestId(), req.getHealFailureRequest().getDetectorEpoch(), layout.getEpoch());

            responseHeader = API.generateResponseHeader(req.getHeader(), false, false);
            response = API.getHealFailureResponse(responseHeader, HealFailureResponse.Type.NACK);
            r.sendResponse(response, ctx);
            return;
        }

        final List<String> unresponsiveServers = layout.getUnresponsiveServers();
        final Set<String> unresponsiveHealedNodes = req.getHealFailureRequest()
                .getHealedNodesList()
                .stream()
                .filter(unresponsiveServers::contains)
                .collect(Collectors.toSet());

        if(!req.getHealFailureRequest().getHealedNodesList().isEmpty() && unresponsiveHealedNodes.isEmpty()) {
            log.warn("handleHealFailure[{}]: No action taken as none of the healed nodes are unresponsive. " +
                    "healedNodes:{} unresponsiveLayoutNodes:{}, latestLayoutEpoch:{}", req.getHeader().getRequestId(),
                    req.getHealFailureRequest().getHealedNodesList(), unresponsiveServers, layout.getEpoch());

            responseHeader = API.generateResponseHeader(req.getHeader(), false, true);
            response = API.getHealFailureResponse(responseHeader, HealFailureResponse.Type.ACK);
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

            responseHeader = API.generateResponseHeader(req.getHeader(), false, false);
            response = API.getHealFailureResponse(responseHeader, HealFailureResponse.Type.NACK);
            r.sendResponse(response, ctx);
            return;
        }

        if(result) {
            responseHeader = API.generateResponseHeader(req.getHeader(), false, true);
            response = API.getHealFailureResponse(responseHeader, HealFailureResponse.Type.ACK);
        } else {
            log.error("handleHealFailure[{}]: healing handling unsuccessful.", req.getHeader().getRequestId());
            responseHeader = API.generateResponseHeader(req.getHeader(), false, false);
            response = API.getHealFailureResponse(responseHeader, HealFailureResponse.Type.NACK);
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
     * @param msg corfu message containing NODE_STATE_REQUEST
     * @param ctx netty ChannelHandlerContext
     * @param r server router
     */
    @ServerHandler(type = CorfuMsgType.NODE_STATE_REQUEST)
    public void handleNodeStateRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        NodeState nodeState = clusterContext.getClusterView()
                .getNode(serverContext.getLocalEndpoint())
                .orElseGet(this::buildDefaultNodeState);

        r.sendResponse(ctx, msg, CorfuMsgType.NODE_STATE_RESPONSE.payloadMsg(nodeState));
    }

    @ServerHandler(type = CorfuMsgType.FAILURE_DETECTOR_METRICS_REQUEST)
    public void handleFailureDetectorMetricsRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        FailureDetectorMetrics metrics = serverContext.getFailureDetectorMetrics();
        r.sendResponse(ctx, msg, CorfuMsgType.FAILURE_DETECTOR_METRICS_RESPONSE.payloadMsg(metrics));
    }

    /**
     * Build default {@link NodeState} nodes state to provide current connectivity status.
     *
     * @return node state
     */
    private NodeState buildDefaultNodeState() {
        log.info("Management server: {}, not ready yet, return default NodeState, current cluster view: {}",
                serverContext.getLocalEndpoint(), clusterContext.getClusterView());

        long epoch = Layout.INVALID_EPOCH;
        Layout layout = serverContext.copyManagementLayout();
        if (layout != null){
            epoch = layout.getEpoch();
        }

        //Node state is connected by default.
        //We believe two servers are connected if another servers is able to send command NODE_STATE_REQUEST
        // and get a response. If we are able to provide NodeState we believe that the state is CONNECTED.
        return NodeState.getNotReadyNodeState(serverContext.getLocalEndpoint());
    }

    /**
     * Handles the Management layout request.
     *
     * @param msg corfu message containing MANAGEMENT_LAYOUT_REQUEST
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    @ServerHandler(type = CorfuMsgType.MANAGEMENT_LAYOUT_REQUEST)
    public void handleLayoutRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        // This server has not been bootstrapped yet, ignore all requests.
        if (!isBootstrapped(msg)) {
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.MANAGEMENT_NOBOOTSTRAP_ERROR));
            return;
        }
        r.sendResponse(ctx, msg,
                CorfuMsgType.LAYOUT_RESPONSE.payloadMsg(serverContext.getManagementLayout()));
    }

    @RequestHandler(type = CorfuProtocol.MessageType.GET_MANAGEMENT_LAYOUT)
    public void handleGetManagementLayout(Request req, ChannelHandlerContext ctx, IRequestRouter r) {
        if(!isBootstrapped(req, ctx, r)) return;

        Header responseHeader = API.generateResponseHeader(req.getHeader(), false, true);
        Response response = API.getGetManagementLayoutResponse(responseHeader, serverContext.getManagementLayout());
        r.sendResponse(response, ctx);
    }
}
