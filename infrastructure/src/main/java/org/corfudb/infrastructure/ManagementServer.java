package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;

import java.lang.invoke.MethodHandles;
import java.util.Map;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.format.Types.NodeMetrics;

import org.corfudb.infrastructure.orchestrator.Orchestrator;

import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorMsg;
import org.corfudb.protocols.wireprotocol.DetectorMsg;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.IReconfigurationHandlerPolicy;
import org.corfudb.runtime.view.Layout;

import javax.annotation.Nonnull;

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

    /**
     * The options map.
     */
    private final Map<String, Object> opts;
    private final ServerContext serverContext;

    private CorfuRuntime corfuRuntime;
    /**
     * Policy to be used to handle failures/healing.
     */
    private IReconfigurationHandlerPolicy failureHandlerPolicy;
    private IReconfigurationHandlerPolicy healingPolicy;
    /**
     * Bootstrap endpoint to seed the Management Server.
     */
    @Getter
    private final String bootstrapEndpoint;

    @Getter
    private final ManagementAgent managementAgent;

    @Getter
    private final String localEndpoint;

    private Orchestrator orchestrator;

    /**
     * Returns new ManagementServer.
     *
     * @param serverContext context object providing parameters and objects
     */
    public ManagementServer(ServerContext serverContext) {

        this.opts = serverContext.getServerConfig();
        this.localEndpoint = this.opts.get("--address") + ":" + this.opts.get("<port>");
        this.serverContext = serverContext;

        bootstrapEndpoint = (opts.get("--management-server") != null)
                ? opts.get("--management-server").toString() : null;

        this.failureHandlerPolicy = serverContext.getFailureHandlerPolicy();
        this.healingPolicy = serverContext.getHealingHandlerPolicy();

        // Creating a management agent.
        this.managementAgent = new ManagementAgent(this::getCorfuRuntime, serverContext);
        this.orchestrator = new Orchestrator(this::getCorfuRuntime, serverContext);
    }

    /**
     * Returns a connected instance of the CorfuRuntime.
     *
     * @return A connected instance of runtime.
     */
    public synchronized CorfuRuntime getCorfuRuntime() {

        if (corfuRuntime == null) {
            CorfuRuntime.CorfuRuntimeParameters params =
                    serverContext.getDefaultRuntimeParameters();
            corfuRuntime = CorfuRuntime.fromParameters(params);
            // Runtime can be set up either using the layout or the bootstrapEndpoint address.
            if (serverContext.getManagementLayout() != null) {
                serverContext
                        .getManagementLayout()
                        .getLayoutServers()
                        .forEach(ls -> corfuRuntime.addLayoutServer(ls));
            } else {
                corfuRuntime.addLayoutServer(getBootstrapEndpoint());
            }
            corfuRuntime.connect();
            log.info("getCorfuRuntime: Corfu Runtime connected successfully");
        }
        return corfuRuntime;
    }

    /**
     * Handler for this server.
     */
    @Getter
    private final CorfuMsgHandler handler =
            CorfuMsgHandler.generateHandler(MethodHandles.lookup(), this);

    private boolean checkBootstrap(CorfuMsg msg,
                                   ChannelHandlerContext ctx,
                                   IServerRouter r) {
        if (serverContext.getManagementLayout() == null && bootstrapEndpoint == null) {
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
        if (serverContext.getManagementLayout() != null) {
            // We are already bootstrapped, bootstrap again is not allowed.
            log.warn("Got a request to bootstrap a server which is already bootstrapped, "
                    + "rejecting!");
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.MANAGEMENT_ALREADY_BOOTSTRAP_ERROR));
        } else {
            log.info("Received Bootstrap Layout : {}", msg.getPayload());
            serverContext.saveManagementLayout(msg.getPayload());
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));

        }
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
    public synchronized void handleFailureDetectedMsg(CorfuPayloadMsg<DetectorMsg> msg,
                                                      ChannelHandlerContext ctx, IServerRouter r) {

        // This server has not been bootstrapped yet, ignore all requests.
        if (!checkBootstrap(msg, ctx, r)) {
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.MANAGEMENT_NOBOOTSTRAP_ERROR));
            return;
        }

        log.info("handleFailureDetectedMsg: Received DetectorMsg : {}", msg.getPayload());

        DetectorMsg detectorMsg = msg.getPayload();
        Layout layout = new Layout(serverContext.getManagementLayout());

        // If this message is stamped with an older epoch which indicates the polling was
        // conducted in an old epoch. This message cannot be considered and is discarded.
        if (!detectorMsg.getDetectorEpoch().equals(layout.getEpoch())) {
            log.error("handleFailureDetectedMsg: Discarding stale detector message received. "
                            + "detectorEpoch:{} latestLayout epoch:{}",
                    detectorMsg.getDetectorEpoch(), layout.getEpoch());
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.NACK));
            return;
        }

        boolean result = managementAgent.getReconfigurationEventHandler().handleFailure(
                failureHandlerPolicy,
                layout,
                managementAgent.getCorfuRuntime(),
                detectorMsg.getFailedNodes());

        if (result) {
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
        } else {
            log.error("handleFailureDetectedMsg: failure handling unsuccessful.");
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.NACK));
        }
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
    public synchronized void handleHealingDetectedMsg(CorfuPayloadMsg<DetectorMsg> msg,
                                                      ChannelHandlerContext ctx, IServerRouter r) {

        // This server has not been bootstrapped yet, ignore all requests.
        if (!checkBootstrap(msg, ctx, r)) {
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.MANAGEMENT_NOBOOTSTRAP_ERROR));
            return;
        }

        log.info("handleHealingDetectedMsg: Received DetectorMsg : {}", msg.getPayload());

        DetectorMsg detectorMsg = msg.getPayload();
        Layout layout = new Layout(serverContext.getManagementLayout());

        // If this message is stamped with an older epoch which indicates the polling was
        // conducted in an old epoch. This message cannot be considered and is discarded.
        if (!detectorMsg.getDetectorEpoch().equals(layout.getEpoch())) {
            log.error("handleHealingDetectedMsg: Discarding stale detector message received. "
                            + "detectorEpoch:{} latestLayout epoch:{}",
                    detectorMsg.getDetectorEpoch(), layout.getEpoch());
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.NACK));
            return;
        }

        boolean result = managementAgent.getReconfigurationEventHandler().handleHealing(
                healingPolicy,
                layout,
                managementAgent.getCorfuRuntime(),
                detectorMsg.getHealedNodes());

        if (result) {
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsgType.ACK));
        } else {
            log.error("handleHealingDetectedMsg: healing handling unsuccessful.");
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
     * Management Server shutdown:
     * Shuts down the fault detector service.
     */
    public void shutdown() {
        super.shutdown();
        managementAgent.shutdown();

        // Shut down the Corfu Runtime.
        if (corfuRuntime != null) {
            corfuRuntime.shutdown();
        }
    }
}
