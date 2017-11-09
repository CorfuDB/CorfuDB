package org.corfudb.infrastructure.orchestrator;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.format.Types;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.orchestrator.AddNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.AddNodeResponse;
import org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorResponse;
import org.corfudb.protocols.wireprotocol.orchestrator.QueryRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.QueryResponse;
import org.corfudb.protocols.wireprotocol.orchestrator.Request;
import org.corfudb.protocols.wireprotocol.orchestrator.Response;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.corfudb.format.Types.OrchestratorRequestType.QUERY;

/**
 * The orchestrator is a stateless service that runs on all management servers and its purpose
 * is to execute workflows. A workflow defines multiple smaller actions that must run in order
 * that is specified by Workflow.getActions() to achieve a bigger goal. For example, growing the
 * cluster. Initiated through RPC, the orchestrator will create a workflow instance and attempt
 * to execute all its actions.
 *
 * <p>
 * Created by Maithem on 10/25/17.
 */

@Slf4j
public class Orchestrator {

    final Callable<CorfuRuntime> getRuntime;
    final Map<String, UUID> activeWorkflows = new ConcurrentHashMap();

    public Orchestrator(@Nonnull Callable<CorfuRuntime> runtime) {
        this.getRuntime = runtime;
    }

    public void handle(@Nonnull CorfuPayloadMsg<OrchestratorRequest> msg,
                       @Nonnull ChannelHandlerContext ctx,
                       @Nonnull IServerRouter r) {

        OrchestratorRequest orchReq = msg.getPayload();

        if (orchReq.getRequest().getType() == QUERY) {
            handleQuery(msg, ctx, r);
        } else {
            addNode(msg, ctx, r);
        }
    }

    void handleQuery(CorfuPayloadMsg<OrchestratorRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        QueryRequest req = (QueryRequest) msg.getPayload().getRequest();

        Response resp;
        if (activeWorkflows.values().contains(req.getId())) {
            resp = new QueryResponse(true);
        } else {
            resp = new QueryResponse(false);
        }

        r.sendResponse(ctx, msg, CorfuMsgType.ORCHESTRATOR_RESPONSE
                .payloadMsg(new OrchestratorResponse(resp )));
    }

    void addNode(CorfuPayloadMsg<OrchestratorRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        CompletableFuture.runAsync(() -> {
            AddNodeRequest req = (AddNodeRequest) msg.getPayload().getRequest();
            if (activeWorkflows.containsKey(req.getEndpoint())) {
                // An add node workflow is already executing for this endpoint, return
                // existing workflow id.
                OrchestratorResponse resp = new OrchestratorResponse(
                        new AddNodeResponse(activeWorkflows.get(req.getEndpoint())));
                r.sendResponse(ctx, msg, CorfuMsgType.ORCHESTRATOR_RESPONSE
                        .payloadMsg(resp));
                return;
            } else {
                // Create a new workflow for this endpoint and return a new workflow id
                Workflow workflow = getWorkflow(msg.getPayload());
                activeWorkflows.put(req.getEndpoint(), workflow.getId());
                OrchestratorResponse resp = new OrchestratorResponse(new AddNodeResponse(workflow.getId()));
                r.sendResponse(ctx, msg, CorfuMsgType.ORCHESTRATOR_RESPONSE
                        .payloadMsg(resp));
                run(workflow);
            }
        });
    }

    /**
     * Create a workflow instance from an orchestrator request
     * @param req Orchestrator request
     * @return Workflow instance
     */
    @Nonnull
    private Workflow getWorkflow(@Nonnull OrchestratorRequest req) {
        Request payload = req.getRequest();
        if (payload.getType().equals(Types.OrchestratorRequestType.ADD_NODE)) {
            return new AddNodeWorkflow(payload);
        }

        throw new IllegalArgumentException("Unknown request");
    }

    /**
     * Run a particular workflow, which entails executing all its defined
     * actions
     * @param workflow instance to run
     */
    void run(@Nonnull Workflow workflow) {
        CorfuRuntime rt = null;

        try {
            Layout currLayout =  getRuntime.call().layout.get();
            String servers = String.join(",", currLayout.getLayoutServers());
            rt = new CorfuRuntime(servers)
                    .setCacheDisabled(true)
                    .setLoadSmrMapsAtConnect(false)
                    .connect();

            log.info("run: Started workflow {} id {}", workflow.getName(), workflow.getId());
            long workflowStart = System.currentTimeMillis();
            for (Action action : workflow.getActions()) {

                log.debug("run: Started action {} for workflow {}", action.getName(), workflow.getId());
                long actionStart = System.currentTimeMillis();
                action.execute(rt);
                long actionEnd = System.currentTimeMillis();
                log.info("run: finished action {} for workflow {} in {} ms",
                        action.getName(), workflow.getId(), actionEnd - actionStart);

                if (action.getStatus() != ActionStatus.COMPLETED) {
                    log.error("run: Failed to execute action {} for workflow {}, status {}, ",
                            action.getName(), workflow.getId(),
                            action.getStatus());
                    return;
                }
            }

            long workflowEnd = System.currentTimeMillis();
            log.info("run: Completed workflow {} in {} ms", workflow.getId(), workflowEnd - workflowStart);
        } catch (Exception e) {
            log.error("run: Encountered an error while running workflow {}", workflow.getId(), e);
            return;
        } finally {
            if (rt != null) {
                rt.shutdown();
            }

            activeWorkflows.remove(workflow.getId());
        }
    }
}
