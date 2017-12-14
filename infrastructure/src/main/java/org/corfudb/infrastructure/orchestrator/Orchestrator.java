package org.corfudb.infrastructure.orchestrator;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.format.Types;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.orchestrator.AddNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.CreateWorkflowResponse;
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
 * Created by Maithem on 10/25/17.
 */

@Slf4j
public class Orchestrator {

    final Callable<CorfuRuntime> getRuntime;
    final Map<UUID, String> activeWorkflows = new ConcurrentHashMap();

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
        if (activeWorkflows.containsKey(req.getId())) {
            resp = new QueryResponse(true);
            log.trace("handleQuery: returning active for id {}", req.getId());
        } else {
            resp = new QueryResponse(false);
            log.trace("handleQuery: returning not active for id {}", req.getId());
        }

        r.sendResponse(ctx, msg, CorfuMsgType.ORCHESTRATOR_RESPONSE
                .payloadMsg(new OrchestratorResponse(resp)));
    }

    void addNode(CorfuPayloadMsg<OrchestratorRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        AddNodeRequest req = (AddNodeRequest) msg.getPayload().getRequest();
        if (findUUID(req.getEndpoint()) != null) {
            // An add node workflow is already executing for this endpoint, return
            // existing workflow id.
            OrchestratorResponse resp = new OrchestratorResponse(
                    new CreateWorkflowResponse(findUUID(req.getEndpoint())));
            r.sendResponse(ctx, msg, CorfuMsgType.ORCHESTRATOR_RESPONSE
                    .payloadMsg(resp));
            log.trace("addNode: ignoring req for {}", findUUID(req.getEndpoint()));
            return;
        } else {
            // Create a new workflow for this endpoint and return a new workflow id
            Workflow workflow = getWorkflow(msg.getPayload());
            activeWorkflows.put(workflow.getId(), req.getEndpoint());
            log.trace("addNode: putting id  {}", workflow.getId());
            OrchestratorResponse resp = new OrchestratorResponse(new CreateWorkflowResponse(workflow.getId()));
            r.sendResponse(ctx, msg, CorfuMsgType.ORCHESTRATOR_RESPONSE
                    .payloadMsg(resp));
            CompletableFuture.runAsync(() -> {
                run(workflow);
            });
        }
    }

    /**
     * Create a workflow instance from an orchestrator request
     *
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
     *
     * @param workflow instance to run
     */
    void run(@Nonnull Workflow workflow) {
        CorfuRuntime rt = null;

        try {
            Layout currLayout = getRuntime.call().layout.get();
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
            activeWorkflows.remove(workflow.getId());
            log.debug("run: removed {} from {}",workflow.getId(), activeWorkflows);
            if (rt != null) {
                rt.shutdown();
            }
        }
    }

    UUID findUUID(String endpoint) {
        for (Map.Entry<UUID, String> entry : activeWorkflows.entrySet()) {
            if (entry.getValue().equals(endpoint)) {
                return entry.getKey();
            }
        }
        return null;
    }
}
