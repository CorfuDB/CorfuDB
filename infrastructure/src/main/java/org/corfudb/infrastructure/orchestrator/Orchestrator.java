package org.corfudb.infrastructure.orchestrator;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.orchestrator.Action;
import org.corfudb.protocols.wireprotocol.orchestrator.ActionStatus;
import org.corfudb.protocols.wireprotocol.orchestrator.CreateRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.IWorkflow;
import org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorMsg;
import org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorResponse;
import org.corfudb.protocols.wireprotocol.orchestrator.QueryRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.Response;
import org.corfudb.protocols.wireprotocol.orchestrator.WorkflowResult;
import org.corfudb.protocols.wireprotocol.orchestrator.WorkflowStatus;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.NodeLocator;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static org.corfudb.protocols.wireprotocol.orchestrator.WorkflowResult.BUSY;
import static org.corfudb.protocols.wireprotocol.orchestrator.WorkflowResult.COMPLETED;
import static org.corfudb.protocols.wireprotocol.orchestrator.WorkflowResult.ERROR;
import static org.corfudb.protocols.wireprotocol.orchestrator.WorkflowResult.UNKNOWN;

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

    final ServerContext serverContext;

    final Callable<CorfuRuntime> getRuntime;

    final Set<String> activeEndpoints = Collections.synchronizedSet(new HashSet<>());

    public Orchestrator(@Nonnull Callable<CorfuRuntime> runtime,
                        @Nonnull ServerContext serverContext) {
        this.serverContext = serverContext;
        this.getRuntime = runtime;
    }

    public void handle(@Nonnull CorfuPayloadMsg<OrchestratorMsg> msg,
                       @Nonnull ChannelHandlerContext ctx,
                       @Nonnull IServerRouter r) {

        OrchestratorMsg orchReq = msg.getPayload();

        switch (orchReq.getRequest().getType()) {
            case QUERY:
                query(msg, ctx, r);
                break;
            case ADD_NODE:
                dispatch(msg, ctx, r);
                break;
            default:
                log.error("handle: Unknown request type {}", orchReq.getRequest().getType());
        }
    }

    /**
     *
     * Query a workflow id.
     *
     * Queries a workflow id and returns true if this orchestrator is still
     * executing the workflow, otherwise return false.
     *
     * @param msg corfu message containing the query request
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    void query(CorfuPayloadMsg<OrchestratorMsg> msg, ChannelHandlerContext ctx, IServerRouter r) {
        QueryRequest req = (QueryRequest) msg.getPayload().getRequest();

        Response resp;
        if (activeEndpoints.contains(req.getEndpoint())) {
            resp = new WorkflowStatus(BUSY);
            log.trace("handleQuery: returning active for id {}", req.getEndpoint());
        } else {
            resp = new WorkflowStatus(UNKNOWN);
            log.trace("handleQuery: returning not active for id {}", req.getEndpoint());
        }

        r.sendResponse(ctx, msg, CorfuMsgType.ORCHESTRATOR_RESPONSE
                .payloadMsg(new OrchestratorResponse(resp)));
    }

    /**
     *
     * Dispatch a workflow create request.
     *
     * Create and start a workflow on this orchestrator, if there is
     * an existing workflow that is executing on the same endpoint,
     * the orchestrator will notify the client that endpoint is busy.
     * Dispatch is the only place where workflows are created based on
     * reading activeWorkflows and therefore access needs to be synchronized
     * to prevent launching multiple workflows for the same endpoint concurrently.
     *
     * @param msg corfu message containing the create workflow request
     * @param ctx netty ChannelHandlerContext
     * @param r   server router
     */
    void dispatch(CorfuPayloadMsg<OrchestratorMsg> msg,
                  ChannelHandlerContext ctx, IServerRouter r) {
        CreateRequest req = (CreateRequest) msg.getPayload().getRequest();

        synchronized (activeEndpoints) {
            if (activeEndpoints.contains(req.getEndpoint())) {
                // A workflow is already executing for this endpoint, return
                // BUSY status
                OrchestratorResponse resp = new OrchestratorResponse(
                        new WorkflowStatus(WorkflowResult.BUSY));
                r.sendResponse(ctx, msg, CorfuMsgType.ORCHESTRATOR_RESPONSE
                        .payloadMsg(resp));
                return;
            } else {
                activeEndpoints.add(req.getEndpoint());
            }
        }

        WorkflowStatus res;
        // Create a new workflow for this endpoint and return a new workflow id
        IWorkflow workflow = req.getWorkflow();
        log.debug("dispatch: running workflow for request {}", req);
        run(workflow);
        if (workflow.completed()) {
            res = new WorkflowStatus(COMPLETED);
            log.debug("dispatch: workflow completed");
        } else {
            log.debug("dispatch: workflow error");
            res = new WorkflowStatus(ERROR);
        }

        activeEndpoints.remove(req.getEndpoint());

        OrchestratorResponse resp = new OrchestratorResponse(res);
        r.sendResponse(ctx, msg, CorfuMsgType.ORCHESTRATOR_RESPONSE
                .payloadMsg(resp));
        log.debug("dispatch: sent response {}", res);
    }

    /**
     * Run a particular workflow, which entails executing all its defined
     * actions
     *
     * @param workflow instance to run
     */
    void run(@Nonnull IWorkflow workflow) {
        CorfuRuntime rt = null;
        try {
            getRuntime.call().invalidateLayout();
            Layout currLayout = getRuntime.call().getLayoutView().getLayout();
            List<NodeLocator> servers = currLayout.getAllServers().stream()
                    .map(NodeLocator::parseString)
                    .collect(Collectors.toList());

            // Create a new runtime for this workflow. Since this runtime will
            // only be used to execute this workflow, it doesn't need a cache and has
            // the default authentication config parameters as the ManagementServer's
            // runtime
            CorfuRuntimeParameters params = serverContext.getDefaultRuntimeParameters();
            params.setCacheDisabled(true);
            params.setUseFastLoader(false);
            params.setLayoutServers(servers);

            rt = CorfuRuntime.fromParameters(params).connect();

            log.info("run: Started workflow {} id {} for request {}", workflow.getName(),
                    workflow.getId(), workflow);
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
        }
    }
}
