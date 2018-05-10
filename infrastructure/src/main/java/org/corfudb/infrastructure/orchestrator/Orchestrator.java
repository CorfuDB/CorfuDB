package org.corfudb.infrastructure.orchestrator;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.orchestrator.workflows.AddNodeWorkflow;
import org.corfudb.infrastructure.orchestrator.workflows.HealNodeWorkflow;
import org.corfudb.infrastructure.orchestrator.workflows.RemoveNodeWorkflow;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.orchestrator.AddNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.CreateRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.CreateWorkflowResponse;
import org.corfudb.protocols.wireprotocol.orchestrator.ForceRemoveNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.HealNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorMsg;
import org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorResponse;
import org.corfudb.protocols.wireprotocol.orchestrator.QueryRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.QueryResponse;
import org.corfudb.protocols.wireprotocol.orchestrator.RemoveNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.Response;
import org.corfudb.infrastructure.orchestrator.workflows.ForceRemoveWorkflow;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.concurrent.SingletonResource;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * The orchestrator is a stateless service that runs on all management servers and its purpose
 * is to execute workflows. A workflow defines multiple smaller actions that must run in order
 * that is specified by Workflow.getActions() to achieve a bigger goal. For example, growing the
 * cluster. Initiated through RPC, the orchestrator will create a workflow instance and attempt
 * to execute all its actions.
 * <p>
 * Created by Maithem on 10/25/17.
 */

@Slf4j
public class Orchestrator {

    /**
     * The number of times to retry an action before failing the workflow.
     */
    private static final int ACTION_RETRY = 3;

    final ServerContext serverContext;

    final SingletonResource<CorfuRuntime> getRuntime;

    final BiMap<UUID, String> activeWorkflows = Maps.synchronizedBiMap(HashBiMap.create());

    final ExecutorService executor;

    public Orchestrator(@Nonnull SingletonResource<CorfuRuntime> runtime,
                        @Nonnull ServerContext serverContext) {
        this.serverContext = serverContext;
        this.getRuntime = runtime;

        executor = Executors.newFixedThreadPool(Runtime.getRuntime()
                .availableProcessors(), new ThreadFactory() {

            final AtomicInteger threadNumber = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                String threadName = serverContext.getThreadPrefix() + "orchestrator-"
                        + threadNumber.getAndIncrement();
                thread.setName(threadName);
                thread.setUncaughtExceptionHandler(this::handleUncaughtException);
                return new Thread(r);
            }

            void handleUncaughtException(Thread t, @Nonnull Throwable e) {
                log.error("handleUncaughtException[{}]: Uncaught {}:{}",
                        t.getName(),
                        e.getClass().getSimpleName(),
                        e.getMessage(),
                        e);
            }
        });
    }

    public void handle(@Nonnull CorfuPayloadMsg<OrchestratorMsg> msg,
                       @Nonnull ChannelHandlerContext ctx,
                       @Nonnull IServerRouter r) {

        OrchestratorMsg orchReq = msg.getPayload();
        IWorkflow workflow;
        switch (orchReq.getRequest().getType()) {
            case QUERY:
                query(msg, ctx, r);
                break;
            case ADD_NODE:
                workflow = new AddNodeWorkflow((AddNodeRequest) orchReq.getRequest());
                dispatch(workflow, msg, ctx, r);
                break;
            case REMOVE_NODE:
                workflow = new RemoveNodeWorkflow((RemoveNodeRequest) orchReq.getRequest());
                dispatch(workflow, msg, ctx, r);
                break;
            case HEAL_NODE:
                workflow = new HealNodeWorkflow((HealNodeRequest) orchReq.getRequest());
                dispatch(workflow, msg, ctx, r);
                break;
            case FORCE_REMOVE_NODE:
                workflow = new ForceRemoveWorkflow((ForceRemoveNodeRequest) orchReq.getRequest());
                dispatch(workflow, msg, ctx, r);
                break;
            default:
                log.error("handle: Unknown request type {}", orchReq.getRequest().getType());
        }
    }

    /**
     * Query a workflow id.
     * <p>
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

    /**
     * Run a workflow on this orchestrator, if there is an existing workflow
     * that is executing on the same endpoint, then just return the corresponding
     * workflow id. Dispatch is the only place where workflows are executed based
     * on reading activeWorkflows and therefore needs to be synchronized to prevent
     * launching multiple workflows for the same endpoint concurrently.
     *
     * @param workflow the workflow to execute
     * @param msg      corfu message containing the create workflow request
     * @param ctx      netty ChannelHandlerContext
     * @param r        server router
     */
    synchronized void dispatch(@Nonnull IWorkflow workflow,
                               @Nonnull CorfuPayloadMsg<OrchestratorMsg> msg,
                               @Nonnull ChannelHandlerContext ctx,
                               @Nonnull IServerRouter r) {
        CreateRequest req = (CreateRequest) msg.getPayload().getRequest();

        UUID id = activeWorkflows.inverse().get(req.getEndpoint());
        if (id != null) {
            // A workflow is already executing for this endpoint, return
            // existing workflow id.
            OrchestratorResponse resp = new OrchestratorResponse(
                    new CreateWorkflowResponse(id));
            r.sendResponse(ctx, msg, CorfuMsgType.ORCHESTRATOR_RESPONSE
                    .payloadMsg(resp));
            return;
        } else {
            // Create a new workflow for this endpoint and return a new workflow id
            activeWorkflows.put(workflow.getId(), req.getEndpoint());

            executor.execute(() -> run(workflow, ACTION_RETRY));

            OrchestratorResponse resp = new OrchestratorResponse(new CreateWorkflowResponse(workflow.getId()));
            r.sendResponse(ctx, msg, CorfuMsgType.ORCHESTRATOR_RESPONSE
                    .payloadMsg(resp));
        }
    }

    /**
     * Run a particular workflow, which entails executing all its defined
     * actions
     *
     * @param workflow    instance to run
     * @param actionRetry the number of times to retry an action before failing the workflow.
     */
    void run(@Nonnull IWorkflow workflow, int actionRetry) {
        CorfuRuntime rt = null;
        try {
            getRuntime.get().invalidateLayout();
            Layout currLayout = getRuntime.get().getLayoutView().getLayout();

            List<NodeLocator> servers = currLayout.getAllActiveServers().stream()
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

            log.info("run: Started workflow {} id {}", workflow.getName(), workflow.getId());
            long workflowStart = System.currentTimeMillis();
            for (Action action : workflow.getActions()) {

                log.debug("run: Started action {} for workflow {}", action.getName(), workflow.getId());
                long actionStart = System.currentTimeMillis();
                action.execute(rt, actionRetry);
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
        } finally {
            activeWorkflows.remove(workflow.getId());
            log.debug("run: removed {} from {}", workflow.getId(), activeWorkflows);
            if (rt != null) {
                rt.shutdown();
            }
        }
    }

    /**
     * Shuts down the orchestrator executor.
     */
    public void shutdown() {
        executor.shutdownNow();
        try {
            executor.awaitTermination(ServerContext.SHUTDOWN_TIMER.getSeconds(), TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            log.debug("Orchestrator executor awaitTermination interrupted : {}", ie);
            Thread.currentThread().interrupt();
        }
        log.info("Orchestrator shutting down.");
    }
}
