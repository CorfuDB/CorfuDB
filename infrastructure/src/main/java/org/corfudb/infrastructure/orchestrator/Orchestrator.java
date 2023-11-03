package org.corfudb.infrastructure.orchestrator;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.health.HealthMonitor;
import org.corfudb.infrastructure.health.Issue;
import org.corfudb.infrastructure.orchestrator.workflows.AddNodeWorkflow;
import org.corfudb.infrastructure.orchestrator.workflows.ForceRemoveWorkflow;
import org.corfudb.infrastructure.orchestrator.workflows.HealNodeWorkflow;
import org.corfudb.infrastructure.orchestrator.workflows.RemoveNodeWorkflow;
import org.corfudb.infrastructure.orchestrator.workflows.RestoreRedundancyMergeSegmentsWorkflow;
import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.protocols.wireprotocol.orchestrator.AddNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.ForceRemoveNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.HealNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.RemoveNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.RestoreRedundancyMergeSegmentsRequest;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.Management.OrchestratorRequestMsg;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.concurrent.SingletonResource;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.health.Component.ORCHESTRATOR;
import static org.corfudb.infrastructure.health.Issue.IssueId.ORCHESTRATOR_TASK_FAILED;
import static org.corfudb.protocols.CorfuProtocolCommon.getUUID;
import static org.corfudb.protocols.CorfuProtocolWorkflows.getAddNodeRequest;
import static org.corfudb.protocols.CorfuProtocolWorkflows.getForceRemoveNodeRequest;
import static org.corfudb.protocols.CorfuProtocolWorkflows.getHealNodeRequest;
import static org.corfudb.protocols.CorfuProtocolWorkflows.getRemoveNodeRequest;
import static org.corfudb.protocols.CorfuProtocolWorkflows.getRestoreRedundancyMergeSegmentsRequest;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getCreatedWorkflowResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolManagement.getQueriedWorkflowResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;


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

    final WorkflowFactory workflowFactory;

    public Orchestrator(@Nonnull SingletonResource<CorfuRuntime> runtime,
                        @Nonnull ServerContext serverContext,
                        @Nonnull WorkflowFactory workflowFactory) {
        this.serverContext = serverContext;
        this.workflowFactory = workflowFactory;
        getRuntime = runtime;

        executor = serverContext.getExecutorService(Runtime.getRuntime().availableProcessors(),
                new ThreadFactory() {

                    final AtomicInteger threadNumber = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setDaemon(true);
                        String threadName = serverContext.getThreadPrefix() + "orchestrator-"
                                + threadNumber.getAndIncrement();
                        thread.setName(threadName);
                        thread.setUncaughtExceptionHandler(this::handleUncaughtException);
                        return thread;
                    }

                    void handleUncaughtException(Thread t, @Nonnull Throwable e) {
                        log.error("handleUncaughtException[{}]: Uncaught {}:{}",
                                t.getName(),
                                e.getClass().getSimpleName(),
                                e.getMessage(),
                                e);
                    }
                });
        HealthMonitor.resolveIssue(Issue.createInitIssue(ORCHESTRATOR));
    }

    public void handle(@Nonnull RequestMsg req, @Nonnull ChannelHandlerContext ctx, @Nonnull IServerRouter r) {
        OrchestratorRequestMsg msg = req.getPayload().getOrchestratorRequest();
        IWorkflow workflow;

        switch (msg.getPayloadCase()) {
            case QUERY:
                handleQuery(req, ctx, r);
                break;

            case ADD_NODE:
                workflow = workflowFactory.getAddNode(getAddNodeRequest(msg.getAddNode()));
                dispatch(workflow, req, ctx, r, msg.getAddNode().getEndpoint());
                break;

            case REMOVE_NODE:
                workflow = workflowFactory.getRemoveNode(getRemoveNodeRequest(msg.getRemoveNode()));
                dispatch(workflow, req, ctx, r, msg.getRemoveNode().getEndpoint());
                break;

            case FORCE_REMOVE_NODE:
                workflow = workflowFactory.getForceRemove(getForceRemoveNodeRequest(msg.getForceRemoveNode()));
                dispatch(workflow, req, ctx, r, msg.getForceRemoveNode().getEndpoint());
                break;

            case HEAL_NODE:
                workflow = workflowFactory.getHealNode(getHealNodeRequest(msg.getHealNode()), serverContext);
                dispatch(workflow, req, ctx, r, msg.getHealNode().getEndpoint());
                break;

            case RESTORE_REDUNDANCY_MERGE_SEGMENTS:
                workflow = workflowFactory.getRestoreRedundancy(
                        getRestoreRedundancyMergeSegmentsRequest(msg.getRestoreRedundancyMergeSegments())
                );
                dispatch(workflow, req, ctx, r, msg.getRestoreRedundancyMergeSegments().getEndpoint());
                break;

            default:
                log.error("handle[{}]: Unknown orchestrator request type {}",
                        req.getHeader().getRequestId(), msg.getPayloadCase());
                break;
        }
    }

    /**
     * Query a workflow id.
     * <p>
     * Queries a workflow id and returns true if this orchestrator is still
     * executing the workflow, otherwise return false.
     *
     * @param req a message containing the query request
     * @param ctx the netty ChannelHandlerContext
     * @param r   the server router
     */
    void handleQuery(@Nonnull RequestMsg req, @Nonnull ChannelHandlerContext ctx, @Nonnull IServerRouter r) {
        final UUID workflowId = getUUID(req.getPayload().getOrchestratorRequest().getQuery().getWorkflowId());
        boolean isActive = false;

        if (activeWorkflows.containsKey(workflowId)) {
            isActive = true;
        }

        log.trace("handleQuery[{}]: isActive={} for workflowId={}",
                req.getHeader().getRequestId(), isActive, workflowId);

        HeaderMsg responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
        ResponseMsg response = getResponseMsg(responseHeader, getQueriedWorkflowResponseMsg(isActive));
        r.sendResponse(response, ctx);
    }

    /**
     * Run a workflow on this orchestrator, if there is an existing workflow
     * that is executing on the same endpoint, then just return the corresponding
     * workflow id. Dispatch is the only place where workflows are executed based
     * on reading activeWorkflows and therefore needs to be synchronized to prevent
     * launching multiple workflows for the same endpoint concurrently.
     *
     * @param workflow the workflow to execute
     * @param req      request message containing the create workflow request
     * @param ctx      netty ChannelHandlerContext
     * @param r        server router
     * @param endpoint the endpoint parameter from the workflow request
     */
    synchronized void dispatch(@Nonnull IWorkflow workflow,
                               @Nonnull RequestMsg req,
                               @Nonnull ChannelHandlerContext ctx,
                               @Nonnull IServerRouter r,
                               @Nonnull String endpoint) {
        final UUID id = activeWorkflows.inverse().get(endpoint);

        HeaderMsg responseHeader = getHeaderMsg(req.getHeader(), ClusterIdCheck.CHECK, EpochCheck.IGNORE);
        ResponseMsg response;

        if (id != null) {
            // A workflow is already executing for this endpoint, return existing workflow id.
            response = getResponseMsg(responseHeader, getCreatedWorkflowResponseMsg(id));
        } else {
            // Create a new workflow for this endpoint and return a new workflow id.
            activeWorkflows.put(workflow.getId(), endpoint);
            executor.execute(() -> run(workflow, ACTION_RETRY));
            response = getResponseMsg(responseHeader, getCreatedWorkflowResponseMsg(workflow.getId()));
        }

        r.sendResponse(response, ctx);
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
            CorfuRuntimeParameters params = serverContext.getManagementRuntimeParameters();
            params.setCacheDisabled(true);
            params.setLayoutServers(servers);

            rt = CorfuRuntime.fromParameters(params).connect();

            log.info("run: Started workflow {} id {}", workflow.getName(), workflow.getId());
            long workflowStart = System.currentTimeMillis();
            for (Action action : workflow.getActions()) {

                log.info("run: Started action {} for workflow {}", action.getName(), workflow.getId());
                long actionStart = System.currentTimeMillis();
                action.execute(rt, actionRetry);
                long actionEnd = System.currentTimeMillis();
                log.info("run: finished action {} for workflow {} in {} ms",
                        action.getName(), workflow.getId(), actionEnd - actionStart);

                if (action.getStatus() != ActionStatus.COMPLETED) {
                    log.error("run: Failed to execute action {} for workflow {}, status {}, ",
                            action.getName(), workflow.getId(),
                            action.getStatus());
                    String reportMsg = String.format("Failed to execute action %s for workflow %s",
                            action.getName(), workflow.getName());
                    HealthMonitor.reportIssue(Issue.createIssue(ORCHESTRATOR,
                            ORCHESTRATOR_TASK_FAILED, reportMsg));
                    return;
                }
            }

            long workflowEnd = System.currentTimeMillis();
            log.info("run: Completed workflow {} in {} ms", workflow.getId(), workflowEnd - workflowStart);
            HealthMonitor.resolveIssue(Issue.createIssue(ORCHESTRATOR,
                    ORCHESTRATOR_TASK_FAILED, "Last workflow completed successfully"));
        } catch (Exception e) {
            log.error("run: Encountered an error while running workflow {}", workflow.getId(), e);
            String reportMsg = String.format("Encountered an error while running a workflow %s:%n%s",
                    workflow.getName(), e.getLocalizedMessage());
            HealthMonitor.reportIssue(Issue.createIssue(ORCHESTRATOR,
                    ORCHESTRATOR_TASK_FAILED, reportMsg));
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
            log.debug("Orchestrator executor awaitTermination interrupted.", ie);
            throw new UnrecoverableCorfuInterruptedError(ie);
        }
        log.info("Orchestrator shutting down.");
        HealthMonitor.reportIssue(Issue.createInitIssue(ORCHESTRATOR));
    }

    /**
     * Factory class used by the Orchestrator to create workflows from
     * their corresponding requests.
     */
    public static class WorkflowFactory {
        AddNodeWorkflow getAddNode(@Nonnull AddNodeRequest req) {
            return new AddNodeWorkflow(req);
        }

        RemoveNodeWorkflow getRemoveNode(@Nonnull RemoveNodeRequest req) {
            return new RemoveNodeWorkflow(req);
        }

        ForceRemoveWorkflow getForceRemove(@Nonnull ForceRemoveNodeRequest req) {
            return new ForceRemoveWorkflow(req);
        }

        HealNodeWorkflow getHealNode(@Nonnull HealNodeRequest req, ServerContext serverContext) {
            return new HealNodeWorkflow(req, serverContext);
        }

        RestoreRedundancyMergeSegmentsWorkflow getRestoreRedundancy(@Nonnull RestoreRedundancyMergeSegmentsRequest req) {
            return new RestoreRedundancyMergeSegmentsWorkflow(req);
        }
    }
}
