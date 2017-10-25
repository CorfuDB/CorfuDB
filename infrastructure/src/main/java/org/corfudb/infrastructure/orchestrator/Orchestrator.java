package org.corfudb.infrastructure.orchestrator;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorRequest;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

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

    public Orchestrator(@Nonnull Callable<CorfuRuntime> runtime) {
        this.getRuntime = runtime;
    }

    public void handle(@Nonnull CorfuPayloadMsg<OrchestratorRequest> msg,
                       @Nonnull ChannelHandlerContext ctx,
                       @Nonnull IServerRouter r) {

        OrchestratorRequest req = msg.getPayload();
        dispatch(req);
        r.sendResponse(ctx, msg, CorfuMsgType.ORCHESTRATOR_RESPONSE.msg());
    }

    void dispatch(@Nonnull OrchestratorRequest req) {
        CompletableFuture.runAsync(() -> {
            run(getWorkflow(req));
        });
    }

    /**
     * Create a workflow instance from an orchestrator request
     * @param req Orchestrator request
     * @return Workflow instance
     */
    @Nonnull
    private Workflow getWorkflow(@Nonnull OrchestratorRequest req) {
        return null;
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
            String servers = String.join(",", currLayout.getAllServers());
            rt = new CorfuRuntime(servers)
                    .setCacheDisabled(true)
                    .setLoadSmrMapsAtConnect(false)
                    .connect();

            log.info("run: Started workflow {} id {}", workflow.getName(), workflow.getId());
            long workflowStart = System.currentTimeMillis();

            for (Action action : workflow.getActions()) {
                try {
                    log.debug("run: Started action {} for workflow {}", action.getName(), workflow.getId());
                    long actionStart = System.currentTimeMillis();
                    action.execute(rt);
                    long actionEnd = System.currentTimeMillis();
                    log.debug("run: finished action {} for workflow {} in {} ms",
                            action.getName(), workflow.getId(), actionEnd - actionStart);

                    if (action.getStatus() == ActionStatus.COMPLETED) {
                        throw new IllegalStateException("Action hasn't completed!");
                    }
                } catch (Exception e) {
                    log.error("run: Failed to execute action {} for workflow {}, status {}, ",
                            action.getName(), workflow.getId(),
                            action.getStatus(), e);
                    return;
                }
            }

            long workflowEnd = System.currentTimeMillis();
            log.info("run: Completed workflow {} in {} ms", workflow.getId(), workflowEnd - workflowStart);
        } catch (Exception e) {
            log.error("run: Encountered an error while running workflow {}", workflow.getName(), e);
        } finally {
            if (rt != null) {
                rt.shutdown();
            }
        }
    }
}
