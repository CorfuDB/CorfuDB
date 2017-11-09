package org.corfudb.infrastructure.orchestrator;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.format.Types;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.Request;
import org.corfudb.runtime.CorfuRuntime;

import javax.annotation.Nonnull;
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
        System.out.println("-------------- orchestrator");
    }

    public void handle(@Nonnull CorfuPayloadMsg<OrchestratorRequest> msg,
                       @Nonnull ChannelHandlerContext ctx,
                       @Nonnull IServerRouter r) {

        System.out.println("-------------- here");
        OrchestratorRequest req = msg.getPayload();
        dispatch(req);
        System.out.println("-------------- here2");
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
        Request payload = req.getRequest();
        if (payload.getType().equals(Types.OrchestratorRequestType.ADD_NODE)) {
            return new AddNodeWorkflow(payload);
        }

        throw new RuntimeException("Unknown request");
    }

    /**
     * Run a particular workflow, which entails executing all its defined
     * actions
     * @param workflow instance to run
     */
    void run(@Nonnull Workflow workflow) {

        log.info("Started workflow {}", workflow.getName());
        long startTime = System.currentTimeMillis();

        for (Action action : workflow.getActions()) {
            try {
                log.info("Action {} Status {}", action.getName(), ActionStatus.STARTED);
                action.execute(getRuntime.call());
                log.info("Action {} Status {}", action.getName(), ActionStatus.COMPLETED);
            } catch (Exception e) {
                log.info("Action {} Status {}", action.getName(), ActionStatus.ERROR, e);
                return;
            }
        }

        long stopTime = System.currentTimeMillis();
        log.info("Completed workflow {} in {} ms", workflow.getName(), stopTime - startTime);
    }
}
