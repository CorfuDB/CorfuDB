package org.corfudb.workflow;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Workflow comprises of a series of atomic steps which on sequential execution delivers
 * the desired state.
 * These are used to drive reconfiguration changes in the cluster.
 *
 * <p>Created by zlokhandwala on 10/16/17.
 */
@Slf4j
public class Workflow {

    /**
     * Unique UUID.
     */
    @Getter
    private final UUID workflowId;

    /**
     * List of steps to be executed successfully for the completion of this workflow.
     */
    @Getter
    private final List<Step> workflow;

    /**
     * Completable future tracking the execution of all the steps.
     */
    private CompletableFuture workflowFuture = null;

    /**
     * The status of the workflow.
     */
    @Getter
    private volatile Status status = Status.INIT;

    /**
     * Thread pool to execute the workflows.
     */
    private static ExecutorService workflowExecutor = Executors.newWorkStealingPool(1);

    Workflow(WorkflowBuilder builder) {
        workflowId = builder.getWorkflowId();
        workflow = new ImmutableList.Builder<Step>()
                .addAll(builder.workflow)
                .build();
    }

    /**
     * Executes the workflow asynchronously and allows the user to track it using the status field.
     *
     * @return A completable future which completes on the successful execution of all the steps.
     */
    public CompletableFuture executeAsync() {

        workflowFuture = CompletableFuture.supplyAsync(() -> {

            status = Status.IN_PROGRESS;
            for (Step step : workflow) {
                if (!step.future.isDone()) {
                    try {
                        log.info("executeAsync: Executing workflow step: id:{}, name:{}",
                                step.stepId, step.name);
                        if (step.getTimeout() == Long.MAX_VALUE) {
                            step.execute().get();
                        } else {
                            step.execute().get(step.getTimeout(), TimeUnit.MILLISECONDS);
                        }
                        log.info("executeAsync: Completed workflow step: id:{}, name:{}",
                                step.stepId, step.name);
                    } catch (Exception e) {
                        log.error("executeAsync: Failed workflow step: id:{}, name:{} with "
                                + "exception:{}", step.stepId, step.name, e);
                        workflowFuture.completeExceptionally(e);
                        status = Status.ABORTED;
                        return false;
                    }
                }
            }
            status = Status.IN_PROGRESS;
            return true;
        }, workflowExecutor);

        return workflowFuture;
    }

    @Data
    @Accessors(chain = true)
    public static class WorkflowBuilder {

        public UUID workflowId = UUID.randomUUID();

        public List<Step> workflow = new ArrayList<>();

        /**
         * Adds a step to the existing workflow sequence.
         *
         * @param step Step to be added.
         * @return Builder instance.
         */
        public WorkflowBuilder addStep(Step step) {
            workflow.add(step);
            return this;
        }

        /**
         * Sets the workflow uuid. Used for recreation of the workflow on a different node.
         *
         * @param workflowId workflow UUID.
         * @return Builder instance.
         */
        public WorkflowBuilder setWorkflowId(@NonNull UUID workflowId) {
            this.workflowId = workflowId;
            return this;
        }

        /**
         * Builds the workflow.
         *
         * @return new Workflow object.
         */
        public Workflow build() {
            return new Workflow(this);
        }
    }
}
