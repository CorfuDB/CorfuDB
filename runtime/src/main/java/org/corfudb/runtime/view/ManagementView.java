package org.corfudb.runtime.view;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.orchestrator.CreateWorkflowResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.WorkflowException;
import org.corfudb.runtime.exceptions.WorkflowResultUnknownException;
import org.corfudb.runtime.view.workflows.AddNode;
import org.corfudb.runtime.view.workflows.HealNode;
import org.corfudb.runtime.view.workflows.RemoveNode;
import org.corfudb.util.Sleep;

import javax.annotation.Nonnull;

/**
 * A view of the Management Service to manage reconfigurations of the Corfu Cluster.
 * <p>
 * <p>Created by zlokhandwala on 11/20/17.</p>
 */
@Slf4j
public class ManagementView extends AbstractView {

    public ManagementView(@NonNull CorfuRuntime runtime) {
        super(runtime);
    }

    /**
     * Remove a node from the cluster.
     *
     * @param endpointToRemove Endpoint of the node to be removed from the cluster.
     * @param retry            the number of times to retry a workflow if it fails
     * @param timeout          total time to wait before the workflow times out
     * @param pollPeriod       the poll interval to check whether a workflow completed or not
     * @throws WorkflowResultUnknownException when the side affect of the operation
     *                                        can't be determined
     * @throws WorkflowException              when the remove operation fails
     */
    public void removeNode(@Nonnull String endpointToRemove, int retry,
                           @Nonnull Duration timeout, @Nonnull Duration pollPeriod) {

        new RemoveNode(endpointToRemove, runtime, retry, timeout, pollPeriod).invoke();
    }

    /**
     * Add a new node to the existing cluster.
     *
     * @param endpointToAdd Endpoint of the new node to be added to the cluster.
     * @param retry         the number of times to retry a workflow if it fails
     * @param timeout       total time to wait before the workflow times out
     * @param pollPeriod    the poll interval to check whether a workflow completed or not
     * @throws WorkflowResultUnknownException when the side affect of the operation
     *                                        can't be determined
     */
    public void addNode(@Nonnull String endpointToAdd, int retry,
                        @Nonnull Duration timeout, @Nonnull Duration pollPeriod) {
        new AddNode(endpointToAdd, runtime, retry, timeout, pollPeriod)
                .invoke();
    }

    /**
     * Heal an unresponsive node.
     *
     * @param endpointToHeal Endpoint of the new node to be healed in the cluster.
     * @param retry          the number of times to retry a workflow if it fails
     * @param timeout        total time to wait before the workflow times out
     * @param pollPeriod     the poll interval to check whether a workflow completed or not
     * @throws WorkflowResultUnknownException when the side affect of the operation
     *                                        can't be determined
     */
    public void healNode(@Nonnull String endpointToHeal, int retry, @Nonnull Duration timeout,
                         @Nonnull Duration pollPeriod) {
        new HealNode(endpointToHeal, runtime, retry, timeout, pollPeriod).invoke();
    }
}
