package org.corfudb.infrastructure.orchestrator;

import java.util.List;
import java.util.UUID;

/**
 *
 * A workflow is an abstract container that specifies a series of ordered steps that achieves a
 * multi-step operation. For example, adding a new node to the cluster, which requires several
 * steps to complete.
 *
 * Created by Maithem on 10/25/17.
 */

public interface IWorkflow {

    /**
     * Gets the unique identifier of this workflow instance
     * @return id of a particular instance
     */
    UUID getId();

    /**
     * Return the name of this workflow
     * @return workflow's name
     */
    String getName();

    /**
     * Returns the ordered that are associated with
     * this workflow.
     * @return List of actions
     */
    List<Action> getActions();

    /**
     * Returns whether all the actions completed successfully or not.
     * @return true if all actions completed successfully
     */
    default boolean completed() {
        return getActions().stream()
                .allMatch(a -> a.getStatus().equals(ActionStatus.COMPLETED));
    }

    /**
     * Returns whether this workflow recovers the cluster from a failure, as opposed to a workflow
     * that was explicitly requested to reconfigure the cluster (adding or removing a node).
     * The failure detector re-dispatches recovery workflows for as long as the layout needs
     * recovery, so a failure of a recovery workflow is reported as a separate health issue that is
     * also resolved once nothing is left to recover, and not only by a later successful workflow.
     *
     * @return true if this is a recovery workflow
     */
    default boolean isRecoveryWorkflow() {
        return false;
    }
}
