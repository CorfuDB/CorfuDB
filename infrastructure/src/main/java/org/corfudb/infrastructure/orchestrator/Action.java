package org.corfudb.infrastructure.orchestrator;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;

import javax.annotation.Nonnull;

/**
 *
 * A workflow action. All workflow actions must extend this class.
 *
 * Created by Maithem on 10/25/17.
 */

@Slf4j
public abstract class Action {

    ActionStatus status = ActionStatus.CREATED;

    /**
     * Returns the name of this action.
     * @return Name of action
     */
    @Nonnull
    public abstract String getName();

    /**
     * The implementation of the action
     * @param runtime A runtime that the action will use to execute
     * @throws Exception
     */
    public abstract void impl(@Nonnull CorfuRuntime runtime) throws Exception;

    /**
     * Execute the action.
     */
    @Nonnull
    public void execute(@Nonnull CorfuRuntime runtime) {
        try {
            changeStatus(ActionStatus.STARTED);
            impl(runtime);
            changeStatus(ActionStatus.COMPLETED);
        } catch (Exception e) {
            log.error("execute: error executing action {}", getName(), e);
            changeStatus(ActionStatus.ERROR);
        }
    }

    /**
     * Get the status of this action.
     * @return ActionStatus
     */
    public ActionStatus getStatus() {
        return status;
    }

    /**
     * Changes the status of this action
     * @param newStatus the new status
     */
    void changeStatus(ActionStatus newStatus) {
        status = newStatus;
    }

}
