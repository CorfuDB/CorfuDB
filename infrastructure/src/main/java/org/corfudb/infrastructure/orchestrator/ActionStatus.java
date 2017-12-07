package org.corfudb.infrastructure.orchestrator;

/**
 * The possible states in which an action can be in.
 *
 * Created by Maithem on 10/25/17.
 */

public enum ActionStatus {

    /**
     * The state of the action when it gets created
     */
    CREATED,

    /**
     * The state of the action once it is invoked
     */
    STARTED,

    /**
     * The state of the action once it completes successfully
     */
    COMPLETED,

    /**
     * The state of the action if it fails executing
     */
    ERROR
}