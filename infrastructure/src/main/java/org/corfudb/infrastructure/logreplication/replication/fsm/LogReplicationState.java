package org.corfudb.infrastructure.logreplication.replication.fsm;

import java.util.UUID;

/**
 * An interface for log replication state classes.
 *
 * All log replication states implement this interface.
 */
public interface LogReplicationState {

    /**
     * Get LogReplicationState type.
     */
    LogReplicationStateType getType();

    /**
     * Method to process a log replication event.
     *
     * @return next LogReplicationState to transition to.
     */
    LogReplicationState processEvent(LogReplicationEvent event) throws IllegalTransitionException;

    /**
     * On Entry
     *
     * @param from  LogReplicationState transitioning from.
     */
    default void onEntry(LogReplicationState from) {}

    /**
     * On Exit
     *
     * @param to  LogReplicationState transitioning to.
     */
    default void onExit(LogReplicationState to) {}

    /**
     * Provides capability to clear/clean state information onEntry.
     */
    default void clear() {}

    /**
     * Link state to the event that caused the transition to it.
     *
     * @param eventId event identifier
     */
    default void setTransitionEventId(UUID eventId) {}

    /**
     * Retrieve the id of the event that caused the transition to this state.
     */
    default UUID getTransitionEventId() { return null; }
}



