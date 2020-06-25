package org.corfudb.infrastructure.logreplication.runtime.fsm;

import java.util.UUID;

public interface LogReplicationRuntimeState {

    /**
     * Get LogReplicationRuntimeState type.
     */
    LogReplicationRuntimeStateType getType();

    /**
     * Method to process a communication event.
     *
     * @return next LogReplicationState to transition to.
     */
    LogReplicationRuntimeState processEvent(LogReplicationRuntimeEvent event) throws IllegalTransitionException;

    /**
     * On Entry
     *
     * @param from  LogReplicationRuntimeState transitioning from.
     */
    default void onEntry(LogReplicationRuntimeState from) {}

    /**
     * On Exit
     *
     * @param to  LogReplicationRuntimeState transitioning to.
     */
    default void onExit(LogReplicationRuntimeState to) {}

    /**
     * Provides capability to clear/clean state information onEntry.
     */
    default void clear() {}

}
