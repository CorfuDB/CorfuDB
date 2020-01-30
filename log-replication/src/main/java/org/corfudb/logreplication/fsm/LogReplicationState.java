package org.corfudb.logreplication.fsm;

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
    LogReplicationState processEvent(LogReplicationEvent event);

    /**
     * On Entry
     *
     * @param from  LogReplicationState transitioning from.
     */
    void onEntry(LogReplicationState from);

    /**
     * On Exit
     *
     * @param to  LogReplicationState transitioning from.
     */
    void onExit(LogReplicationState to);
}



