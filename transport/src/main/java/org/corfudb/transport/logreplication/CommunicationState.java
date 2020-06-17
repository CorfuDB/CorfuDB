package org.corfudb.transport.logreplication;

import java.util.UUID;

public interface CommunicationState {

    /**
     * Get CommunicationState type.
     */
    CommunicationStateType getType();

    /**
     * Method to process a communication event.
     *
     * @return next LogReplicationState to transition to.
     */
    CommunicationState processEvent(CommunicationEvent event) throws IllegalTransitionException;

    /**
     * On Entry
     *
     * @param from  CommunicationState transitioning from.
     */
    default void onEntry(CommunicationState from) {}

    /**
     * On Exit
     *
     * @param to  CommunicationState transitioning to.
     */
    default void onExit(CommunicationState to) {}

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
