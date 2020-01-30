package org.corfudb.logreplication.fsm;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


/**
 * This class represents the Log Replication Finite State Machine.
 */
public class LogReplicationFSM {

    /**
     * LogReplicationFSM consumer scheduled period in milliseconds.
     */
    private final int LOG_REPLICATION_FSM_PERIOD = 1000;

    /**
     * Log Replication Context, contains elements shared across different states in log replication.
     */
    private final LogReplicationContext context;

    /**
     * Current state of the FSM.
     */
    private LogReplicationState state;

    /**
     * Log Replication transition.
     */
    private final LogReplicationTransition transition;

    /**
     * A queue of events.
     */
    private final LinkedBlockingQueue<LogReplicationEvent> eventQueue = new LinkedBlockingQueue<>();

    /**
     * Constructor.
     *
     * @param context LogReplicationContext.
     */
    public LogReplicationFSM(LogReplicationContext context) {
        this.context = context;
        this.state = new InitializedState(context);
        this.transition = new LogReplicationTransitionImpl(context);
        context.getNonBlockingOpsScheduler()
                .scheduleWithFixedDelay(this::consume, 0,
                        LOG_REPLICATION_FSM_PERIOD,
                        TimeUnit.MILLISECONDS);
    }

    /**
     * Input function of the FSM.
     *
     * This method enqueues log replication events for further processing.
     *
     * @param event LogReplicationEvent to process.
     */
    public void input(LogReplicationEvent event) {
        try {
            eventQueue.put(event);
        } catch (InterruptedException ex) {
            // Log Error Message
        }
    }

    /**
     * Consumer of the eventQueue.
     *
     * This method consumes the log replication events and does the state transition.
     */
    private void consume() {
        try {
            while (eventQueue.size() > 0) {
                // Block until an event shows up in the queue.
                LogReplicationEvent event = eventQueue.take();

                // Add any logic that given an event requires to update context data

                // Process the event
                LogReplicationState newState = state.processEvent(event);

                transition.onTransition(state, newState);

                state = newState;
            }
        } catch (Throwable t) {
            // Log Error
        }
    }
}
