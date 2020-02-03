package org.corfudb.logreplication.fsm;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;


/**
 * This class represents the Log Replication Finite State Machine.
 */
@Slf4j
public class LogReplicationFSM {

    /**
     * Log Replication Context, contains elements shared across different states in log replication.
     */
    private final LogReplicationContext context;

    /**
     * Current state of the FSM.
     */
    @Getter
    private volatile LogReplicationState state;

    /**
     * Log Replication transition.
     */
    private final LogReplicationTransition transition;

    /**
     * A queue of events.
     */
    private final LinkedBlockingQueue<LogReplicationEvent> eventQueue = new LinkedBlockingQueue<>();

    @Getter
    private volatile int numTransitions = 0;

    /**
     * Constructor.
     *
     * @param context LogReplicationContext.
     */
    public LogReplicationFSM(LogReplicationContext context) {
        this.context = context;
        this.state = new InitializedState(context);
        this.transition = new LogReplicationTransitionImpl(context);
        // Consumer thread will run on a dedicated single thread (poll queue for incoming events)
        ThreadFactory consumerThreadFactory =
            new ThreadFactoryBuilder().setNameFormat("log-replication-consumer-%d").build();
        Executors.newSingleThreadExecutor(consumerThreadFactory).submit(this::consume);
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
            if (state.getType().equals(LogReplicationStateType.STOPPED)) {
                // Log: not accepting events, in stopped state
                return;
            }
            eventQueue.put(event);
        } catch (InterruptedException ex) {
            log.error("Log Replication interrupted Exception: ", ex);
        }
    }

    /**
     * Consumer of the eventQueue.
     *
     * This method consumes the log replication events and does the state transition.
     */
    private void consume() {
        try {
            while (true) {
                // Finish consumer thread if in STOP state
                if(state.getType() == LogReplicationStateType.STOPPED) {
                    break;
                }

                // Block until an event shows up in the queue.
                LogReplicationEvent event = eventQueue.take();

                // Process the event
                LogReplicationState newState = state.processEvent(event);

                transition.onTransition(state, newState);

                numTransitions++;

                state = newState;
            }
        } catch (Throwable t) {
            log.error("Error on event consumer: ", t);
        }
    }
}
