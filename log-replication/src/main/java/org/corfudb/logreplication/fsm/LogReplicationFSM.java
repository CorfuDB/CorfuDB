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
     * Current state of the FSM.
     */
    @Getter
    private LogReplicationState state;

    /**
     * A queue of events.
     */
    private final LinkedBlockingQueue<LogReplicationEvent> eventQueue = new LinkedBlockingQueue<>();

    @Getter
    private ObservableValue numTransitions = new ObservableValue(0);

    /**
     * Constructor.
     *
     * @param context LogReplicationContext.
     */
    public LogReplicationFSM(LogReplicationContext context) {
        this.state = new InitializedState(context);
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

                System.out.println("Processing event: " + event.getType());

                // Process the event
                LogReplicationState newState = state.processEvent(event);

                transition(state, newState);

                state = newState;

                numTransitions.setValue(numTransitions.getValue() + 1);
            }
        } catch (Throwable t) {
            log.error("Error on event consumer: ", t);
        }
    }

    void transition(LogReplicationState from, LogReplicationState to) {
        if (from != to) {
            from.onExit(to);

            to.onEntry(from);
        }
    }
}
