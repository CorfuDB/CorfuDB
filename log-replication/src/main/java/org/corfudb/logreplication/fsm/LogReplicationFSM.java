package org.corfudb.logreplication.fsm;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.transmitter.LogEntryListener;
import org.corfudb.logreplication.transmitter.LogEntryReader;
import org.corfudb.logreplication.transmitter.LogEntryTransmitter;
import org.corfudb.logreplication.transmitter.SnapshotListener;
import org.corfudb.logreplication.transmitter.SnapshotReader;
import org.corfudb.logreplication.transmitter.SnapshotTransmitter;
import org.corfudb.logreplication.transmitter.StreamsLogEntryReader;
import org.corfudb.logreplication.transmitter.StreamsSnapshotReader;
import org.corfudb.runtime.CorfuRuntime;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
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
     * Map of all Log Replication FSM States
     */
    @Getter
    private Map<LogReplicationStateType, LogReplicationState> states = new HashMap<>();

    /**
     * Executor service for FSM state tasks
     */
    @Getter
    private ExecutorService stateMachineWorker;

    /**
     * A queue of events.
     */
    private final LinkedBlockingQueue<LogReplicationEvent> eventQueue = new LinkedBlockingQueue<>();

    @Getter
    private ObservableValue numTransitions = new ObservableValue(0);




    public LogReplicationFSM(CorfuRuntime runtime, LogReplicationConfig config, SnapshotListener snapshotListener,
                             LogEntryListener logEntryListener, ExecutorService workers) {

        // Create transmitters to be used by the the sync states (Snapshot and LogEntry) to read and transmit data
        // through the callbacks provided by the application
        SnapshotReader defaultSnapshotReader = new StreamsSnapshotReader(runtime, config);
        LogEntryReader defaultLogEntryReader = new StreamsLogEntryReader(runtime, config);

        SnapshotTransmitter snapshotTransmitter = new SnapshotTransmitter(runtime, defaultSnapshotReader, snapshotListener, this);
        LogEntryTransmitter logEntryTransmitter = new LogEntryTransmitter(runtime, defaultLogEntryReader, logEntryListener);

        initializeStates(snapshotTransmitter, logEntryTransmitter);
        this.state = states.get(LogReplicationStateType.INITIALIZED);
        this.stateMachineWorker = workers;

        // Consumer thread will run on a dedicated single thread (poll queue for incoming events)
        ThreadFactory consumerThreadFactory =
                new ThreadFactoryBuilder().setNameFormat("replication-fsm-consumer-%d").build();
        Executors.newSingleThreadExecutor(consumerThreadFactory).submit(this::consume);
    }

    public LogReplicationFSM(CorfuRuntime runtime, LogReplicationConfig config,
                             SnapshotReader snapshotReader, SnapshotListener snapshotListener,
                             LogEntryReader logEntryReader, LogEntryListener logEntryListener, ExecutorService workers) {

        // Create transmitters to be used by the the sync states (Snapshot and LogEntry) to read and transmit data
        // through the callbacks provided by the application
        SnapshotTransmitter snapshotTransmitter = new SnapshotTransmitter(runtime, snapshotReader, snapshotListener, this);
        LogEntryTransmitter logEntryTransmitter = new LogEntryTransmitter(runtime, logEntryReader, logEntryListener);

        // Initialize Log Replication 5 FSM states - single instance per state
        initializeStates(snapshotTransmitter, logEntryTransmitter);

        // Set INITIALIZED as the initial state
        this.state = states.get(LogReplicationStateType.INITIALIZED);
        this.stateMachineWorker = workers;

        // Consumer thread will run on a dedicated single thread (poll queue for incoming events)
        ThreadFactory consumerThreadFactory =
                new ThreadFactoryBuilder().setNameFormat("replication-fsm-consumer-%d").build();
        Executors.newSingleThreadExecutor(consumerThreadFactory).submit(this::consume);
    }

    private void initializeStates(SnapshotTransmitter snapshotTransmitter, LogEntryTransmitter logEntryTransmitter) {
        states.put(LogReplicationStateType.INITIALIZED, new InitializedState(this));
        states.put(LogReplicationStateType.IN_SNAPSHOT_SYNC, new InSnapshotSyncState(this, snapshotTransmitter));
        states.put(LogReplicationStateType.IN_LOG_ENTRY_SYNC, new InLogEntrySyncState(this, logEntryTransmitter));
        states.put(LogReplicationStateType.IN_REQUIRE_SNAPSHOT_SYNC, new InRequireSnapshotSyncState(this));
        states.put(LogReplicationStateType.STOPPED, new StoppedState());
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

                transition(state, newState);

                state = newState;

                numTransitions.setValue(numTransitions.getValue() + 1);
            }
        } catch (Throwable t) {
            log.error("Error on event consumer: ", t);
        }
    }

    void transition(LogReplicationState from, LogReplicationState to) {
        from.onExit(to);
        to.clear();
        to.onEntry(from);
    }
}
