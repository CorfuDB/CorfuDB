package org.corfudb.logreplication.fsm;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.transmitter.LogEntryListener;
import org.corfudb.logreplication.transmitter.LogEntryReader;
import org.corfudb.logreplication.transmitter.LogEntryTransmitter;
import org.corfudb.logreplication.transmitter.ReadProcessor;
import org.corfudb.logreplication.transmitter.SimpleReadProcessor;
import org.corfudb.logreplication.transmitter.SnapshotListener;
import org.corfudb.logreplication.transmitter.SnapshotReader;
import org.corfudb.logreplication.transmitter.SnapshotTransmitter;
import org.corfudb.logreplication.transmitter.StreamsLogEntryReader;
import org.corfudb.logreplication.transmitter.StreamsSnapshotReader;
import org.corfudb.runtime.CorfuRuntime;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class implements the Log Replication Finite State Machine.
 *
 * CorfuDB provides a Log Replication functionality for standby data-stores. This enables logs to
 * be automatically replicated from the primary site to a remote site. So in the case of failure or data corruption,
 * the system can failover to the standby data-store.
 *
 * This functionality is driven by the application and initiated through the ReplicationTxManager
 * on the source (primary) site and handled through the ReplicationRxManager on the destination site.
 *
 * Log Replication on the transmitter side is defined by an event-driven finite state machine, with 5 states
 * and 7 events/messages---which can trigger the transition between states.
 *
 * States:
 * ------
 *  - Initialized (initial state)
 *  - In_Log_Entry_Sync
 *  - In_Snapshot_Sync
 *  - Snapshot_Sync_Required
 *  - Stopped
 *
 * Events:
 * ------
 *  - replication_start
 *  - replication_stop
 *  - snapshot_sync_request
 *  - snapshot_sync_complete
 *  - log_entry_sync_request
 *  - trimmed_exception
 *  - replication_shutdown
 *
 *
 *
 *                                       replication_stop
 *                      +-------------------------------------------------+
 *    replication_stop  |                                                 |
 *             +-----+  |              replication_stop                   |
 *             |     |  v      v-----------------------------+            |
 *             |    ++--+---------+                          |        +---+--------------------+
 *             +--->+ INITIALIZED +------------------------+ |        | SNAPSHOT_SYNC_REQUIRED +<---+
 *                  +---+----+----+ snapshot_sync_request  | |        +---+-------------+----+-+    |
 *                      ^    |                             | |            |             ^    ^      |
 *                      |    |                             | |    snapshot|             |    |      |
 *                      |    |                             | |      sync  |             |    |      |
 *     replication_stop |    | replication_start           | |     request|     trimmed |    |      |
 *                      |    |                             | |            |    exception|    |      |
 *                      |    v                             v |            v             |    |      |
 *               +------+----+-------+  snapshot_sync    +-+-+------------+-+           |    |      |
 *               | IN_LOG_ENTRY_SYNC |     request       | IN_SNAPSHOT_SYNC +-----------+    |      |
 *               |                   +------------------>+                  |                |      |
 *               +----+----+---------+                   +---+--------------+----------------+      |
 *                    |    ^                                 |                  snapshot_sync       |
 *                    |    +---------------------------------+                     cancel           |
 *                    |                snapshot_sync                                                |
 *                    |                  complete                                                   |
 *                    |                                                                             |
 *                    +-----------------------------------------------------------------------------+
 *                                                     trimmed_exception
 *               replication
 * +---------+    shutdown    +------------+
 * | STOPPED +<---------------+ ALL_STATES |
 * +---------+                +------------+
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

    /**
     * An observable object on the number of transitions.
     */
    @Getter
    private ObservableValue numTransitions = new ObservableValue(0);

    /**
     * Constructor for LogReplicationFSM, default read processor.
     *
     * @param runtime Corfu Runtime
     * @param config log replication configuration
     * @param snapshotListener application callback for snapshot sync
     * @param logEntryListener application callback for log entry sync
     * @param workers FSM executor service for state tasks
     * @param consumers FSM executor service for event consumption
     */
    public LogReplicationFSM(CorfuRuntime runtime, LogReplicationConfig config, SnapshotListener snapshotListener,
                             LogEntryListener logEntryListener, ExecutorService workers, ExecutorService consumers) {

        // This uses the default readProcessor (no actual transformation of the Data)
        ReadProcessor readProcessor = new SimpleReadProcessor(runtime);

        // Create transmitters to be used by the the sync states (Snapshot and LogEntry) to read and transmit data
        // through the callbacks provided by the application
        SnapshotReader defaultSnapshotReader = new StreamsSnapshotReader(runtime, config, readProcessor);
        LogEntryReader defaultLogEntryReader = new StreamsLogEntryReader(runtime, config, readProcessor);

        SnapshotTransmitter snapshotTransmitter = new SnapshotTransmitter(runtime, defaultSnapshotReader, snapshotListener, this);
        LogEntryTransmitter logEntryTransmitter = new LogEntryTransmitter(runtime, defaultLogEntryReader, logEntryListener, this);

        initializeStates(snapshotTransmitter, logEntryTransmitter);
        this.state = states.get(LogReplicationStateType.INITIALIZED);
        this.stateMachineWorker = workers;

        consumers.submit(this::consume);
    }

    /**
     * Constructor for LogReplicationFSM, custom read processor for data transformation.
     *
     * @param runtime Corfu Runtime
     * @param config log replication configuration
     * @param snapshotListener application callback for snapshot sync
     * @param logEntryListener application callback for log entry sync
     * @param readProcessor read processor for data transformation
     * @param workers FSM executor service for state tasks
     * @param consumers FSM executor service for event consumption
     */
    public LogReplicationFSM(CorfuRuntime runtime, LogReplicationConfig config, SnapshotListener snapshotListener,
                             LogEntryListener logEntryListener, ReadProcessor readProcessor, ExecutorService workers,
                             ExecutorService consumers) {

        // Create transmitters to be used by the the sync states (Snapshot and LogEntry) to read and transmit data
        // through the callbacks provided by the application
        SnapshotReader defaultSnapshotReader = new StreamsSnapshotReader(runtime, config, readProcessor);
        LogEntryReader defaultLogEntryReader = new StreamsLogEntryReader(runtime, config, readProcessor);

        SnapshotTransmitter snapshotTransmitter = new SnapshotTransmitter(runtime, defaultSnapshotReader, snapshotListener, this);
        LogEntryTransmitter logEntryTransmitter = new LogEntryTransmitter(runtime, defaultLogEntryReader, logEntryListener, this);

        initializeStates(snapshotTransmitter, logEntryTransmitter);
        this.state = states.get(LogReplicationStateType.INITIALIZED);
        this.stateMachineWorker = workers;

        consumers.submit(this::consume);
    }

    /**
     * Constructor for LogReplicationFSM, custom readers.
     *
     * @param runtime Corfu Runtime
     * @param config log replication configuration
     * @param snapshotReader snapshot reader implementation
     * @param snapshotListener application callback for snapshot sync
     * @param logEntryReader log entry reader implementation
     * @param logEntryListener application callback for log entry sync
     * @param workers FSM executor service for state tasks
     */
    public LogReplicationFSM(CorfuRuntime runtime, LogReplicationConfig config,
                             SnapshotReader snapshotReader, SnapshotListener snapshotListener,
                             LogEntryReader logEntryReader, LogEntryListener logEntryListener,
                             ExecutorService workers, ExecutorService consumers) {

        // Create transmitters to be used by the the sync states (Snapshot and LogEntry) to read and transmit data
        // through the callbacks provided by the application
        SnapshotTransmitter snapshotTransmitter = new SnapshotTransmitter(runtime, snapshotReader, snapshotListener, this);
        LogEntryTransmitter logEntryTransmitter = new LogEntryTransmitter(runtime, logEntryReader, logEntryListener, this);

        // Initialize Log Replication 5 FSM states - single instance per state
        initializeStates(snapshotTransmitter, logEntryTransmitter);

        // Set INITIALIZED as the initial state
        this.state = states.get(LogReplicationStateType.INITIALIZED);
        this.stateMachineWorker = workers;

        consumers.submit(this::consume);
    }

    /**
     * Initialize all states for the Log Replication FSM.
     *
     * @param snapshotTransmitter reads and transmits snapshot syncs
     * @param logEntryTransmitter reads and transmits log entry sync
     */
    private void initializeStates(SnapshotTransmitter snapshotTransmitter, LogEntryTransmitter logEntryTransmitter) {
        /*
         * Log Replication State instances are kept in a map to be reused in transitions.
         */
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

                // TODO (Anny): consider strategy for continuously failing snapshot sync (never ending cancellation)
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

    /**
     * Perform transition between states.
     *
     * @param from initial state
     * @param to final state
     */
    void transition(LogReplicationState from, LogReplicationState to) {
        from.onExit(to);
        to.clear();
        to.onEntry(from);
    }
}
