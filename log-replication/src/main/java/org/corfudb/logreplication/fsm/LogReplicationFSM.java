package org.corfudb.logreplication.fsm;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.transmitter.LogEntryListener;
import org.corfudb.logreplication.transmitter.LogEntryReader;
import org.corfudb.logreplication.transmitter.LogEntryTransmitter;
import org.corfudb.logreplication.transmitter.PersistedReaderMetadata;
import org.corfudb.logreplication.transmitter.ReadProcessor;
import org.corfudb.logreplication.transmitter.DefaultReadProcessor;
import org.corfudb.logreplication.transmitter.SnapshotListener;
import org.corfudb.logreplication.transmitter.SnapshotReader;
import org.corfudb.logreplication.transmitter.SnapshotTransmitter;
import org.corfudb.logreplication.transmitter.StreamsLogEntryReader;
import org.corfudb.logreplication.transmitter.StreamsSnapshotReader;
import org.corfudb.logreplication.fsm.LogReplicationEvent.LogReplicationEventType;
import org.corfudb.runtime.CorfuRuntime;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class implements the Log Replication Finite State Machine.
 *
 * CorfuDB provides a Log Replication functionality for standby sites. This enables logs to
 * be automatically replicated from the primary site to a remote site. So in the case of failure or data corruption,
 * the system can failover to the standby data-store.
 *
 * This functionality is driven by the application and initiated through the ReplicationTxManager
 * on the source (primary) site and handled through the ReplicationRxManager on the destination (standby) site.
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
    private ExecutorService logReplicationFSMWorkers;

    /**
     * Executor service for FSM state tasks
     */
    @Getter
    private ExecutorService logReplicationFSMConsumer;

    /**
     * A queue of events.
     */
    private final LinkedBlockingQueue<LogReplicationEvent> eventQueue = new LinkedBlockingQueue<>();

    /**
     * An observable object on the number of transitions of this state machine (for testing & visibility)
     */
    @Getter
    private ObservableValue numTransitions = new ObservableValue(0);

    /**
     * TODO add comments
     */
    PersistedReaderMetadata persistedReaderMetadata;

    /**
     * Constructor for LogReplicationFSM, using default read processor.
     *
     * @param runtime Corfu Runtime
     * @param config log replication configuration
     * @param snapshotListener application callback for snapshot sync
     * @param logEntryListener application callback for log entry sync
     * @param workers FSM executor service for state tasks
     */
    public LogReplicationFSM(CorfuRuntime runtime, LogReplicationConfig config, SnapshotListener snapshotListener,
                             LogEntryListener logEntryListener, ExecutorService workers) {

        this(runtime, config, snapshotListener, logEntryListener, new DefaultReadProcessor(runtime), workers);
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
     */
    public LogReplicationFSM(CorfuRuntime runtime, LogReplicationConfig config, SnapshotListener snapshotListener,
                             LogEntryListener logEntryListener, ReadProcessor readProcessor, ExecutorService workers) {
        // Create transmitters to be used by the the sync states (Snapshot and LogEntry) to read and transmit data
        // through the callbacks provided by the application
        SnapshotReader defaultSnapshotReader = new StreamsSnapshotReader(runtime, config);
        LogEntryReader defaultLogEntryReader = new StreamsLogEntryReader(runtime, config);

        SnapshotTransmitter snapshotTransmitter = new SnapshotTransmitter(runtime, defaultSnapshotReader,
                snapshotListener, readProcessor, this);
        LogEntryTransmitter logEntryTransmitter = new LogEntryTransmitter(runtime, defaultLogEntryReader,
                logEntryListener, readProcessor, this);

        initializeStates(snapshotTransmitter, logEntryTransmitter);
        this.state = states.get(LogReplicationStateType.INITIALIZED);
        this.logReplicationFSMWorkers = workers;
        this.logReplicationFSMConsumer = Executors.newSingleThreadExecutor(new
                ThreadFactoryBuilder().setNameFormat("replication-fsm-consumer").build());

        logReplicationFSMConsumer.submit(this::consume);
    }

    /**
     * Constructor for LogReplicationFSM, custom readers.
     *
     * @param runtime Corfu Runtime
     * @param snapshotReader snapshot reader implementation
     * @param snapshotListener application callback for snapshot sync
     * @param logEntryReader log entry reader implementation
     * @param logEntryListener application callback for log entry sync
     * @param readProcessor read processor (for data transformation)
     * @param workers FSM executor service for state tasks
     */
    @VisibleForTesting
    public LogReplicationFSM(CorfuRuntime runtime, SnapshotReader snapshotReader, SnapshotListener snapshotListener,
                             LogEntryReader logEntryReader, LogEntryListener logEntryListener,
                             ReadProcessor readProcessor, ExecutorService workers) {

        // Create transmitters to be used by the the sync states (Snapshot and LogEntry) to read and transmit data
        // through the callbacks provided by the application
        SnapshotTransmitter snapshotTransmitter = new SnapshotTransmitter(runtime, snapshotReader, snapshotListener,
                readProcessor, this);
        LogEntryTransmitter logEntryTransmitter = new LogEntryTransmitter(runtime, logEntryReader, logEntryListener,
                readProcessor, this);

        // Initialize Log Replication 5 FSM states - single instance per state
        initializeStates(snapshotTransmitter, logEntryTransmitter);

        // Set INITIALIZED as the initial state
        this.state = states.get(LogReplicationStateType.INITIALIZED);
        this.logReplicationFSMWorkers = workers;
        this.logReplicationFSMConsumer = Executors.newSingleThreadExecutor(new
                ThreadFactoryBuilder().setNameFormat("replication-fsm-consumer").build());

        logReplicationFSMConsumer.submit(this::consume);
    }

    /**
     * Initialize all states for the Log Replication FSM.
     *
     * @param snapshotTransmitter reads and transmits snapshot syncs
     * @param logEntryTransmitter reads and transmits log entry sync
     */
    private void initializeStates(SnapshotTransmitter snapshotTransmitter, LogEntryTransmitter logEntryTransmitter) {
        /*
         * Log Replication State instances are kept in a map to be reused in transitions, avoid creating one
          * per every transition (reduce GC cycles).
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
            if (state.getType() == LogReplicationStateType.STOPPED) {
                log.info("Log Replication State Machine has been stopped. No more events will be processed.");
                return;
            }

            // TODO (Anny): consider strategy for continuously failing snapshot sync (never ending cancellation)
            //   Block until an event shows up in the queue.
            LogReplicationEvent event = eventQueue.take();

            if (event.getType() == LogReplicationEventType.LOG_ENTRY_SYNC_REPLICATED) {
                // Verify it's for the same request, as that request could've been canceled and was received later
                if (state.getType() == LogReplicationStateType.IN_LOG_ENTRY_SYNC && state.getTransitionEventId()
                        == event.getMetadata().getRequestId()) {
                    persistedReaderMetadata.setLastAckedTimestamp(event.getMetadata().getSyncTimestamp());
                }
            } else {
                if (event.getType() == LogReplicationEventType.SNAPSHOT_SYNC_COMPLETE) {
                    // Verify it's for the same request, as that request could've been canceled and was received later
                    if (state.getType() == LogReplicationStateType.IN_SNAPSHOT_SYNC && state.getTransitionEventId()
                            == event.getMetadata().getRequestId()) {
                        // Retrieve the base snapshot timestamp associated to this snapshot sync request from the
                        // transmitter
                        persistedReaderMetadata.setLastAckedTimestamp(((InSnapshotSyncState)state)
                                .getSnapshotTransmitter().getBaseSnapshotTimestamp());
                    }
                }

                try {
                    LogReplicationState newState = state.processEvent(event);
                    transition(state, newState);
                    state = newState;
                    numTransitions.setValue(numTransitions.getValue() + 1);
                } catch (IllegalLogReplicationTransition illegalState) {
                    log.debug("Illegal log replication event {} when in state {}", event.getType(), state.getType());
                }
            }

            // Consume one event in the queue and re-submit, this is done so events are consumed in
            // a round-robin fashion for the case of multi-site replication.
            logReplicationFSMConsumer.submit(this::consume);

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
