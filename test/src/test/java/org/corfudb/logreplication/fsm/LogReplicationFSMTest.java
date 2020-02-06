package org.corfudb.logreplication.fsm;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.corfudb.logreplication.LogReplicationError;
import org.corfudb.logreplication.transmitter.LogEntryListener;
import org.corfudb.logreplication.transmitter.SnapshotListener;
import org.corfudb.logreplication.transmitter.TxMessage;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.logreplication.fsm.LogReplicationEvent.LogReplicationEventType;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test Log Replication FSM.
 */
public class LogReplicationFSMTest extends AbstractViewTest implements Observer {

    private final Semaphore transitionAvailable = new Semaphore(1, true);
    private ObservableValue transitionObservable;
    private LogReplicationFSM fsm;
    private CorfuRuntime runtime;
    private SnapshotListener snapshotListener;
    private LogEntryListener logEntryListener;


    @Before
    public void setRuntime() {
        runtime = getDefaultRuntime().connect();
        initialize();
    }

    public void initialize() {
        snapshotListener = new SnapshotListener() {
            @Override
            public boolean onNext(TxMessage message, UUID snapshotSyncId) {
                return false;
            }

            @Override
            public boolean onNext(List<TxMessage> messages, UUID snapshotSyncId) {
                return false;
            }

            @Override
            public void complete(UUID snapshotSyncId) {
            }

            @Override
            public void onError(LogReplicationError error, UUID snapshotSyncId) {
            }
        };

        logEntryListener = new LogEntryListener() {
            @Override
            public boolean onNext(TxMessage message) {
                return false;
            }

            @Override
            public boolean onNext(List<TxMessage> messages) {
                return false;
            }

            @Override
            public void onError(LogReplicationError error) { }
        };
    }

    /**
     * Verify transitions of LogReplicationFSM, which starts from Initialized State (expected path) - no error path
     * tested here.
     */
    @Test
    public void testLogReplicationFSMTransitions() throws Exception {

        initLogReplicationFSM();

        // Initial state: Initialized
        LogReplicationState initState = fsm.getState();
        assertThat(initState.getType()).isEqualTo(LogReplicationStateType.INITIALIZED);

        transitionAvailable.acquire();

        // Transition #1: Replication Stop (without any replication having started)
        transition(LogReplicationEventType.REPLICATION_STOP, LogReplicationStateType.INITIALIZED);

        // Transition #2: Replication Start
        transition(LogReplicationEventType.REPLICATION_START, LogReplicationStateType.IN_LOG_ENTRY_SYNC);

        // Transition #3: Snapshot Sync Request
        UUID snapshotSyncId = transition(LogReplicationEventType.SNAPSHOT_SYNC_REQUEST, LogReplicationStateType.IN_SNAPSHOT_SYNC);

        // Transition #4: Snapshot Sync Complete
        transition(LogReplicationEventType.SNAPSHOT_SYNC_COMPLETE, LogReplicationStateType.IN_LOG_ENTRY_SYNC, snapshotSyncId);

        // Transition #5: Stop Replication
        transition(LogReplicationEventType.REPLICATION_STOP, LogReplicationStateType.INITIALIZED);
    }

    private void initLogReplicationFSM() {
        LogReplicationConfig config = new LogReplicationConfig(Collections.EMPTY_SET, UUID.randomUUID());

        fsm = new LogReplicationFSM(runtime, config, snapshotListener, logEntryListener,
                Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                .setNameFormat("state-machine-worker").build()), Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                .setNameFormat("state-machine-consumer").build()));
        transitionObservable = fsm.getNumTransitions();
        transitionObservable.addObserver(this);
    }

    /**
     * It performs a transition, based on the given event type and asserts the FSM has moved to the expected state
     * @param eventType log replication event
     * @param expectedState expected state after transition is completed
     */
    private UUID transition(LogReplicationEventType eventType,
                            LogReplicationStateType expectedState,
                            UUID eventId)
            throws InterruptedException {

        LogReplicationEvent event;
        // Enforce eventType into the FSM queue
        if (eventId != null) {
            event = new LogReplicationEvent(eventType, eventId);
        } else {
            event = new LogReplicationEvent(eventType);
        }

        fsm.input(event);

        transitionAvailable.acquire();

        assertThat(fsm.getState().getType()).isEqualTo(expectedState);

        return event.getEventID();
    }

    private UUID transition(LogReplicationEventType eventType,
                            LogReplicationStateType expectedState)
            throws InterruptedException {

        return transition(eventType, expectedState, null);
    }

    /**
     * Observer callback, will be called on every transition of the log replication FSM.
     */
    @Override
    public void update(Observable obs, Object arg) {
        if (obs == transitionObservable)
        {
            transitionAvailable.release();
            System.out.println("Num transitions :: "  + transitionObservable.getValue());
        }
    }
}
