package org.corfudb.logreplication.fsm;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.logreplication.fsm.LogReplicationEvent.LogReplicationEventType;
import org.junit.Test;

import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import static org.assertj.core.api.Assertions.assertThat;


public class LogReplicationFSMTest implements Observer {

    private final Semaphore transitionAvailable = new Semaphore(1, true);
    private ObservableValue transitionObservable;
    private LogReplicationFSM fsm;

    private void initLogReplicationFSM() {
        CorfuRuntime rt = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build());
        LogReplicationContext context = LogReplicationContext.builder().corfuRuntime(rt)
                .stateMachineWorker(Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                        .setNameFormat("state-machine-worker-%d").build())).build();
        fsm = new LogReplicationFSM(context);
        transitionObservable = fsm.getNumTransitions();
        transitionObservable.addObserver(this);
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
        transition(LogReplicationEventType.SNAPSHOT_SYNC_REQUEST, LogReplicationStateType.IN_SNAPSHOT_SYNC);

        // Transition #4: Snapshot Sync Complete
        transition(LogReplicationEventType.SNAPSHOT_SYNC_COMPLETE, LogReplicationStateType.IN_LOG_ENTRY_SYNC);

        // Transition #5: Stop Replication
        transition(LogReplicationEventType.REPLICATION_STOP, LogReplicationStateType.INITIALIZED);
    }

    /**
     * It performs a transition, based on the given event type and asserts the FSM has moved to the expected state
     * @param eventType log replication event
     * @param expectedState expected state after transition is completed
     */
    private void transition(LogReplicationEventType eventType,
                            LogReplicationStateType expectedState) throws InterruptedException {

        // Enforce eventType into the FSM queue
        fsm.input(new LogReplicationEvent(eventType));

        transitionAvailable.acquire();

        assertThat(fsm.getState().getType()).isEqualTo(expectedState);
    }

    @Override
    public void update(Observable obs, Object arg) {
        if (obs == transitionObservable)
        {
            transitionAvailable.release();
            System.out.println("Num transitions ::  "  + transitionObservable.getValue());
        }
    }
}
