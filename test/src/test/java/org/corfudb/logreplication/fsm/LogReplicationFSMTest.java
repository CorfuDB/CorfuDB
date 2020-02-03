package org.corfudb.logreplication.fsm;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.logreplication.fsm.LogReplicationEvent.LogReplicationEventType;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static org.assertj.core.api.Assertions.assertThat;


public class LogReplicationFSMTest {

    private int fsmTransitions = 0;
    private LogReplicationFSM fsm;

    private void initLogReplicationFSM() {
        CorfuRuntime rt = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build());
        LogReplicationContext context = LogReplicationContext.builder().corfuRuntime(rt)
                .blockingOpsScheduler(Executors.newScheduledThreadPool(1, (r) -> {
                    ThreadFactory threadFactory =
                            new ThreadFactoryBuilder().setNameFormat("replication-fsm-%d").build();
                    Thread t = threadFactory.newThread(r);
                    t.setDaemon(true);
                    return t;
                })).build();
        fsm = new LogReplicationFSM(context);
    }

    /**
     * Verify transitions of LogReplicationFSM, which starts from Initialized State (expected path) - no error path
     * tested here.
     */
    @Test
    public void testLogReplicationFSMTransitions() {

        initLogReplicationFSM();

        // Initial state: Initialized
        LogReplicationState initState = fsm.getState();
        assertThat(initState.getType()).isEqualTo(LogReplicationStateType.INITIALIZED);

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

    private void transition(LogReplicationEventType eventType, LogReplicationStateType expectedState) {

        // Enforce eventType into the FSM queue
        fsm.input(new LogReplicationEvent(eventType));

        // Wait for the transition to happen
        while (fsmTransitions >= fsm.getNumTransitions()) {
            // Wait for transition, if the transition does not occur the test will timeout
        }

        fsmTransitions++;

        assertThat(fsm.getState().getType()).isEqualTo(expectedState);
    }

}
