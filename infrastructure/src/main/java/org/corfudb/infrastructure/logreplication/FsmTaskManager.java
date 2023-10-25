package org.corfudb.infrastructure.logreplication;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationState;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationStateType;
import org.corfudb.infrastructure.logreplication.runtime.fsm.IllegalTransitionException;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeState;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeStateType;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

@Slf4j
public class FsmTaskManager {

    private ExecutorService fsmWorker;

    public FsmTaskManager (String threadName, int threadCount) {
        fsmWorker = Executors.newFixedThreadPool(threadCount, new ThreadFactoryBuilder().setNameFormat(threadName+"-%d").build());
    }

    public <E, T> void addTask(E event, Class<T> clazz) {
        fsmWorker.submit(() -> {
            if (clazz.getName().equals(LogReplicationRuntimeEvent.class.getName())) {
                addLrRuntimeTask((LogReplicationRuntimeEvent) event);
            } else {
                addLrReplicationTask((LogReplicationEvent) event);
            }

        });
    }

    private void addLrRuntimeTask(LogReplicationRuntimeEvent event) {
        try {
            LogReplicationRuntimeState currState;
            // the get and set of "fsm.state" is synchronized on a session. This is to because the submitting of the task
            // to the thread pool and updating the "fsm.state" is async. So its possible that thread-1 tries to execute
            // a task submitted by thread-0 before thread-0 could advance the FSM.
            synchronized (event.getRuntimeFsm()) {
                currState = event.getRuntimeFsm().getState();
            }
            if (currState.getType() == LogReplicationRuntimeStateType.STOPPED) {
                log.info("Log Replication Communication State Machine has been stopped. No more events will be processed.");
                return;
            }


            try {
                LogReplicationRuntimeState newState = currState.processEvent(event);
                if (newState != null) {
                    synchronized (event.getRuntimeFsm()) {
                        event.getRuntimeFsm().transition(currState, newState);
                        event.getRuntimeFsm().setState(newState);
                    }
                }
            } catch (IllegalTransitionException illegalState) {
                log.error("Illegal log replication event {} when in state {}", event.getType(), currState.getType());
            }

        } catch (Throwable t) {
            log.error("Error on event consumer: ", t);
        }
    }

    private void addLrReplicationTask(LogReplicationEvent event) {
        LogReplicationState currState;
        synchronized(event.getReplicationFsm()) {
            currState = event.getReplicationFsm().getState();
        }
        if (currState.getType() == LogReplicationStateType.ERROR) {
            log.info("Log Replication State Machine has been stopped. No more events will be processed.");
            return;
        }

        // TODO (Anny): consider strategy for continuously failing snapshot sync (never ending cancellation)

        try {
            LogReplicationState newState = currState.processEvent(event);
            log.trace("Transition from {} to {}", currState, newState);
            synchronized (event.getReplicationFsm()) {
                event.getReplicationFsm().transition(currState, newState);
                event.getReplicationFsm().setState(newState);
                event.getReplicationFsm().getNumTransitions().setValue(event.getReplicationFsm().getNumTransitions().getValue() + 1);

            }
        } catch (org.corfudb.infrastructure.logreplication.replication.fsm.IllegalTransitionException illegalState) {
            // Ignore LOG_ENTRY_SYNC_REPLICATED events for logging purposes as they will likely come in frequently,
            // as it is used for update purposes but does not imply a transition.
            if (!event.getType().equals(LogReplicationEvent.LogReplicationEventType.LOG_ENTRY_SYNC_REPLICATED)) {
                log.error("Illegal log replication event {} when in state {}", event.getType(), currState.getType());
            }
        }

        // For testing purpose to notify the event generator the stop of the event.
        if (event.getType() == LogReplicationEvent.LogReplicationEventType.REPLICATION_STOP) {
            synchronized (event) {
                event.notifyAll();
            }
        }
    }

    public void shutdown() {
        fsmWorker.shutdown();
    }
}
