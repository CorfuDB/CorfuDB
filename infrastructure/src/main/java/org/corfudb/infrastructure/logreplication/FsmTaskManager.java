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
import org.corfudb.runtime.LogReplication.LogReplicationSession;

import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class manages the tasks submitted by the runtime FSM and the replication FSM.
 * Both the FSMs have their own instances of this class to which tasks are submitted and executed.
 *
 * The threads are shared by all the sessions traversing an FSM.
 */
@Slf4j
public class FsmTaskManager {

    private ExecutorService fsmWorker;

    private Map<LogReplicationSession, LinkedList<UUID>> sessionToRuntimeEventIdMap;

    private Map<LogReplicationSession, LinkedList<UUID>> sessionToReplicationEventIdMap;

    public FsmTaskManager (String threadName, int threadCount) {
        //TODO V2: add metrics around the queue sizes
        fsmWorker = Executors.newFixedThreadPool(threadCount, new ThreadFactoryBuilder().setNameFormat(threadName+"-%d").build());
        sessionToRuntimeEventIdMap = new ConcurrentHashMap<>();
        sessionToReplicationEventIdMap = new ConcurrentHashMap<>();
    }

    public <E> void addTask(E event, boolean isRuntimeEvent) {
        if (isRuntimeEvent) {
            LogReplicationSession session = ((LogReplicationRuntimeEvent) event).getRuntimeFsm().getSession();
            sessionToRuntimeEventIdMap.putIfAbsent(session, new LinkedList<>());
            sessionToRuntimeEventIdMap.get(session).add(((LogReplicationRuntimeEvent) event).getEventId());
            fsmWorker.submit(() -> processRuntimeTask((LogReplicationRuntimeEvent) event));
        } else {
            LogReplicationSession session = ((LogReplicationEvent) event).getReplicationFsm().getSession();
            sessionToReplicationEventIdMap.putIfAbsent(session, new LinkedList<>());
            sessionToReplicationEventIdMap.get(session).add(((LogReplicationEvent) event).getEventId());
            fsmWorker.submit(() -> processReplicationTask((LogReplicationEvent) event));
        }
    }

    private void processRuntimeTask(LogReplicationRuntimeEvent event) {
        // the processing of event is synchronized on a session. This is to because the submitting of the task
        // to the thread pool and updating the fsm's state is async. So its possible that thread-1 tries to execute
        // a task submitted by thread-0 before thread-0 could advance the state of FSM.
        synchronized (event.getRuntimeFsm()) {
            LogReplicationSession session = event.getRuntimeFsm().getSession();

            // for a given session, The fsm event should be processed in the order they are queued. This condition ensures
            // that in the event of 2 threads contending for the monitor, only the task submitted first would be processed.
            while(!sessionToRuntimeEventIdMap.get(session).get(0).equals(event.getEventId())) {
                try {
                    event.getRuntimeFsm().wait();
                } catch (InterruptedException e) {
                    log.error("Wait for session {} was interrupted {}", session, e.getMessage());
                }
            }

            LogReplicationRuntimeState currState = event.getRuntimeFsm().getState();
            if (currState.getType() == LogReplicationRuntimeStateType.STOPPED) {
                log.info("Log Replication Communication State Machine has been stopped. No more events will be processed.");
                return;
            }


            try {
                LogReplicationRuntimeState newState = currState.processEvent(event);
                if (newState != null) {
                    event.getRuntimeFsm().transition(currState, newState);
                    event.getRuntimeFsm().setState(newState);

                }
            } catch (IllegalTransitionException illegalState) {
                log.error("Illegal log replication event {} when in state {}", event.getType(), currState.getType());
            }

            sessionToRuntimeEventIdMap.get(session).remove(0);
            event.getRuntimeFsm().notifyAll();
        }
    }

    private void processReplicationTask(LogReplicationEvent event) {
        LogReplicationSession session = event.getReplicationFsm().getSession();
        // for a given session, the fsm events should be processed in the order they are queued. This snippets ensures
        // that in the event of 2 threads contending for the monitor, only the task submitted first would be processed.
        synchronized(event.getReplicationFsm()) {
            while (!sessionToReplicationEventIdMap.get(session).get(0).equals(event.getEventId())) {
                try {
                    event.getReplicationFsm().wait();
                } catch (InterruptedException e) {
                    log.error("Wait for session {} was interrupted {}", session, e.getMessage());
                }
            }
        }

            LogReplicationState currState = event.getReplicationFsm().getState();
            if (currState.getType() == LogReplicationStateType.ERROR) {
                log.info("Log Replication State Machine has been stopped. No more events will be processed.");
                return;
            }

            // TODO (Anny): consider strategy for continuously failing snapshot sync (never ending cancellation)

            try {
                LogReplicationState newState = currState.processEvent(event);
                log.trace("Transition from {} to {}", currState, newState);

                event.getReplicationFsm().transition(currState, newState);
                event.getReplicationFsm().setState(newState);
                event.getReplicationFsm().getNumTransitions().setValue(event.getReplicationFsm().getNumTransitions().getValue() + 1);

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

        sessionToReplicationEventIdMap.get(session).remove(0);
        synchronized(event.getReplicationFsm()) {
            event.getReplicationFsm().notifyAll();
        }
    }

    public void shutdown() {
        fsmWorker.shutdown();
    }
}
