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
import org.corfudb.infrastructure.logreplication.runtime.fsm.sink.LogReplicationSinkEvent;
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

    private Map<LogReplicationSession, LinkedList<UUID>> sessionToSinkEventIdMap;

    public FsmTaskManager(String threadName, int threadCount) {
        fsmWorker = Executors.newFixedThreadPool(threadCount, new ThreadFactoryBuilder().setNameFormat(threadName+"-%d").build());
        sessionToRuntimeEventIdMap = new ConcurrentHashMap<>();
        sessionToReplicationEventIdMap = new ConcurrentHashMap<>();
        sessionToSinkEventIdMap = new ConcurrentHashMap<>();
    }

    public <E> void addTask(E event, FsmEventType fsm) {
        if (fsm.equals(FsmEventType.LogReplicationRuntimeEvent)) {
            LogReplicationSession session = ((LogReplicationRuntimeEvent) event).getRuntimeFsm().getSession();
            sessionToRuntimeEventIdMap.putIfAbsent(session, new LinkedList<>());
            // makes the value part of the map thread safe.
            sessionToRuntimeEventIdMap.computeIfPresent(session, (sessionKey, eventList) -> {
                eventList.add(((LogReplicationRuntimeEvent) event).getEventId());
                return eventList;
            });
            fsmWorker.submit(() -> processRuntimeTask((LogReplicationRuntimeEvent) event));
        } else if (fsm.equals(FsmEventType.LogReplicationEvent)){
            LogReplicationSession session = ((LogReplicationEvent) event).getReplicationFsm().getSession();
            sessionToReplicationEventIdMap.putIfAbsent(session, new LinkedList<>());
            // makes the value part of the map thread safe.
            sessionToReplicationEventIdMap.computeIfPresent(session, (sessionKey, eventList) -> {
                eventList.add(((LogReplicationEvent) event).getEventId());
                return eventList;
            });
            fsmWorker.submit(() -> processReplicationTask((LogReplicationEvent) event));
        } else {
            LogReplicationSession session = ((LogReplicationSinkEvent) event).getSourceLeadershipManager().getSession();
            sessionToSinkEventIdMap.putIfAbsent(session, new LinkedList<>());
            // makes the value part of the map thread safe.
            sessionToSinkEventIdMap.computeIfPresent(session, (sessionKey, eventList) -> {
                eventList.add(((LogReplicationSinkEvent) event).getEventId());
                return eventList;
            });
            fsmWorker.submit(() -> processSinkTask((LogReplicationSinkEvent) event));
        }
    }

    private void processSinkTask(LogReplicationSinkEvent event) {
        LogReplicationSession session = event.getSourceLeadershipManager().getSession();
        synchronized (event.getSourceLeadershipManager()) {
            while (!sessionToSinkEventIdMap.get(session).get(0).equals(event.getEventId())) {
                try {
                    event.getSourceLeadershipManager().wait();
                } catch (InterruptedException e) {
                    log.error("Wait for session {} was interrupted {}", session, e.getMessage());
                }
            }
        }

        event.getSourceLeadershipManager().processEvent(event);

        synchronized (event.getSourceLeadershipManager()) {
            sessionToSinkEventIdMap.get(session).remove(0);
            event.getSourceLeadershipManager().notifyAll();
        }
    }

    private void processRuntimeTask(LogReplicationRuntimeEvent event) {
        LogReplicationSession session = event.getRuntimeFsm().getSession();
        // for a given session, The fsm event should be processed in the order they are queued. This condition ensures
        // that in the event of 2 threads contending for the monitor, only the task submitted first would be processed.
        synchronized (event.getRuntimeFsm()) {
            while (!sessionToRuntimeEventIdMap.get(session).get(0).equals(event.getEventId())) {
                try {
                    event.getRuntimeFsm().wait();
                } catch (InterruptedException e) {
                    log.error("Wait for session {} was interrupted {}", session, e.getMessage());
                }
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

        synchronized(event.getRuntimeFsm()) {
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

        synchronized(event.getReplicationFsm()) {
            sessionToReplicationEventIdMap.get(session).remove(0);
            event.getReplicationFsm().notifyAll();
        }
    }

    public void shutdown() {
        fsmWorker.shutdown();
    }

    public static enum FsmEventType {
        LogReplicationEvent,
        LogReplicationRuntimeEvent,
        LogReplicationSinkEvent
    }
}
