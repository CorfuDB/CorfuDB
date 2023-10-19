package org.corfudb.infrastructure.logreplication.runtime;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationSourceManager;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeState;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeStateType;
import org.corfudb.infrastructure.logreplication.runtime.fsm.NegotiatingState;
import org.corfudb.infrastructure.logreplication.runtime.fsm.ReplicatingState;
import org.corfudb.infrastructure.logreplication.runtime.fsm.StoppedState;
import org.corfudb.infrastructure.logreplication.runtime.fsm.UnrecoverableState;
import org.corfudb.infrastructure.logreplication.runtime.fsm.VerifyingRemoteSinkLeaderState;
import org.corfudb.infrastructure.logreplication.runtime.fsm.WaitingForConnectionsState;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Runtime to connect to a remote Corfu Log Replication Cluster.
 * <p>
 * This class represents the Log Replication Runtime Finite State Machine, which defines
 * all states in which the leader node on the source cluster can be.
 *
 *
 *                                                  R-LEADER_LOSS / NOT_FOUND
 *                                             +-------------------------------+
 *                              ON_CONNECTION  |                               |    ON_CONNECTION_DOWN
 *                                    UP       |       ON_CONNECTION_DOWN      |       (NON_LEADER)
 *                                    +----+   |          (R-LEADER)           |
 *                                    |    |   |   +-----------------------+   |        +-----+
 *                                    |    |   |   |                       |   |        |     |
 * +---------------+  ON_CONNECTION  ++----v---v---v--+                  +-+---+--------+-+   |
 * |               |       UP        |                |  R-LEADER_FOUND  |                <---+
 * |    WAITING    +---------------->+    VERIFYING   +------------------>                +---+
 * |      FOR      |                 |     REMOTE     |                  |   NEGOTIATING  |   |  NEGOTIATION_FAILED
 * |  CONNECTIONS  +<----------------+     LEADER     |                  |                <---+       (ALARM)
 * |               |  ON_CONNECTION  |                +<-----------+     |                +----+
 * +---------------+      DOWN       +-^----+---^----++            |     +-------+-----^--+    |
 *                       (ALL)         |    |   |    |             |             |     |       |
 *                                     |    |   |    |        R-LEADER_LOSS      |     +-------+
 *                                     +----+   +----+             |             |  ON_CONNECTION_UP
 *                              ON_CONNECTION     R-LEADER_NOT     |             |    (NON-LEADER)
 *                                  DOWN              FOUND        |             |
 *                                (NOT ALL)                        |     NEGOTIATE_COMPLETE
 *                                                                 |             |
 *                                                           ON_CONNECTION       |   ON_CONNECTION_UP
 *                                                               DOWN            |     (NON-LEADER)
 *                                                             (R-LEADER)        |      +-----+
 *                                                                 |             |      |     |
 *                                                                 |     +-------v------+-+   |
 *            +---------------+      ALL STATES                    +-----+                <---+
 *            |               |                                          |                |
 *            |   STOPPED     <---- L-LEADER_LOSS                        |  REPLICATING   |
 *            |               |                     SITE FLIP <-----     |                |
 *            |               |                                          |                +----+
 *            +---------------+                                          +--------------^-+    |
 *                                                                                      |      |
 *                                                                                      +------+
 *            +---------------+     ALL STATES
 *            |               |                                                     ON_CONNECTION_DOWN
 *            | UNRECOVERABLE <---- ON_ERROR                                           (NON-LEADER)
 *            |    STATE      |
 *            |               |
 *            +---------------+
 *
 *
 * States:
 * ------
 *
 * - WAITING_FOR_CONNECTIVITY    :: initial state, waiting for any connection to remote cluster to be established.
 * - VERIFYING_REMOTE_LEADER     :: verifying the leader endpoint on remote cluster (querying all connected nodes)
 * - NEGOTIATING                 :: negotiating against the leader endpoint
 * - REPLICATING                 :: replicating data to remote cluster through the leader endpoint
 * - STOPPED                     :: stop state machine, no error, just lost leadership so replication stops from this node
 * - UNRECOVERABLE_STATE         :: error state, unrecoverable error reported by replication, transport or cluster manager, despite
 *                                  being the leader node.
 *
 *
 * Events / Transitions:
 * --------------------
 *
 * - ON_CONNECTION_UP           :: connection to a remote endpoint comes UP
 * - ON_CONNECTION_DOWN         :: connection to a remote endpoint comes DOWN
 * - REMOTE_LEADER_NOT_FOUND,   :: remote leader not found
 * - REMOTE_LEADER_FOUND,       :: remote leader found
 * - REMOTE_LEADER_LOSS,        :: remote Leader Lost (remote node reports it is no longer the leader)
 * - LOCAL_LEADER_LOSS          :: local node looses leadership
 * - NEGOTIATION_COMPLETE,      :: negotiation succeeded and completed
 * - NEGOTIATION_FAILED,        :: negotiation failed
 * - STOPPED                    :: stop log replication server (fatal state)
 *
 * @author amartinezman
 */
@Slf4j
public class CorfuLogReplicationRuntime {

    // TODO(Anny): add cluster_role_flip event... probably we need a new state called finishing_ongoing_replication...
    //   and go to stopped...

    public static final int DEFAULT_TIMEOUT = 5000;

    /**
     * Current state of the FSM.
     */
    @Getter
    @Setter
    private volatile LogReplicationRuntimeState state;

    /**
     * Map of all Log Replication Communication FSM States (reuse single instance for each state)
     */
    @Getter
    private Map<LogReplicationRuntimeStateType, LogReplicationRuntimeState> states = new HashMap<>();

    private final LogReplicationClientServerRouter router;

    @Getter
    private final LogReplicationSourceManager sourceManager;
    private final Set<String> connectedNodes;
    private volatile Optional<String> leaderNodeId = Optional.empty();

    @Getter
    public final String remoteClusterId;

    @Getter
    public final LogReplicationSession session;

    @Getter
    private final LogReplicationContext replicationContext;

    /**
     * Default Constructor
     */
    public CorfuLogReplicationRuntime(LogReplicationMetadataManager metadataManager, LogReplicationSession session,
                                      LogReplicationContext replicationContext, LogReplicationClientServerRouter router) {
        this.remoteClusterId = session.getSinkClusterId();
        this.session = session;
        this.router = router;
        this.sourceManager = new LogReplicationSourceManager(router, metadataManager,
                session, replicationContext);
        this.connectedNodes = new HashSet<>();
        this.replicationContext = replicationContext;

        initializeStates(metadataManager);
        this.state = states.get(LogReplicationRuntimeStateType.WAITING_FOR_CONNECTIVITY);

        log.info("Log Replication Runtime State Machine initialized");
    }

    /**
     * Initialize all states for the Log Replication Runtime FSM.
     */
    private void initializeStates(LogReplicationMetadataManager metadataManager) {
        /*
         * Log Replication Runtime State instances are kept in a map to be reused in transitions, avoid creating one
         * per every transition (reduce GC cycles).
         */
        states.put(LogReplicationRuntimeStateType.WAITING_FOR_CONNECTIVITY, new WaitingForConnectionsState(this));
        states.put(LogReplicationRuntimeStateType.VERIFYING_REMOTE_LEADER, new VerifyingRemoteSinkLeaderState(this,
            router));
        states.put(LogReplicationRuntimeStateType.NEGOTIATING, new NegotiatingState(this, router, metadataManager));
        states.put(LogReplicationRuntimeStateType.REPLICATING, new ReplicatingState(this, sourceManager, router));
        states.put(LogReplicationRuntimeStateType.STOPPED, new StoppedState(sourceManager));
        states.put(LogReplicationRuntimeStateType.UNRECOVERABLE, new UnrecoverableState());
    }

    /**
     * Input function of the FSM.
     * <p>
     * This method enqueues runtime events for further processing.
     *
     * @param event LogReplicationRuntimeEvent to process.
     */
    public synchronized void input(LogReplicationRuntimeEvent event) {
        if (state.getType().equals(LogReplicationRuntimeStateType.STOPPED)) {
            // Not accepting events, in stopped state
            return;
        }
        replicationContext.getRuntimeFsmTaskManager().addTask(event, event.getClass());
    }

    /**
     * Perform transition between states.
     *
     * @param from initial state
     * @param to   final state
     */
    public void transition(LogReplicationRuntimeState from, LogReplicationRuntimeState to) {
        log.trace("Transition from {} to {}", from, to);
        from.onExit(to);
        to.clear();
        to.onEntry(from);
    }

    public synchronized void updateConnectedNodes(String nodeId) {
        connectedNodes.add(nodeId);
    }

    public synchronized void updateDisconnectedNodes(String nodeId) {
        connectedNodes.remove(nodeId);
    }

    public synchronized void setRemoteLeaderNodeId(String leaderId) {
        log.debug("Set remote leader node id {}", leaderId);
        leaderNodeId = Optional.ofNullable(leaderId);
    }

    public synchronized void resetRemoteLeaderNodeId() {
        log.debug("Reset remote leader node id");
        leaderNodeId = Optional.empty();
    }

    public synchronized Optional<String> getRemoteLeaderNodeId() {
        log.trace("Retrieve remote leader node id {}", leaderNodeId);
        return leaderNodeId;
    }

    public synchronized Set<String> getConnectedNodes() {
        return connectedNodes;
    }

    public synchronized void refresh(ClusterDescriptor clusterDescriptor, long topologyConfigId) {
        log.warn("Update router's cluster descriptor {}", clusterDescriptor);
        router.onClusterChange(clusterDescriptor);
        sourceManager.getLogReplicationFSM().setTopologyConfigId(topologyConfigId);
    }

    /**
     * Stop Log Replication, regardless of current state.
     */
    public void stop() {
        replicationContext.getRuntimeFsmTaskManager().addTask(
                new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.LOCAL_LEADER_LOSS,
                        router.isConnectionStarterForSession(session), this), LogReplicationRuntimeEvent.class);
    }
}
